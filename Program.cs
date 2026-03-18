using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

// ─────────────────────────────────────────────────────────────────────────────
//  AMR OSS Cluster Policy Validator · StackExchange.Redis 2.x · .NET 8
//
//  What this proves:
//    TEST 1 · How SE.Redis discovers shard IPs at runtime 
//    TEST 2 · Cross-slot behaviour and what hash tags fix (the loyalty question)
//    TEST 3 · CLUSTER INFO — what the client sees about the cluster topology
//    TEST 4 · MOVED/ASK — how SE.Redis handles redirects transparently
// ─────────────────────────────────────────────────────────────────────────────

const string RESET = "\x1b[0m";
const string BOLD = "\x1b[1m";
const string RED = "\x1b[31m";
const string GREEN = "\x1b[32m";
const string YELLOW = "\x1b[33m";
const string CYAN = "\x1b[36m";
const string DIM = "\x1b[2m";

// ── FILL THIS IN ─────────────────────────────────────────────────────────────
const string OSS_CONN =
    "<hostname>:10000,password=<your-key>,ssl=True,abortConnect=False,connectTimeout=5000,responseTimeout=5000";

// ─────────────────────────────────────────────────────────────────────────────

var findings = new List<string>();
var redirectLog = new List<string>();

PrintBanner();

if (OSS_CONN.StartsWith("<hostname>"))
{
    Fail("Connection string not set. Fill in OSS_CONN at the top of Program.cs.");
    return;
}

ConnectionMultiplexer? mux = null;
try
{
    var opts = ConfigurationOptions.Parse(OSS_CONN);
    opts.AllowAdmin = true;
    opts.ReconnectRetryPolicy = new ExponentialRetry(5000);

    // ── Print what the hostname resolves to ─────────────────────────────────────
    // This is the key check for Private Endpoint:
    // If the hostname resolves to a private IP (10.x.x.x / 172.x.x.x),
    // Private Endpoint DNS is working correctly inside this VNet.
    // Compare this IP against what CLUSTER NODES returns in Test 1.
    try
    {
        var host =
            opts.EndPoints.OfType<System.Net.DnsEndPoint>().FirstOrDefault()?.Host
            ?? opts.EndPoints.FirstOrDefault()?.ToString() ?? "unknown";
        var addrs = await System.Net.Dns.GetHostAddressesAsync(host);
        Console.WriteLine($"  Hostname : {host}");
        foreach (var a in addrs)
            Console.WriteLine(
                $"  Resolves to: {CYAN}{a}{RESET}  {(a.ToString().StartsWith("10.") || a.ToString().StartsWith("172.") || a.ToString().StartsWith("192.168.") ? GREEN + "<-- private IP, Private Endpoint DNS is working" + RESET : YELLOW + "<-- public IP, not going through Private Endpoint" + RESET)}"
            );
        Console.WriteLine();
    }
    catch (Exception dnsEx)
    {
        Console.WriteLine($"  DNS resolution check failed: {dnsEx.Message}");
    }

    Print("Connecting to AMR OSS Cluster Policy instance...");
    mux = await ConnectionMultiplexer.ConnectAsync(opts);

    // Hook MOVED/ASK events before any commands run.
    // SE.Redis handles these silently — this surfaces them so we can observe them.
    mux.ErrorMessage += (_, e) =>
    {
        if (e.Message.StartsWith("MOVED") || e.Message.StartsWith("ASK"))
        {
            var entry = $"  [{DateTime.UtcNow:HH:mm:ss.fff}] {e.Message}";
            redirectLog.Add(entry);
            Warn($"  REDIRECT OBSERVED: {e.Message}");
        }
    };
    mux.InternalError += (_, e) =>
    {
        var msg = e.Exception?.Message ?? "";
        if (msg.Contains("MOVED") || msg.Contains("ASK"))
            redirectLog.Add($"  [InternalError] {msg}");
    };

    OK("Connected.\n");

    await Test1_ShardDiscovery(mux, findings);
    await Test2_CrossSlot(mux, findings, redirectLog);
    await Test3_ClusterInfo(mux, findings);
    await Test4_MovedAsk(mux, findings, redirectLog);

    PrintSummary(findings, redirectLog);
}
catch (Exception ex)
{
    Fail($"Connection failed: {ex.Message}");
}
finally
{
    mux?.Dispose();
}

// ── TEST 1: Shard Discovery ───────────────────────────────────────────────────
async Task Test1_ShardDiscovery(ConnectionMultiplexer mux, List<string> findings)
{
    Section("TEST 1 · Shard Discovery — What SE.Redis Actually Connects To");

    var configuredEndpoints = mux.GetEndPoints();

    Print("  What you configured (your connection string endpoint):");
    foreach (var ep in configuredEndpoints)
    {
        var srv = mux.GetServer(ep);
        Print($"    {Cyan(ep.ToString() ?? "unknown")}  connected={srv.IsConnected}");
    }
    Print(
        $"\n  {DimText("GetEndPoints() only shows what YOU configured — not what SE.Redis discovered.")}"
    );
    Print($"  {DimText("The real Calico question is answered by CLUSTER NODES below.")}");
    Console.WriteLine();

    Print("  Shard IPs discovered via CLUSTER NODES:");
    Print("  SE.Redis sends CLUSTER NODES immediately after handshake to build its slot map.");
    Print("  It then opens a direct TCP connection to each discovered IP on port 10000.");
    Console.WriteLine();

    try
    {
        var server = mux.GetServer(configuredEndpoints.First());
        var clusterNodes = await server.ExecuteAsync("CLUSTER", "NODES");
        var lines = clusterNodes.ToString()!.Split('\n', StringSplitOptions.RemoveEmptyEntries);

        var primaries = new List<string>();
        var replicas = new List<string>();

        foreach (var line in lines)
        {
            var parts = line.Split(' ');
            if (parts.Length < 8)
                continue;

            // CLUSTER NODES reports the gossip/bus port (e.g. 8500), NOT the client port.
            // AMR always listens on 10000 for Redis client connections.
            var ipPortRaw = parts[1].Split('@')[0];
            var ip = ipPortRaw.Split(':')[0];
            var gossipPort = ipPortRaw.Split(':').Last();
            var flags = parts[2];
            var slotStr =
                parts.Length > 8 ? string.Join(" ", parts.Skip(8)).Trim() : "(no slots — replica)";
            var isMaster = flags.Contains("master") && !flags.Contains("slave");
            var clientEp = $"{ip}:10000";

            if (isMaster)
            {
                primaries.Add(ip);
                OK($"    PRIMARY  {Cyan(clientEp.PadRight(22))}  slots: {slotStr}");
                Print(
                    $"             {DimText($"gossip port :{gossipPort} is internal — Calico needs port 10000")}"
                );
            }
            else
            {
                replicas.Add(ip);
                Print($"    {DimText($"replica  {clientEp.PadRight(22)}  {slotStr}")}");
            }
        }

        Console.WriteLine();
        Warn($"  CALICO FINDING:");
        Warn(
            $"  You configured 1 endpoint. SE.Redis discovered {primaries.Count} primary shard(s) and {replicas.Count} replica(s)."
        );
        Warn($"  SE.Redis opens direct TCP connections to each PRIMARY on port 10000.");
        Warn($"  These IPs are not in your connection string — discovered at runtime.");
        Warn($"  They can change when AMR rescales or rebalances.");
        Console.WriteLine();
        Warn($"  IPs that must be allowed in Calico egress rules:");
        foreach (var ip in primaries)
            Warn($"    -> {ip}:10000");
        Console.WriteLine();
        Print(
            $"  {DimText("Recommended: a CIDR rule covering the AMR VNet subnet is more resilient than per-IP rules.")}"
        );

        findings.Add(
            $"[T1] Configured 1 endpoint. Discovered {primaries.Count} primary shard IP(s) via CLUSTER NODES."
        );
        findings.Add($"[T1] Calico must allow port 10000 to: {string.Join(", ", primaries)}");
        findings.Add($"[T1] These IPs are runtime-discovered — not in the connection string.");
    }
    catch (Exception ex)
    {
        Fail($"  CLUSTER NODES failed: {ex.Message}");
        findings.Add($"[T1] CLUSTER NODES unavailable: {ex.Message.Split('\n')[0]}");
    }
}

// ── TEST 2: Cross-Slot Behaviour ─────────────────────────────────────────────
async Task Test2_CrossSlot(
    ConnectionMultiplexer mux,
    List<string> findings,
    List<string> redirectLog
)
{
    Section("TEST 2 · Cross-Slot Behaviour — The Loyalty Workload Question");

    var db = mux.GetDatabase();

    // 2a: MGET without hash tags
    Print("  2a. MGET on keys without hash tags (likely on different slots):");
    Print(
        $"      {DimText("SE.Redis computes CRC16(key) % 16384 for each key locally before sending.")}"
    );
    Print($"      {DimText("If they don't share a slot, SE.Redis blocks the call client-side.")}");
    Console.WriteLine();

    var k1 = "chipotle:member:1001";
    var k2 = "chipotle:points:1001";
    var k3 = "chipotle:orders:1001";
    await db.StringSetAsync(k1, "member_data");
    await db.StringSetAsync(k2, "1500");
    await db.StringSetAsync(k3, "order_list");

    try
    {
        await db.StringGetAsync(new RedisKey[] { k1, k2, k3 });
        OK($"    MGET succeeded — keys happened to land on the same slot.");
        findings.Add("[T2a] MGET no hash tags -> succeeded (keys on same slot by chance)");
    }
    catch (Exception ex)
    {
        Fail($"    MGET BLOCKED: {ex.Message}");
        Console.WriteLine();
        Warn($"    This error came from SE.Redis itself — not the server.");
        Warn($"    SE.Redis computed the slots locally, saw a mismatch, and rejected it.");
        findings.Add("[T2a] MGET no hash tags -> BLOCKED client-side by SE.Redis (slot mismatch)");
    }

    // 2b: MGET with hash tags
    Console.WriteLine();
    Print("  2b. MGET with {memberId} hash tag — the fix:");
    Print(
        $"      {DimText("Hash tag tells SE.Redis: only use the text inside {} for slot calculation.")}"
    );
    Print($"      {DimText("All keys with the same tag are guaranteed on the same shard.")}");
    Console.WriteLine();

    var memberId = "member:1001";
    var hk1 = $"{{{memberId}}}:profile";
    var hk2 = $"{{{memberId}}}:points";
    var hk3 = $"{{{memberId}}}:orders";
    await db.StringSetAsync(hk1, "member_data");
    await db.StringSetAsync(hk2, "1500");
    await db.StringSetAsync(hk3, "order_list");

    try
    {
        var vals = await db.StringGetAsync(new RedisKey[] { hk1, hk2, hk3 });
        OK($"    MGET succeeded: {string.Join(", ", vals.Select(v => (string?)v ?? "nil"))}");
        Console.WriteLine();
        OK($"    Hash tag pattern for Chipotle loyalty:");
        OK($"      {{{memberId}}}:profile  -> same shard");
        OK($"      {{{memberId}}}:points   -> same shard");
        OK($"      {{{memberId}}}:orders   -> same shard");
        OK($"    MGET, MSET, and transactions all work once hash tags are in place.");
        findings.Add(
            "[T2b] MGET with {memberId} hash tag -> SUCCESS — all three keys on same shard"
        );
        findings.Add(
            "[T2b] Chipotle pattern: {memberId}:profile, {memberId}:points, {memberId}:orders"
        );
    }
    catch (Exception ex)
    {
        Fail($"    MGET with hash tag FAILED (unexpected): {ex.Message}");
        findings.Add($"[T2b] MGET with hash tag -> unexpected failure: {ex.Message}");
    }

    // 2c: Transaction
    Console.WriteLine();
    Print("  2c. MULTI/EXEC transaction across keys on different slots:");
    Console.WriteLine();

    var tx = db.CreateTransaction();
    _ = tx.StringSetAsync(k1, "updated_member");
    _ = tx.StringSetAsync(k2, "1600");
    try
    {
        var committed = await tx.ExecuteAsync();
        OK($"    Transaction committed={committed} (keys on same slot by chance)");
        findings.Add("[T2c] Multi-key transaction -> committed (same slot by chance)");
    }
    catch (Exception ex)
    {
        Fail($"    Transaction BLOCKED: {ex.Message}");
        Warn($"    Same rule as MGET — transactions across slots require hash tags.");
        findings.Add("[T2c] Multi-key transaction -> BLOCKED (hash tags required)");
    }

    Console.WriteLine();
    Print(
        $"  {DimText($"MOVED/ASK redirects observed so far: {redirectLog.Count} (SE.Redis absorbs these silently)")}"
    );

    try
    {
        await db.KeyDeleteAsync(new RedisKey[] { k1, k2, k3, hk1, hk2, hk3 });
    }
    catch
    { /* best effort */
    }
}

// ── TEST 3: CLUSTER INFO ──────────────────────────────────────────────────────
async Task Test3_ClusterInfo(ConnectionMultiplexer mux, List<string> findings)
{
    Section("TEST 3 · CLUSTER INFO — What SE.Redis Sees About the Cluster");

    var server = mux.GetServer(mux.GetEndPoints().First());

    try
    {
        var info = await server.ExecuteAsync("CLUSTER", "INFO");
        var lines = info.ToString()!
            .Split('\n')
            .Where(l =>
                l.StartsWith("cluster_enabled")
                || l.StartsWith("cluster_state")
                || l.StartsWith("cluster_slots_assigned")
                || l.StartsWith("cluster_slots_ok")
                || l.StartsWith("cluster_known_nodes")
                || l.StartsWith("cluster_size")
                || l.StartsWith("cluster_current_epoch")
            )
            .Select(l => l.Trim())
            .ToList();

        Print("  Key fields:");
        Console.WriteLine();
        foreach (var line in lines)
            Print($"    {Cyan(line)}");

        Console.WriteLine();
        Print($"  {DimText("cluster_enabled:1    this instance is running in cluster mode")}");
        Print($"  {DimText("cluster_state:ok     all slots assigned, cluster healthy")}");
        Print(
            $"  {DimText("cluster_known_nodes  total nodes SE.Redis tracks (primaries + replicas)")}"
        );
        Print($"  {DimText("cluster_size         number of primary shards")}");

        findings.Add($"[T3] CLUSTER INFO: {string.Join(" | ", lines)}");
    }
    catch (Exception ex)
    {
        Fail($"  CLUSTER INFO failed: {ex.Message}");
    }

    Console.WriteLine();
    Print("  CLUSTER SHARDS (the slot-to-shard map SE.Redis builds internally):");
    try
    {
        var shards = await server.ExecuteAsync("CLUSTER", "SHARDS");
        Print($"  {DimText($"Response length: {shards.ToString()!.Length} chars")}");
        Print(
            $"  {DimText("SE.Redis parses this to route every GET/SET directly to the correct shard.")}"
        );
        Print(
            $"  {DimText("This is also the authoritative source of shard IPs for Calico rules.")}"
        );
        findings.Add(
            "[T3] CLUSTER SHARDS available — SE.Redis uses this for slot-to-shard routing"
        );
    }
    catch (Exception ex)
    {
        Print($"  {DimText($"CLUSTER SHARDS: {ex.Message.Split('\n')[0]}")}");
    }
}

// ── TEST 4: MOVED/ASK Transparency ───────────────────────────────────────────
async Task Test4_MovedAsk(
    ConnectionMultiplexer mux,
    List<string> findings,
    List<string> redirectLog
)
{
    Section("TEST 4 · MOVED/ASK — How SE.Redis Handles Cluster Redirects");

    var db = mux.GetDatabase();
    var server = mux.GetServer(mux.GetEndPoints().First());

    // Show slot calculation — proves SE.Redis routes locally without guessing
    Print("  CLUSTER KEYSLOT — slot assignments SE.Redis computes before routing:");
    Print(
        $"  {DimText("SE.Redis does this locally using CRC16. No server round-trip for routing decisions.")}"
    );
    Console.WriteLine();

    var demoKeys = new[]
    {
        "chipotle:member:1001",
        "{member:1001}:profile",
        "{member:1001}:points",
        "{member:1001}:orders",
        "loyalty:session:abc",
    };

    foreach (var key in demoKeys)
    {
        try
        {
            var slot = await server.ExecuteAsync("CLUSTER", "KEYSLOT", key);
            var sameTag = key.Contains("{member:1001}")
                ? DimText(" <- same slot as others with this tag")
                : "";
            Print($"    {Cyan(key.PadRight(36))} slot {Bold(slot.ToString()!)}{sameTag}");
        }
        catch (Exception ex)
        {
            Print($"    {key.PadRight(36)} error: {ex.Message.Split('\n')[0]}");
        }
    }

    Console.WriteLine();
    Print("  What MOVED and ASK mean:");
    Console.WriteLine();
    Print($"    {Cyan("MOVED")}  Slot has permanently migrated to a different shard.");
    Print($"           SE.Redis receives MOVED, silently updates its slot map,");
    Print($"           and retries the command on the correct shard.");
    Print($"           App code: zero impact. One extra round-trip, fully transparent.");
    Console.WriteLine();
    Print($"    {Cyan("ASK  ")}  Slot is mid-migration right now.");
    Print($"           SE.Redis follows the redirect for this one command only.");
    Print($"           Does not update the slot map — waits for migration to finish.");
    Print($"           App code: zero impact.");
    Console.WriteLine();

    if (redirectLog.Count > 0)
    {
        Warn(
            $"  {redirectLog.Count} redirect(s) observed during this run (SE.Redis handled all of them):"
        );
        foreach (var r in redirectLog)
            Warn(r);
        findings.Add(
            $"[T4] {redirectLog.Count} MOVED/ASK redirect(s) observed — all handled transparently by SE.Redis"
        );
    }
    else
    {
        OK("  No MOVED/ASK redirects during this run — cluster is stable.");
        OK("  This is normal. Redirects only appear during active slot migration.");
        findings.Add("[T4] 0 redirects observed. SE.Redis would handle any redirects silently.");
    }

    // Slot coverage sanity check
    Console.WriteLine();
    try
    {
        var clusterInfo = await server.ExecuteAsync("CLUSTER", "INFO");
        var assignedLine = clusterInfo
            .ToString()!
            .Split('\n')
            .FirstOrDefault(l => l.StartsWith("cluster_slots_assigned"))
            ?.Trim();
        if (assignedLine != null)
        {
            Print($"  Slot coverage: {Cyan(assignedLine)}");
            if (assignedLine.Contains("16384"))
                OK("  All 16384 slots assigned — cluster fully operational.");
            else
                Warn("  Not all slots assigned — cluster may be partially configured.");
        }
    }
    catch
    { /* best effort */
    }

    await Task.CompletedTask;
}

// ─── HELPERS ─────────────────────────────────────────────────────────────────

void PrintBanner()
{
    Console.WriteLine(
        $"\n{BOLD}{CYAN}+==============================================================+"
    );
    Console.WriteLine($"|   AMR OSS Cluster Policy Validator  StackExchange.Redis     |");
    Console.WriteLine($"|   T1: Shard Discovery  T2: Cross-Slot  T3: Info  T4: MOVED  |");
    Console.WriteLine($"+==============================================================+{RESET}\n");
}

void Section(string text)
{
    Console.WriteLine();
    Console.WriteLine($"{BOLD}{YELLOW}== {text} =={RESET}");
    Console.WriteLine();
}

void Print(string text) => Console.WriteLine(text);
void OK(string text) => Console.WriteLine($"{GREEN}{text}{RESET}");
void Fail(string text) => Console.WriteLine($"{RED}{text}{RESET}");
void Warn(string text) => Console.WriteLine($"{YELLOW}{text}{RESET}");
string Cyan(string s) => $"{CYAN}{s}{RESET}";
string Bold(string s) => $"{BOLD}{s}{RESET}";
string DimText(string s) => $"{DIM}{s}{RESET}";

void PrintSummary(List<string> findings, List<string> redirectLog)
{
    Console.WriteLine();
    Console.WriteLine(
        $"{BOLD}{CYAN}+==============================================================+"
    );
    Console.WriteLine($"|   FINDINGS SUMMARY                                           |");
    Console.WriteLine($"+==============================================================+{RESET}");
    Console.WriteLine();
    foreach (var f in findings)
    {
        var color =
            f.StartsWith("[T1]") ? CYAN
            : f.Contains("BLOCKED") || f.Contains("FAIL") ? RED
            : f.Contains("redirect") && !f.Contains("0 redirect") ? YELLOW
            : GREEN;
        Console.WriteLine($"  {color}{f}{RESET}");
    }
    Console.WriteLine();
}
