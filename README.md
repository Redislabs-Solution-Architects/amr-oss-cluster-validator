# amr-oss-cluster-validator

Validates Azure Managed Redis (AMR) OSS Cluster Policy behaviour with StackExchange.Redis (.NET).

**What it tests:**
- T1 · Shard discovery — what IPs SE.Redis actually connects to (the Calico question)
- T2 · Cross-slot behaviour — MGET/transactions with and without hash tags
- T3 · CLUSTER INFO — topology, shard count, slot assignments
- T4 · MOVED/ASK — how SE.Redis handles cluster redirects transparently

---

## Run it

**1. Clone the repo**
```bash
git clone https://github.com/Redislabs-Solution-Architects/amr-oss-cluster-validator.git
cd amr-oss-cluster-validator
```

**2. Fill in your connection string** in `Program.cs`:
```csharp
const string OSS_CONN = "YOUR_HOST:10000,password=YOUR_KEY,ssl=True,abortConnect=False,connectTimeout=5000,responseTimeout=5000";
```
Get hostname and key from: **Portal → your AMR instance → Settings → Authentication**

**3. Run:**
```bash
dotnet run
```

> For accurate Private Endpoint results, run from a VM inside the same VNet as your AMR instance.

---

## Requirements
- .NET 8+
- AMR instance with OSS Cluster Policy
