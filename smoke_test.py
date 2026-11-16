"""3-node smoke test: start nodes, send visits, converge, partition, heal, stop."""
import json, os, subprocess, sys, time, shutil, requests, socket
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

ports = {f"node{i}": free_port() for i in range(1,4)}
data_dirs = {}
procs = {}

def peers_str(nid):
    return ",".join(f"{k}=http://127.0.0.1:{v}" for k,v in ports.items() if k != nid)

# Start nodes
print("=== Starting 3-node cluster ===")
for nid, port in ports.items():
    dd = ROOT / "data" / f"smoke_{nid}"
    dd.mkdir(parents=True, exist_ok=True)
    data_dirs[nid] = dd
    cmd = [sys.executable, "-m", "node",
           "--node-id", nid, "--port", str(port), "--host", "127.0.0.1",
           "--peers", peers_str(nid), "--gossip-interval", "0.3",
           "--data-dir", str(dd)]
    procs[nid] = subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# Wait for all healthy
print("Waiting for nodes to become healthy...")
deadline = time.time() + 15
for nid, port in ports.items():
    while time.time() < deadline:
        try:
            r = requests.get(f"http://127.0.0.1:{port}/health", timeout=0.5)
            if r.status_code == 200:
                print(f"  {nid}:{port} UP - {r.json()}")
                break
        except:
            time.sleep(0.2)
    else:
        print(f"  ERROR: {nid} never became healthy!")
        sys.exit(1)

# Send visits
print("\n=== Sending visits ===")
for nid, port in ports.items():
    n = {"node1": 100, "node2": 200, "node3": 300}[nid]
    r = requests.post(f"http://127.0.0.1:{port}/visit?metric=page_views&n={n}", timeout=2)
    print(f"  {nid}: page_views +{n} -> {r.json()}")
r2 = requests.post(f"http://127.0.0.1:{ports['node1']}/visit?metric=clicks&n=42", timeout=2)
print(f"  node1: clicks +42 -> {r2.json()}")

# Wait for convergence
print("\n=== Waiting for convergence (up to 8s) ===")
deadline = time.time() + 8
converged = False
while time.time() < deadline:
    try:
        states = [requests.get(f"http://127.0.0.1:{p}/state", timeout=1).json() for p in ports.values()]
        hashes = [s["state_hash"] for s in states]
        totals = [s["totals"] for s in states]
        if len(set(hashes)) == 1 and totals[0].get("page_views") == 600:
            converged = True
            break
    except:
        pass
    time.sleep(0.3)

for nid, port in ports.items():
    s = requests.get(f"http://127.0.0.1:{port}/state", timeout=2).json()
    print(f"  {nid}: totals={s['totals']} hash={s['state_hash'][:16]}...")

print(f"  CONVERGED: {'YES' if converged else 'NO'}")

# Partition test
print("\n=== Partition node3 ===")
requests.post(f"http://127.0.0.1:{ports['node3']}/partition", json={"blocked":["node1","node2"]}, timeout=2)
requests.post(f"http://127.0.0.1:{ports['node1']}/partition", json={"blocked":["node3"]}, timeout=2)
requests.post(f"http://127.0.0.1:{ports['node2']}/partition", json={"blocked":["node3"]}, timeout=2)
requests.post(f"http://127.0.0.1:{ports['node1']}/visit?metric=page_views&n=40", timeout=2)
requests.post(f"http://127.0.0.1:{ports['node3']}/visit?metric=page_views&n=60", timeout=2)
time.sleep(2.5)
s1 = requests.get(f"http://127.0.0.1:{ports['node1']}/state", timeout=2).json()
s3 = requests.get(f"http://127.0.0.1:{ports['node3']}/state", timeout=2).json()
print(f"  node1 page_views={s1['totals']['page_views']} hash={s1['state_hash'][:16]}")
print(f"  node3 page_views={s3['totals']['page_views']} hash={s3['state_hash'][:16]}")
print(f"  DIVERGED: {'YES' if s1['state_hash'] != s3['state_hash'] else 'NO'}")

# Heal
print("\n=== Healing partition ===")
for port in ports.values():
    requests.delete(f"http://127.0.0.1:{port}/partition", timeout=2)
time.sleep(3)
hashes2 = [requests.get(f"http://127.0.0.1:{p}/state", timeout=2).json()["state_hash"] for p in ports.values()]
totals2 = [requests.get(f"http://127.0.0.1:{p}/state", timeout=2).json()["totals"] for p in ports.values()]
print(f"  Hashes: {[h[:16] for h in hashes2]}")
print(f"  Totals: {totals2[0]}")
print(f"  CONVERGED AFTER HEAL: {'YES' if len(set(hashes2))==1 else 'NO'}")
print(f"  page_views total: {totals2[0].get('page_views')} (expected 700)")

# Stop
print("\n=== Stopping nodes ===")
for nid, proc in procs.items():
    proc.terminate()
    try: proc.wait(timeout=3)
    except: proc.kill(); proc.wait(timeout=1)
    print(f"  {nid} stopped")

# Cleanup
for d in data_dirs.values():
    shutil.rmtree(d, ignore_errors=True)
print("\nSmoke test DONE")
