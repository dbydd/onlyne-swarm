"""E2E-3 loop stub: forward each loop task to the peer, then out, loop forever.

Usage: stub_loop.py <workspace_abs> <tree_path> <handle> <peer_tree_path> <e2e_root>

Runs until killed. Only loop tasks participate: a task is a loop task iff its
body carries `loop-n=` or `loop-seed` and is not already a stub reply.
Each hop forwards one new task to the peer (transfer_send_to = this task,
lineage only) and immediately writes its own out. Nothing waits for anything:
the loop advances because every hop exits right after spawning the next.
"""
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stub_common import (fetch_loopback, header_fields, reply_task,
                         send_ready, wait_daemon)

ws = os.path.realpath(sys.argv[1])
tree_path = sys.argv[2]
handle = sys.argv[3]
peer = sys.argv[4]
e2e_root = os.path.realpath(sys.argv[5])

wait_daemon(ws)
send_ready(ws, handle)
print("ready", tree_path, flush=True)

seen = set()
while True:
    for m in fetch_loopback(ws, 30):
        t = m.get("text") or ""
        if not t.startswith("---swarm") or t.startswith("---swarm-ctl") or m.get("direction") != "inbound":
            continue
        tid, transfer = header_fields(t)
        if not tid or tid in seen or "stub reply" in t:
            continue
        if "loop-n=" not in t and "loop-seed" not in t:
            continue
        seen.add(tid)
        n = -1
        for line in t.split("\n"):
            if "loop-n=" in line:
                try:
                    n = int(line.split("loop-n=")[1].split()[0])
                except ValueError:
                    pass
        n += 1
        cid = str(uuid.uuid4())
        body = (f"---swarm\ntask_id: {cid}\nfrom: {tree_path}\n"
                f"transfer_send_to: {tid}\nattempt: 1\n---\n"
                f"loop-n={n} ping from {tree_path}\n")
        fifo = os.path.join(e2e_root, ".ws", peer,
                            ".onlyne/channels/loopback/in")
        fd = os.open(fifo, os.O_WRONLY)
        os.write(fd, body.encode())
        os.close(fd)
        print(f"fwd {tid[:8]} -> {peer} loop-n={n} as {cid[:8]}", flush=True)
        reply_task(ws, tree_path, tid, transfer or "",
                   f"stub reply loop-n={n} from {tree_path}")
        print(f"replied {tid[:8]}", flush=True)
    time.sleep(1.0)
