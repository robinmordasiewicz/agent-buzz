#!/usr/bin/python3
"""Bounded, isolated two-account baseline. Never drives an AI or normal cursors."""
import argparse
import contextlib
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import sqlite3
import stat
import subprocess
import sys
import time
import uuid

try:
    from . import loop_control as control
except ImportError:
    import loop_control as control

PIN = "4d70e07a8eb24a218c715a40cc3d96b5e386ec36"
HASHES = {
    "broker.py": "bac42c565fbab2633f5f97d405250ed2fac7efe10b8c32d7c69e1e60637e7901",
    "client.py": "67a961d31394184e4ce58a74c236343a26587b3a9d862605980b7855db5c400e",
}
ROLES = {"robin": 1000, "zeul": 996}
WARMUP, MEASURED, RETRIES = 8, 256, 8
RATE, POLL, PAUSE, SECONDS = 20, .1, 15, 120
SHARED = Path("/srv/agent-channel/run")
REGISTRY = Path.home() / ".local/state/agent-buzz/baseline-registry"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def experiment(value):
    parsed = uuid.UUID(value)
    require(str(parsed) == value and parsed.version == 4, "Use a fresh canonical UUID4")
    return value


def socket_path(eid):
    return SHARED / ("baseline-" + experiment(eid)) / "broker.sock"


def expected(eid, sender):
    return {str(uuid.uuid5(uuid.UUID(eid), f"{sender}:{phase}:{index}")):
            (phase, index) for phase, count in (("warmup", WARMUP), ("measured", MEASURED))
            for index in range(count)}


def text(eid, sender, phase, index):
    value = json.dumps(dict(experiment=eid, sender=sender, phase=phase, index=index),
                       sort_keys=True, separators=(",", ":"))
    raw = value.encode("utf-8")
    require(len(raw) <= 512, "Synthetic header exceeds payload size")
    value += " " * (512 - len(raw))
    require(len(value.encode("utf-8")) == 512, "Payload must be exactly 512 UTF-8 bytes")
    return value


def request(eid, role, phase, index):
    mid = str(uuid.uuid5(uuid.UUID(eid), f"{role}:{phase}:{index}"))
    return dict(v=1, op="send", id=mid, conversation_id="baseline-" + eid,
                reply_to=None, kind="message", agreement=None,
                text=text(eid, role, phase, index))


def quantiles(values):
    require(all(isinstance(v, (int, float)) and math.isfinite(v) for v in values),
            "Non-finite metric sample")
    values = sorted(values)
    if not values:
        return dict(n=0, p50=None, p95=None, p99=None, maximum=None)
    return {"n": len(values), "negative_samples": sum(v < 0 for v in values),
            **{name: values[math.ceil(p * len(values)) - 1]
            for name, p in (("p50", .50), ("p95", .95), ("p99", .99))},
            "maximum": values[-1]}


def private(path, fresh=False):
    require(not path.is_symlink(), "Private directory cannot be a symlink")
    path.mkdir(parents=True, mode=0o700, exist_ok=not fresh)
    st = path.stat()
    require(st.st_uid == os.getuid() and stat.S_IMODE(st.st_mode) == 0o700,
            "State directory must be owned/private 0700")
    return path


def reserve(base, eid, role):
    experiment(eid)
    require(role in {"broker", *ROLES}, "Invalid experiment role")
    private(REGISTRY)
    # A different output directory must not silently permit the same run again.
    marker = REGISTRY / (eid + "." + role + ".json")
    with marker.open("x") as handle:
        os.fchmod(handle.fileno(), 0o600)
        json.dump(dict(experiment=eid, role=role, state_base=str(base)), handle)
        handle.flush()
        os.fsync(handle.fileno())
    fd = os.open(REGISTRY, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    private(base)
    private(base / experiment(eid))
    # Refuse reuse even after an interrupted/failed prior run. Internal consumer
    # restart reuses its journal; a new command invocation needs a new UUID.
    return private(base / eid / role, fresh=True)


def core(directory):
    for name, expected_hash in HASHES.items():
        require(sha(directory / name) == expected_hash, "Core hash differs from pinned baseline: " + name)
    spec = importlib.util.spec_from_file_location("baseline_core_client", directory / "client.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def identity(role):
    require(sys.platform == "linux" and os.getuid() == ROLES[role],
            "Run through the named account's own Linux access; no simulated identity")
    require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"),
            "This harness requires Linux pidfd support")
    fd = os.pidfd_open(os.getpid())
    os.close(fd)


def window(path, eid, now=None):
    value = json.loads(path.read_text())
    require(isinstance(value, dict) and set(value) == {"experiment_id", "start_utc", "core_commit", "harness_sha256", "ack_urls"},
            "Unexpected readiness manifest fields")
    require(value["experiment_id"] == experiment(eid) and value["core_commit"] == PIN,
            "Readiness manifest does not match run/source")
    require(value["harness_sha256"] == sha(__file__), "Manifest must bind the reviewed harness bytes")
    require(isinstance(value["ack_urls"], dict) and set(value["ack_urls"]) == set(ROLES), "Both owners need explicit readiness references")
    urls = list(value["ack_urls"].values())
    require(all(isinstance(url, str) and re.fullmatch(
        r"https://github\.com/robinmordasiewicz/agent-buzz/issues/[0-9]+#issuecomment-[0-9]+", url)
        for url in urls) and len(set(urls)) == 2, "Provide distinct project readiness references")
    # References are operator-supplied evidence pointers, not an authorization
    # verifier. Operators must read the actual two readiness ACKs first.
    require(isinstance(value["start_utc"], str), "UTC start must be text")
    start = datetime.fromisoformat(value["start_utc"].replace("Z", "+00:00"))
    require(start.utcoffset() is not None and start.utcoffset().total_seconds() == 0,
            "Window must be timezone-qualified UTC")
    start = start.timestamp()
    now = time.time() if now is None else now
    require(2 <= start - now <= 60, "Missed or distant window; obtain a new explicit readiness window")
    return value, start


def append(path, value):
    with path.open("a") as handle:
        handle.write(json.dumps(value, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def resources(record):
    if not control.owned(record):
        return None
    pid = record["pid"]
    try:
        fields = (Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
        return dict(pid=pid, start_ticks=record["start_ticks"], utc=time.time(), cpu_seconds=(int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                    rss_bytes=int(fields[21]) * os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError, IndexError):
        return None


def wait_until(target, deadline):
    while time.time() < target:
        require(time.time() < deadline, "Experiment deadline")
        time.sleep(min(.02, target - time.time()))


def usage():
    own, children = resource.getrusage(resource.RUSAGE_SELF), resource.getrusage(resource.RUSAGE_CHILDREN)
    return dict(pid=os.getpid(), cpu_seconds=own.ru_utime + own.ru_stime,
                max_rss_bytes=own.ru_maxrss * 1024,
                checkpoint_children_cpu_seconds=children.ru_utime + children.ru_stime)


def sender(client, config, root):
    results, retry_results = [], []
    start = config["start"]
    with contextlib.closing(client.journal(root / "sender-journal")) as db:
        for ordinal, (mid, (phase, index)) in enumerate(expected(config["experiment"], config["role"]).items()):
            wait_until(start + ordinal / RATE, config["deadline"])
            require(time.time() < config["deadline"], "Send deadline")
            req = request(config["experiment"], config["role"], phase, index)
            attempted = time.time()
            began = time.perf_counter()
            result = client.send(db, config["socket"], req)
            elapsed = (time.perf_counter() - began) * 1000
            require(result["ok"] and not result["duplicate"] and result["id"] == mid, "Unexpected fresh acceptance")
            row = dict(id=mid, phase=phase, index=index, seq=result["seq"], send_accept_ms=elapsed,
                       attempt_started_utc=attempted, planned_start_utc=start + ordinal / RATE,
                       completed_utc=time.time())
            append(root / "sender.jsonl", row)
            results.append(row)
        measured = [row for row in results if row["phase"] == "measured"]
        for row in measured[:RETRIES]:
            require(time.time() < config["deadline"], "Retry deadline")
            req = request(config["experiment"], config["role"], "measured", row["index"])
            result = client.send(db, config["socket"], req)
            require(result["ok"] and result["duplicate"] and result["id"] == row["id"]
                    and result["seq"] == row["seq"], "Exact retry changed acceptance")
            retry_results.append(result)
    control.atomic(root / "sender-result.json", dict(
        ids=[row["id"] for row in results], retries=retry_results,
        send_accept_ms=quantiles([row["send_accept_ms"] for row in measured]),
        achieved_measured_sends_per_second=((len(measured) - 1) /
            (measured[-1]["attempt_started_utc"] - measured[0]["attempt_started_utc"])),
        pacing_lag_ms=quantiles([(row["attempt_started_utc"] - row["planned_start_utc"]) * 1000 for row in measured]),
        socket_connections=len(results) + len(retry_results),
        timing_scope="unchanged client.send including durable outbox; excludes subsequent sample logging"))


def checkpoint(config, journal, after):
    left = config["deadline"] - time.time()
    require(left > 0, "Checkpoint deadline")
    result = subprocess.run([sys.executable, str(Path(config["core_dir"]) / "client.py"),
        "--state-dir", str(journal), "checkpoint", "--after", str(after)],
        capture_output=True, text=True, timeout=min(15, left))
    require(result.returncode == 0, "Unchanged client checkpoint command failed")


def consumer(client, config, root, name):
    directory = private(root / name)
    path = directory / "observations.json"
    observations = json.loads(path.read_text()) if path.exists() else {}
    other = "zeul" if config["role"] == "robin" else "robin"
    wanted = expected(config["experiment"], other)
    marker = directory / "interrupted.json"
    connections = 0
    if name == "paused":
        wait_until(config["start"] + WARMUP / RATE + PAUSE, config["deadline"])
    else:
        wait_until(config["start"], config["deadline"])
    with contextlib.closing(client.journal(directory / "journal")) as db:
        while time.time() < config["deadline"]:
            began = time.time()
            page = client.exchange(config["socket"], dict(v=1, op="receive",
                after=client.cursor(db, "processed"), limit=100))
            observed = time.time()
            connections += 1
            require(page["ok"], "Receive rejected")
            append(directory / "polls.jsonl", dict(started_utc=began, observed_utc=observed,
                   received=len(page["messages"]), pid=os.getpid()))
            with db:
                client.cache(db, page)
            for row in page["messages"]:
                require(row["id"] in wanted and row["conversation_id"] == "baseline-" + config["experiment"],
                        "Fixture received unrelated input")
                phase, index = wanted[row["id"]]
                require(row["text"] == text(config["experiment"], other, phase, index)
                        and row["sender"] == other and row["recipient"] == config["role"], "Synthetic payload/participants differ")
                observations.setdefault(row["id"], dict(phase=phase, seq=row["seq"],
                    accepted_utc=row["accepted_utc"], first_observed_utc=observed, poll_started_utc=began))
            control.atomic(path, observations)
            measured_seen = sum(row["phase"] == "measured" for row in observations.values())
            if name == "fast" and measured_seen >= 64 and not marker.exists():
                control.atomic(marker, dict(pending_ids=[row["id"] for row in page["messages"]],
                    processed_before=client.cursor(db, "processed"),
                    note="Controlled exit after durable cache and before synthetic processing/checkpoint"))
                return 75
            if page["messages"]:
                if marker.exists():
                    replay = set(json.loads(marker.read_text())["pending_ids"])
                    for row in page["messages"]:
                        if row["id"] in replay:
                            observations[row["id"]]["replayed_after_interruption"] = True
                for row in page["messages"]:
                    observations[row["id"]]["synthetic_processing_utc"] = time.time()
                control.atomic(path, observations)
                checkpoint(config, directory / "journal", page["next_cursor"])
                for row in page["messages"]:
                    observations[row["id"]]["checkpoint_utc"] = time.time()
                control.atomic(path, observations)
            if set(observations) == set(wanted) and all("checkpoint_utc" in row for row in observations.values()):
                pending = set(json.loads(marker.read_text())["pending_ids"]) if marker.exists() else set()
                require(all(observations[mid].get("replayed_after_interruption") for mid in pending), "Pending input did not replay")
                control.atomic(directory / "result.json", dict(ids=sorted(observations),
                    processed_cursor=client.cursor(db, "processed"), replayed_ids=sorted(pending),
                    socket_connections_this_process=connections, model_processing=False))
                return 0
            time.sleep(max(0, min(POLL - (time.time() - began), config["deadline"] - time.time())))
    raise TimeoutError("Consumer did not catch up within experiment deadline")


def spawn(argv, root, name, state, handles):
    with (root / (name + ".log")).open("ab") as log:
        child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    handles.append(child)
    # Capture a kernel handle before any poll/reap, protecting partial-start
    # cleanup even when /proc identity discovery itself fails.
    fd = os.pidfd_open(child.pid)
    try:
        current = control.identity(child.pid)
        if current is None:
            signal.pidfd_send_signal(fd, signal.SIGTERM)
            try:
                child.wait(timeout=1)
            except subprocess.TimeoutExpired:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
                child.wait()
            raise RuntimeError("Child identity unavailable; kernel-bound cleanup completed")
    finally:
        os.close(fd)
    record = dict(name=name, pid=child.pid, argv=argv, start_ticks=current["start_ticks"])
    state["workers"].append(record)
    control.atomic(root / "control.json", state)
    return child, record


def run_account(client, config, root):
    state, handles, active = {"workers": []}, [], {}
    interrupted = False
    try:
        for name in ("sender", "fast", "paused"):
            argv = [sys.executable, str(Path(__file__).resolve()), "_worker", "--config", str(root / "config.json"), "--name", name]
            active[name] = spawn(argv, root, name, state, handles)
        next_sample = 0
        # Reserve the existing owned-process controller's two-second termination
        # grace inside the 120-second bound, not after it.
        stop_at = min(time.monotonic() + SECONDS, time.monotonic() + max(0, config["deadline"] - time.time())) - 2.1
        while active and time.time() < config["deadline"] and time.monotonic() < stop_at:
            if time.time() >= next_sample:
                append(root / "resources.jsonl", {name: resources(record) for name, (_, record) in active.items()})
                next_sample = time.time() + 1
            for name, (child, record) in list(active.items()):
                status = child.poll()
                if status is None:
                    continue
                if name == "fast" and status == 75 and not interrupted:
                    paused_db = root / "paused/journal/client.sqlite3"
                    require(time.time() < config["start"] + WARMUP / RATE + PAUSE
                            and not paused_db.exists(), "Could not verify interruption during the planned pause; inconclusive run")
                    interrupted = True
                    active[name] = spawn(record["argv"], root, name, state, handles)
                else:
                    require(status == 0, "Experiment worker failed: " + name)
                    del active[name]
            time.sleep(.02)
        require(not active and interrupted, "Experiment incomplete or deadline reached")
        control.atomic(root / "complete.json", dict(experiment=config["experiment"], role=config["role"],
            core_commit=PIN, harness_sha256=config["harness_sha256"], model_processing=False))
    finally:
        control.stop_workers(state)
        control.atomic(root / "control.json", state)
        for child in handles:
            child.wait()


def run_broker(config, root):
    directory = Path(config["socket"]).parent
    require(directory.name == "baseline-" + config["experiment"] and directory.parent == SHARED,
            "Refusing non-experiment socket directory")
    require(SHARED.is_dir() and SHARED.stat().st_uid == os.getuid(), "Shared parent must be Robin-owned")
    private(root / "database")
    directory.mkdir(mode=0o700)  # Never reuse an existing socket/ACL directory.
    original = directory.stat()
    state, handles = {"workers": []}, []
    try:
        argv = [sys.executable, str(Path(config["core_dir"]) / "broker.py"),
                "--socket", config["socket"], "--db", str(root / "database/messages.sqlite3")]
        child, record = spawn(argv, root, "broker", state, handles)
        deadline = min(time.monotonic() + SECONDS, time.monotonic() + max(0, config["deadline"] - time.time())) - 2.1
        while time.time() < config["deadline"] and time.monotonic() < deadline:
            require(child.poll() is None, "Isolated broker exited before deadline")
            append(root / "resources.jsonl", resources(record))
            time.sleep(min(1, max(0, config["deadline"] - time.time())))
    finally:
        control.stop_workers(state)
        control.atomic(root / "control.json", state)
        for child in handles:
            child.wait()
        current = directory.lstat()
        require((current.st_dev, current.st_ino, current.st_uid) ==
                (original.st_dev, original.st_ino, os.getuid()), "Socket directory identity changed; preserve for inspection")
        socket = Path(config["socket"])
        if socket.exists():
            st = socket.lstat()
            require(stat.S_ISSOCK(st.st_mode) and st.st_uid == os.getuid(), "Refuse unexpected cleanup target")
            socket.unlink()
        directory.rmdir()  # Removes only the dedicated empty directory and its ACLs.
    with contextlib.closing(sqlite3.connect((root / "database/messages.sqlite3").as_uri() + "?mode=ro", uri=True)) as db:
        rows = db.execute("SELECT id,seq,sender,recipient,conversation_id,text FROM messages ORDER BY seq").fetchall()
    wanted = {**expected(config["experiment"], "robin"), **expected(config["experiment"], "zeul")}
    ids = [row[0] for row in rows]
    complete = len(ids) == len(wanted) and set(ids) == set(wanted)
    for mid, _, sender_role, recipient, conv, value in rows:
        require(mid in wanted and sender_role in ROLES and recipient != sender_role
                and conv == "baseline-" + config["experiment"], "Unexpected accepted fixture record")
        phase, index = expected(config["experiment"], sender_role)[mid]
        require(value == text(config["experiment"], sender_role, phase, index), "Stored payload differs")
    control.atomic(root / "broker-result.json", dict(complete=complete, accepted_unique=len(ids),
        exact_ids=complete, expected_unique=len(wanted), core_commit=PIN,
        stored_sequences=[dict(id=row[0], seq=row[1]) for row in rows]))
    require(complete, "Broker fixture is incomplete; retained evidence, no success claim")


def summarize(root):
    config = json.loads((root / "config.json").read_text())
    require((root / "complete.json").exists(), "Run incomplete; do not publish success metrics")
    other = "zeul" if config["role"] == "robin" else "robin"
    wanted = set(expected(config["experiment"], other))
    sender_result = json.loads((root / "sender-result.json").read_text())
    require(set(sender_result["ids"]) == set(expected(config["experiment"], config["role"])), "Sender ID set differs")
    result = dict(experiment=config["experiment"], role=config["role"], core_commit=PIN,
        harness_sha256=config["harness_sha256"], send_accept_ms=sender_result["send_accept_ms"],
        achieved_measured_sends_per_second=sender_result["achieved_measured_sends_per_second"],
        pacing_lag_ms=sender_result["pacing_lag_ms"],
        exact_retries=len(sender_result["retries"]), consumers={}, model_processing=False,
        thread_receipt="not measured by synthetic workload",
        limitations="isolated unchanged instance; empirical nearest-rank samples; instrumentation/CLI overhead included as labelled")
    for name in ("fast", "paused"):
        rows = json.loads((root / name / "observations.json").read_text())
        require(set(rows) == wanted, "Consumer exact ID set differs")
        measured = [row for row in rows.values() if row["phase"] == "measured"]
        accepted = lambda row: datetime.fromisoformat(row["accepted_utc"]).timestamp()
        queue = sorted([(accepted(row), 1) for row in measured] + [(row["checkpoint_utc"], -1) for row in measured], key=lambda v: (v[0], -v[1]))
        depth, maximum = 0, 0
        for _, delta in queue:
            depth += delta
            maximum = max(depth, maximum)
        result["consumers"][name] = dict(exact_ids=True,
            commit_to_observe_ms=quantiles([(row["first_observed_utc"] - accepted(row)) * 1000 for row in measured]),
            commit_to_synthetic_checkpoint_ms=quantiles([(row["checkpoint_utc"] - accepted(row)) * 1000 for row in measured]),
            measured_queue_depth_max=maximum,
            processing_scope="synthetic test event, not model processing; includes unchanged CLI checkpoint overhead")
        polls = [json.loads(line) for line in (root / name / "polls.jsonl").read_text().splitlines()]
        result["consumers"][name].update(socket_connections=len(polls),
            exchange_ms=quantiles([(row["observed_utc"] - row["started_utc"]) * 1000 for row in polls]),
            achieved_poll_hz=((len(polls) - 1) / (polls[-1]["started_utc"] - polls[0]["started_utc"])) if len(polls) > 1 else None)
    result["worker_usage"] = [json.loads(line) for line in (root / "usage.jsonl").read_text().splitlines()]
    return result


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("preflight", "run", "broker"):
        sub = commands.add_parser(name)
        sub.add_argument("--experiment", required=True)
        sub.add_argument("--role", choices=list(ROLES), default="robin" if name == "broker" else None, required=name != "broker")
        sub.add_argument("--core-dir", type=Path, default=Path("/srv/agent-channel"))
        if name != "preflight":
            sub.add_argument("--window-file", type=Path, required=True)
            sub.add_argument("--state-base", type=Path, required=True)
    sub = commands.add_parser("_worker")
    sub.add_argument("--config", type=Path, required=True)
    sub.add_argument("--name", choices=["sender", "fast", "paused"], required=True)
    sub = commands.add_parser("summarize")
    sub.add_argument("account_dir", type=Path)
    args = parser.parse_args()
    if args.command == "summarize":
        print(json.dumps(summarize(args.account_dir), indent=2))
        return 0
    if args.command == "_worker":
        config = json.loads(args.config.read_text())
        identity(config["role"])
        require(config["harness_sha256"] == sha(__file__), "Worker tooling changed")
        client = core(Path(config["core_dir"]))
        root = args.config.parent
        try:
            if args.name == "sender":
                sender(client, config, root)
                return 0
            return consumer(client, config, root, args.name)
        finally:
            append(root / "usage.jsonl", dict(worker=args.name, **usage()))
    identity(args.role)
    require(args.command != "broker" or args.role == "robin", "Only Robin runs the isolated broker")
    eid = experiment(args.experiment)
    client = core(args.core_dir)
    if args.command == "preflight":
        print(json.dumps(dict(uid=os.getuid(), role=args.role, core_commit=PIN,
            hashes=HASHES, harness_sha256=sha(__file__), socket=str(socket_path(eid)),
            scope="read-only preflight, not readiness or execution authorization"), indent=2))
        return 0
    readiness, start = window(args.window_file, eid)
    root = reserve(args.state_base.absolute(), eid, "broker" if args.command == "broker" else args.role)
    config = dict(experiment=eid, role=args.role, start=start, deadline=start + SECONDS,
        socket=str(socket_path(eid)), core_dir=str(args.core_dir.resolve()),
        harness_sha256=sha(__file__), readiness=readiness, core_commit=PIN)
    control.atomic(root / "config.json", config)
    # Interrupts go through owned-process cleanup, never broad process matching.
    def interrupted(*_):
        raise KeyboardInterrupt("Experiment explicitly interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        if args.command == "broker":
            run_broker(config, root)
        else:
            run_account(client, config, root)
            control.atomic(root / "summary.json", summarize(root))
    except (Exception, KeyboardInterrupt) as error:
        control.atomic(root / "failure.json", dict(complete=False, error=str(error),
            scope="No success/latency claim; inspect retained private records"))
        raise
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError, KeyboardInterrupt) as error:
        print(json.dumps(dict(error=str(error), complete=False)), file=sys.stderr)
        sys.exit(1)
