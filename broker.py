#!/usr/bin/env python3
"""Local message broker, wire v1 / collaboration protocol v1.1. No execution."""
import argparse
import contextlib
from datetime import datetime, timezone
import fcntl
import json
import logging
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import stat
import struct
import subprocess
import time
import uuid

USERS = {1000: "robin", 996: "zeul"}
MAX_FRAME = 65536
MAX_TEXT = 16384
TIMEOUT = 5
FIELDS = ("v", "id", "conversation_id", "reply_to", "kind", "agreement", "sender", "recipient", "text")

class Invalid(ValueError):
    pass

def require(condition, detail):
    if not condition:
        raise Invalid(detail)

def uuid_value(value):
    require(type(value) is str, "UUID must be text")
    try:
        require(str(uuid.UUID(value)) == value, "UUID must use canonical lowercase form")
    except (ValueError, AttributeError):
        raise Invalid("invalid UUID")

def no_duplicate_keys(pairs):
    obj = {}
    for k, v in pairs:
        require(k not in obj, "duplicate JSON key")
        obj[k] = v
    return obj

def bounded_integer(value):
    require(len(value.lstrip("-")) <= 19, "integer exceeds supported range")
    return int(value)

def initialize(path):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(stat.S_IMODE(path.parent.stat().st_mode) == 0o700, "database directory must be private")
    with contextlib.closing(connect(path)) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                v INTEGER NOT NULL CHECK (v=1),
                id TEXT NOT NULL UNIQUE,
                conversation_id TEXT NOT NULL,
                reply_to TEXT REFERENCES messages(id),
                kind TEXT NOT NULL,
                agreement TEXT,
                sender TEXT NOT NULL,
                recipient TEXT NOT NULL,
                text TEXT NOT NULL,
                accepted_utc TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS recipient_seq ON messages(recipient, seq);
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                fin_id TEXT,
                fin_ack_id TEXT
            );
            PRAGMA user_version=1;
        """)
    os.chmod(path, 0o600)

def connect(path):
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def process(path, uid, request):
    require(uid in USERS, "unauthorized UID")
    require(type(request) is dict, "request must be an object")
    require(type(request.get("v")) is int and request["v"] == 1, "unsupported version")
    require(type(request.get("op")) is str, "operation must be text")
    sender = USERS[uid]
    if request.get("op") == "conversation":
        require(set(request) == {"v", "op", "conversation_id"}, "unexpected conversation fields")
        conv = request["conversation_id"]
        require(type(conv) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", conv), "invalid conversation ID")
        with contextlib.closing(connect(path)) as db:
            row = db.execute("""SELECT c.* FROM conversations c WHERE c.id=?
                AND EXISTS (SELECT 1 FROM messages m WHERE m.conversation_id=c.id
                AND (m.sender=? OR m.recipient=?))""", (conv, sender, sender)).fetchone()
        require(row is not None, "conversation not found")
        return {"v": 1, "ok": True, "conversation": dict(row)}
    if request.get("op") in {"receive", "history"}:
        history = request["op"] == "history"
        expected = {"v", "op", "after", "limit"} | ({"conversation_id"} if history else set())
        require(set(request) == expected, "unexpected or missing read fields")
        after, limit = request["after"], request["limit"]
        require(type(after) is int and 0 <= after <= 9223372036854775807, "invalid cursor")
        require(type(limit) is int and 1 <= limit <= 100, "invalid receive limit")
        conv = request.get("conversation_id")
        require(conv is None or type(conv) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", conv), "invalid conversation ID")
        with contextlib.closing(connect(path)) as db:
            if history:
                rows = db.execute("""SELECT * FROM messages WHERE (sender=? OR recipient=?)
                    AND seq>? AND (? IS NULL OR conversation_id=?) ORDER BY seq LIMIT ?""",
                    (sender, sender, after, conv, conv, limit)).fetchall()
            else:
                rows = db.execute("SELECT * FROM messages WHERE recipient=? AND seq>? ORDER BY seq LIMIT ?", (sender, after, limit)).fetchall()
        return {"v": 1, "ok": True, "messages": [dict(r) for r in rows],
                "next_cursor": rows[-1]["seq"] if rows else after}
    require(request.get("op") == "send", "invalid operation")
    require(set(request) == {"v", "op", "id", "conversation_id", "reply_to", "kind", "agreement", "text"}, "unexpected or missing send fields")
    mid, conv, target, kind, agreement, text = (request[k] for k in ("id", "conversation_id", "reply_to", "kind", "agreement", "text"))
    uuid_value(mid)
    require(type(conv) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", conv), "invalid conversation ID")
    require(type(text) is str and 0 < len(text.encode("utf-8")) <= MAX_TEXT, "text must contain 1..16384 UTF-8 bytes")
    require(type(kind) is str and kind in {"message", "ack", "fin", "fin_ack"}, "invalid kind")
    require(agreement is None or type(agreement) is str, "invalid agreement type")
    if kind == "ack":
        require(agreement in {"received", "agreed", "changes_requested"}, "ack requires agreement value")
    elif kind == "fin_ack":
        require(agreement == "agreed", "fin_ack requires agreed")
    else:
        require(agreement is None, "message/fin agreement must be null")
    if target is not None:
        uuid_value(target)
    if kind in {"ack", "fin_ack"}:
        require(target is not None, "ack/fin_ack requires reply_to")
    recipient = "zeul" if sender == "robin" else "robin"
    candidate = {"v": 1, "id": mid, "conversation_id": conv, "reply_to": target,
                 "kind": kind, "agreement": agreement, "sender": sender, "recipient": recipient, "text": text}
    with contextlib.closing(connect(path)) as db:
        with db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM messages WHERE id=?", (mid,)).fetchone()
            if old:
                require(all(old[k] == candidate[k] for k in FIELDS), "message ID conflict")
                return {"v": 1, "ok": True, "id": mid, "seq": old["seq"], "duplicate": True}
            prior = None
            if target is not None:
                prior = db.execute("SELECT * FROM messages WHERE id=?", (target,)).fetchone()
                require(prior is not None and prior["conversation_id"] == conv, "reply_to must exist in the same conversation")
            if kind in {"ack", "fin_ack"}:
                require(prior["sender"] == recipient, "ack must reference the other account")
                require(prior["kind"] not in {"ack", "fin_ack"}, "acknowledgment loops are forbidden")
            state = db.execute("SELECT * FROM conversations WHERE id=?", (conv,)).fetchone()
            require(state is None or state["state"] != "closed", "conversation closed; start a new conversation")
            if kind == "fin_ack":
                require(prior["kind"] == "fin", "fin_ack must reference fin")
                require(state and state["state"] == "fin_pending" and state["fin_id"] == target, "FIN is no longer current")
            if kind == "fin":
                require(state is None or state["state"] != "fin_pending", "FIN already pending")
            now = datetime.now(timezone.utc).isoformat()
            cols = list(candidate) + ["accepted_utc"]
            cur = db.execute("INSERT INTO messages (" + ",".join(cols) + ") VALUES (" + ",".join("?" for _ in cols) + ")", [*candidate.values(), now])
            seq = cur.lastrowid
            if state is None:
                db.execute("INSERT INTO conversations(id,state) VALUES (?, 'open')", (conv,))
            if kind == "fin":
                db.execute("UPDATE conversations SET state='fin_pending', fin_id=?, fin_ack_id=NULL WHERE id=?", (mid, conv))
            elif kind == "fin_ack":
                db.execute("UPDATE conversations SET state='closed', fin_ack_id=? WHERE id=?", (mid, conv))
            elif kind == "message" or agreement == "changes_requested":
                db.execute("UPDATE conversations SET state='open', fin_id=NULL, fin_ack_id=NULL WHERE id=?", (conv,))
        # Exiting the transaction commits before acceptance is returned.
        return {"v": 1, "ok": True, "id": mid, "seq": seq, "duplicate": False}

def read_frame(conn):
    deadline = time.monotonic() + TIMEOUT
    buf = bytearray()
    while True:
        left = deadline - time.monotonic()
        require(left > 0, "read timeout")
        conn.settimeout(left)
        chunk = conn.recv(min(4096, MAX_FRAME + 1 - len(buf)))
        require(bool(chunk), "incomplete frame")
        buf.extend(chunk)
        require(len(buf) <= MAX_FRAME, "request exceeds 65536 UTF-8 bytes")
        if b"\n" in buf:
            frame, tail = bytes(buf).split(b"\n", 1)
            require(not tail.strip(), "one request per connection")
            # Schema is flat. Bound nesting before the recursive JSON parser.
            depth = 0
            quoted = escaped = False
            for byte in frame:
                if quoted:
                    if escaped:
                        escaped = False
                    elif byte == 92:
                        escaped = True
                    elif byte == 34:
                        quoted = False
                elif byte == 34:
                    quoted = True
                elif byte in (91, 123):
                    depth += 1
                    require(depth <= 32, "JSON nesting exceeds 32 levels")
                elif byte in (93, 125):
                    depth -= 1
            return json.loads(frame.decode("utf-8"), object_pairs_hook=no_duplicate_keys,
                              parse_int=bounded_integer,
                              parse_constant=lambda value: (_ for _ in ()).throw(Invalid("non-finite number")))

def serve(path, socket_path):
    initialize(path)
    lock = (path.parent / "broker.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    require(socket_path.parent.is_dir() and socket_path.parent.stat().st_uid == os.getuid(), "invalid socket directory")
    subprocess.run(["/usr/bin/setfacl", "-b", "-k", str(socket_path.parent)], check=True)
    os.chmod(socket_path.parent, 0o700)
    subprocess.run(["/usr/bin/setfacl", "-m", "u:996:--x,m::--x", str(socket_path.parent)], check=True)
    if socket_path.exists():
        require(stat.S_ISSOCK(socket_path.lstat().st_mode) and socket_path.lstat().st_uid == os.getuid(), "refusing non-owned/non-socket path")
        socket_path.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    os.chmod(socket_path, 0o600)
    subprocess.run(["/usr/bin/setfacl", "-m", "u:996:rw-,m::rw-", str(socket_path)], check=True)
    server.listen(16)
    server.settimeout(1)
    stopping = False
    def stop(*_):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logging.info("ready: %s; allowed UIDs 1000,996; WAL/FULL", socket_path)
    try:
        while not stopping:
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            with conn:
                try:
                    _, uid, _ = struct.unpack("3i", conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                    require(uid in USERS, "unauthorized UID")
                    result = process(path, uid, read_frame(conn))
                except (ValueError, UnicodeError, RecursionError, socket.timeout) as e:
                    result = {"v": 1, "ok": False, "error": str(e)}
                except (sqlite3.Error, OSError):
                    logging.exception("storage/connection failure")
                    result = {"v": 1, "ok": False, "error": "storage or connection failure"}
                try:
                    conn.settimeout(TIMEOUT)
                    conn.sendall((json.dumps(result, ensure_ascii=False) + "\n").encode())
                except OSError:
                    pass  # A retry with the same ID recovers an uncertain acceptance.
    finally:
        server.close()
        socket_path.unlink(missing_ok=True)

if __name__ == "__main__":
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--socket", type=Path, default=Path("/srv/agent-channel/run/broker.sock"))
    p.add_argument("--db", type=Path, default=Path.home() / ".local/share/agent-channel/messages.sqlite3")
    args = p.parse_args()
    serve(args.db, args.socket)
