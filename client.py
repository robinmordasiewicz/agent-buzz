#!/usr/bin/env python3
"""Explicit durable send/retry/receive/history/sync/checkpoint; no execution."""
import argparse
import contextlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import stat
import struct
import sys
import time
import uuid

USERS = {1000: "robin", 996: "zeul"}
def check(condition, message):
    if not condition:
        raise ValueError(message)
def valid_id(value):
    check(type(value) is str and str(uuid.UUID(value)) == value, "invalid UUID")

def validate_response(request, response):
    check(type(response) is dict and type(response.get("v")) is int and response["v"] == 1
          and type(response.get("ok")) is bool, "invalid response envelope")
    if not response["ok"]:
        check(type(response.get("error")) is str, "invalid error response")
        return
    op = request["op"]
    if op == "send":
        check(response.get("id") == request["id"] and type(response.get("seq")) is int
              and response["seq"] > 0 and type(response.get("duplicate")) is bool, "invalid acceptance")
    elif op == "conversation":
        conv = response.get("conversation")
        check(type(conv) is dict and set(conv) == {"id","state","fin_id","fin_ack_id"}
              and conv["id"] == request["conversation_id"]
              and conv["state"] in {"open","fin_pending","closed"}, "invalid conversation state")
        for key in ("fin_id", "fin_ack_id"):
            if conv[key] is not None:
                valid_id(conv[key])
        check(conv["state"] != "closed" or conv["fin_id"] and conv["fin_ack_id"], "incomplete closure")
    else:
        rows = response.get("messages")
        check(type(rows) is list and len(rows) <= request["limit"], "invalid message page")
        last = request["after"]
        own = USERS[os.getuid()]
        for row in rows:
            check(type(row) is dict and set(row) == {"seq","v","id","conversation_id","reply_to","kind","agreement","sender","recipient","text","accepted_utc"}, "invalid stored message")
            check(type(row["seq"]) is int and row["seq"] > last and type(row["v"]) is int and row["v"] == 1, "invalid sequence")
            valid_id(row["id"])
            if row["reply_to"] is not None:
                valid_id(row["reply_to"])
            check(row["sender"] in USERS.values() and row["recipient"] in USERS.values()
                  and row["sender"] != row["recipient"] and own in (row["sender"],row["recipient"]), "invalid participants")
            check(op != "receive" or row["recipient"] == own, "recipient isolation violation")
            check(type(row["conversation_id"]) is str and type(row["text"]) is str
                  and 0 < len(row["text"].encode()) <= 16384 and type(row["accepted_utc"]) is str, "invalid message content")
            check(row["kind"] in {"message","ack","fin","fin_ack"}, "invalid kind")
            check((row["kind"] in {"message","fin"} and row["agreement"] is None)
                  or (row["kind"] == "ack" and row["agreement"] in {"received","agreed","changes_requested"} and row["reply_to"])
                  or (row["kind"] == "fin_ack" and row["agreement"] == "agreed" and row["reply_to"]), "invalid agreement")
            check(request.get("conversation_id") is None or row["conversation_id"] == request["conversation_id"], "conversation filter violation")
            last = row["seq"]
        check(type(response.get("next_cursor")) is int and response["next_cursor"] == last, "invalid next cursor")

def exchange(path, request):
    check(os.getuid() in USERS, "client is limited to the two configured accounts")
    raw = (json.dumps(request, ensure_ascii=False) + "\n").encode("utf-8")
    check(len(raw) <= 65536, "request exceeds 65536 UTF-8 bytes")
    deadline = time.monotonic() + 15
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(10)
        conn.connect(path)
        _, server_uid, _ = struct.unpack("3i", conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        check(server_uid == 1000, "unexpected broker UID")
        conn.sendall(raw)
        response = bytearray()
        while b"\n" not in response:
            left = deadline - time.monotonic()
            check(left > 0, "response deadline exceeded")
            conn.settimeout(left)
            chunk = conn.recv(65536)
            if not chunk:
                raise ConnectionError("incomplete response; retry send using the same ID")
            response.extend(chunk)
            check(len(response) <= 16 * 1024 * 1024, "response too large")
    frame, tail = response.split(b"\n", 1)
    check(not tail.strip(), "multiple response frames")
    result = json.loads(frame)
    validate_response(request, result)
    return result

def journal(directory):
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    st = directory.lstat()
    check(stat.S_ISDIR(st.st_mode) and st.st_uid == os.getuid() and stat.S_IMODE(st.st_mode) == 0o700, "journal directory must be owned/private 0700")
    path = directory / "client.sqlite3"
    check(not path.is_symlink(), "journal cannot be a symlink")
    if path.exists():
        st = path.stat()
        check(st.st_uid == os.getuid() and stat.S_IMODE(st.st_mode) == 0o600, "journal must be owned/private 0600")
    db = sqlite3.connect(path, timeout=5)
    os.chmod(path, 0o600)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, request TEXT NOT NULL, result TEXT);
        CREATE TABLE IF NOT EXISTS cache(seq INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE, message TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS cursors(name TEXT PRIMARY KEY, seq INTEGER NOT NULL);
    """)
    return db

def cursor(db, name):
    row = db.execute("SELECT seq FROM cursors WHERE name=?", (name,)).fetchone()
    return row[0] if row else 0
def cache(db, response):
    for row in response["messages"]:
        serialized = json.dumps(row, sort_keys=True, ensure_ascii=False)
        previous = db.execute("SELECT message FROM cache WHERE seq=? OR id=?", (row["seq"],row["id"])).fetchall()
        check(all(item[0] == serialized for item in previous), "immutable history conflict")
        db.execute("INSERT OR IGNORE INTO cache VALUES(?,?,?)", (row["seq"],row["id"],serialized))
def send(db, path, request):
    valid_id(request["id"])
    serialized = json.dumps(request, sort_keys=True, ensure_ascii=False)
    with db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT request FROM outbox WHERE id=?", (request["id"],)).fetchone()
        check(old is None or old[0] == serialized, "local message ID conflict")
        db.execute("INSERT OR IGNORE INTO outbox(id,request) VALUES(?,?)", (request["id"],serialized))
    result = exchange(path, request)
    with db:
        db.execute("UPDATE outbox SET result=? WHERE id=?", (json.dumps(result),request["id"]))
    return result

def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", default="/srv/agent-channel/run/broker.sock")
    parser.add_argument("--state-dir", type=Path, default=Path.home()/".local/state/agent-channel-client")
    sub = parser.add_subparsers(dest="op", required=True)
    s = sub.add_parser("send")
    s.add_argument("--id", required=True)
    s.add_argument("--conversation", required=True)
    s.add_argument("--reply-to")
    s.add_argument("--kind", choices=["message","ack","fin","fin_ack"], default="message")
    s.add_argument("--agreement", choices=["received","agreed","changes_requested"])
    content = s.add_mutually_exclusive_group(required=True)
    content.add_argument("--text")
    content.add_argument("--text-file", type=Path)
    retry = sub.add_parser("retry")
    retry.add_argument("--id", required=True)
    for name in ("receive","history"):
        r = sub.add_parser(name)
        r.add_argument("--after", type=int, default=None if name == "receive" else 0)
        r.add_argument("--limit", type=int, default=100)
        if name == "history":
            r.add_argument("--conversation")
    sub.add_parser("sync")
    c = sub.add_parser("conversation")
    c.add_argument("--conversation", required=True)
    checkpoint = sub.add_parser("checkpoint")
    checkpoint.add_argument("--after", type=int, required=True)
    args = parser.parse_args()
    try:
        with contextlib.closing(journal(args.state_dir)) as db:
            if args.op == "send":
                text = args.text_file.read_text(encoding="utf-8") if args.text_file else args.text
                req = {"v":1,"op":"send","id":args.id,"conversation_id":args.conversation,"reply_to":args.reply_to,
                       "kind":args.kind,"agreement":args.agreement,"text":text}
                result = send(db,args.socket,req)
            elif args.op == "retry":
                old = db.execute("SELECT request FROM outbox WHERE id=?", (args.id,)).fetchone()
                check(old is not None, "no journaled request for ID")
                result = send(db,args.socket,json.loads(old[0]))
            elif args.op == "checkpoint":
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    check(type(args.after) is int and args.after >= cursor(db,"processed"), "checkpoint cannot regress")
                    row = db.execute("SELECT message FROM cache WHERE seq=?", (args.after,)).fetchone()
                    check(args.after == 0 or row is not None and json.loads(row[0])["recipient"] == USERS[os.getuid()], "checkpoint must identify a cached incoming message")
                    db.execute("INSERT OR REPLACE INTO cursors VALUES('processed',?)", (args.after,))
                result = {"v":1,"ok":True,"processed_cursor":args.after}
            elif args.op == "sync":
                total = 0
                while True:
                    page = exchange(args.socket,{"v":1,"op":"history","after":cursor(db,"history"),"limit":100,"conversation_id":None})
                    if not page["ok"]:
                        result = page
                        break
                    with db:
                        cache(db,page)
                        db.execute("INSERT INTO cursors VALUES('history',?) ON CONFLICT(name) DO UPDATE SET seq=MAX(seq,excluded.seq)", (page["next_cursor"],))
                    total += len(page["messages"])
                    if not page["messages"]:
                        result = {"v":1,"ok":True,"cached":total,"history_cursor":cursor(db,"history"),"processed_cursor":cursor(db,"processed")}
                        break
            else:
                req = {"v":1,"op":args.op}
                if args.op == "conversation":
                    req["conversation_id"] = args.conversation
                else:
                    req.update(after=cursor(db,"processed") if args.after is None else args.after,limit=args.limit)
                    if args.op == "history":
                        req["conversation_id"] = args.conversation
                result = exchange(args.socket,req)
                if result["ok"] and args.op in {"receive","history"}:
                    with db:
                        cache(db,result)
        print(json.dumps(result,ensure_ascii=False,indent=2))
        return 0 if result["ok"] else 1
    except (OSError,ValueError,ConnectionError,sqlite3.Error,KeyError,TypeError,RecursionError) as e:
        print(json.dumps({"v":1,"ok":False,"error":str(e),"recovery":"A send may be pending in your journal. Retry the same ID; never replace an uncertain send's ID."}))
        return 2

if __name__ == "__main__":
    sys.exit(main())
