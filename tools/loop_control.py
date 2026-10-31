#!/usr/bin/python3
"""Control bounded read-only collectors; native Codex goals govern agent work."""
import argparse
from datetime import datetime,timezone
import fcntl
import json
import os
from pathlib import Path
import signal
import sys
import time
import uuid

def atomic(path,value):
    tmp=path.with_suffix(".tmp")
    with tmp.open("w") as handle:
        json.dump(value,handle,indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(path)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)

def identity(pid):
    try:
        proc=Path("/proc")/str(pid)
        fields=(proc/"stat").read_text().rsplit(")",1)[1].split()
        return {"uid":proc.stat().st_uid,"start_ticks":fields[19],
                "argv":[p.decode() for p in (proc/"cmdline").read_bytes().split(b"\0") if p],
                "state":fields[0]}
    except (OSError,ValueError,IndexError,UnicodeError):
        return None

def owned(record):
    current=identity(record["pid"])
    return bool(current and current["state"]!="Z" and current["uid"]==os.getuid()
                and current["start_ticks"]==record["start_ticks"] and current["argv"]==record["argv"])

def stop_workers(state):
    for record in state.get("workers",[]):
        if owned(record) and os.getpgid(record["pid"])==record["pid"]:
            os.killpg(record["pid"],signal.SIGTERM)
    deadline=time.monotonic()+2
    while any(owned(r) for r in state.get("workers",[])) and time.monotonic()<deadline:
        time.sleep(.02)
    for record in state.get("workers",[]):
        if owned(record) and os.getpgid(record["pid"])==record["pid"]:
            os.killpg(record["pid"],signal.SIGKILL)
        try:
            os.waitpid(record["pid"],os.WNOHANG)
        except ChildProcessError:
            pass
    state["stopped_utc"]=datetime.now(timezone.utc).isoformat()

def start_workers(root,state,thread,seconds,scripts,grace=.2):
    live=[r for r in state.get("workers",[]) if owned(r)]
    if live and state.get("goal_thread")!=thread:
        raise ValueError("A different goal owns this loop; refusing to take it over")
    if len(live)==2 and state.get("expires_epoch",0)>time.time():
        return state
    for script in scripts.values():
        if not script.is_file():
            raise ValueError("Collector script does not exist: "+str(script))
    stop_workers(state)
    state={"goal_thread":thread,"seconds":seconds,"started_utc":datetime.now(timezone.utc).isoformat(),
           "expires_epoch":time.time()+seconds,"workers":[],
           "goal_status":"Native goal status is verified separately; this record is not goal invocation proof"}
    try:
        for name,script in scripts.items():
            argv=[sys.executable,str(script.resolve()),"--seconds",str(seconds),"--stream"]
            log=root/(name+".log")
            fd=os.open(log,os.O_WRONLY|os.O_CREAT|os.O_APPEND|os.O_NOFOLLOW,0o600)
            try:
                pid=os.posix_spawn(sys.executable,argv,os.environ.copy(),setsid=True,file_actions=[
                    (os.POSIX_SPAWN_OPEN,0,os.devnull,os.O_RDONLY,0),
                    (os.POSIX_SPAWN_DUP2,fd,1),(os.POSIX_SPAWN_DUP2,fd,2),
                    (os.POSIX_SPAWN_CLOSE,fd)])
            finally:
                os.close(fd)
            current=identity(pid)
            if not current:
                raise RuntimeError("Collector failed before identity capture: "+name)
            state["workers"].append({"name":name,"pid":pid,"argv":argv,
                                     "start_ticks":current["start_ticks"],"log":str(log)})
            atomic(root/"control.json",state)
        time.sleep(grace)
        if not all(owned(r) for r in state["workers"]):
            raise RuntimeError("Collector startup failed; inspect the private logs")
        atomic(root/"control.json",state)
        return state
    except Exception:
        stop_workers(state)
        atomic(root/"control.json",state)
        raise

def report(state):
    workers=[{**r,"running":owned(r)} for r in state.get("workers",[])]
    return {**state,"workers":workers,"running":len(workers)==2 and all(r["running"] for r in workers)}

def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir",type=Path,default=Path.home()/".local/state/agent-buzz/loop")
    commands=parser.add_subparsers(dest="command",required=True)
    start=commands.add_parser("start")
    start.add_argument("--thread",required=True)
    start.add_argument("--seconds",type=int,default=900)
    start.add_argument("--mail-script",type=Path,default=Path.home()/".local/state/agent-channel/wait-mail.py")
    start.add_argument("--github-script",type=Path,default=Path(__file__).resolve().with_name("watch_github.py"))
    commands.add_parser("status")
    stop=commands.add_parser("stop")
    stop.add_argument("--thread",required=True)
    args=parser.parse_args()
    if sys.platform!="linux":
        parser.error("This controller uses Linux process identity; use native goal/loop tools on other hosts")
    root=args.state_dir
    root.mkdir(parents=True,mode=0o700,exist_ok=True)
    if root.is_symlink() or root.stat().st_uid!=os.getuid() or root.stat().st_mode&0o077:
        parser.error("Control directory must be private and owned by this account")
    with (root/"control.lock").open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        path=root/"control.json"
        state=json.loads(path.read_text()) if path.exists() else {}
        try:
            if args.command in {"start","stop"}:
                if str(uuid.UUID(args.thread))!=args.thread:
                    raise ValueError("Thread must be a canonical UUID")
            if args.command=="start":
                if not 1<=args.seconds<=900:
                    raise ValueError("Window must be 1..900 seconds")
                state=start_workers(root,state,args.thread,args.seconds,{"mail":args.mail_script,"github":args.github_script})
            elif args.command=="stop":
                if state and state.get("goal_thread")!=args.thread:
                    raise ValueError("Loop belongs to a different goal; refusing to stop it")
                stop_workers(state)
                atomic(path,state)
            print(json.dumps(report(state),indent=2))
            return 0
        except (ValueError,RuntimeError,OSError) as error:
            print(json.dumps({"error":str(error),"running":report(state)["running"]}))
            return 1

if __name__=="__main__":
    sys.exit(main())
