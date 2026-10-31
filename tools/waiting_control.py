"""Start/status/stop the independently authorized persistent waiter."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import sys
import time
import uuid
try:
    from . import waiting_service as service
except ImportError:
    import waiting_service as service
control=service.control

def status(root):
    path=root/"service.json"
    state=json.loads(path.read_text()) if path.exists() else {}
    observer=root/"observer.json"
    return {**state,"running":any(control.owned(r) for r in state.get("workers",[])),
            "observer":json.loads(observer.read_text()) if observer.exists() else {}}

def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir",type=Path,default=Path.home()/".local/state/agent-buzz/waiter")
    parser.add_argument("command",choices=["start","status","stop","self-test"])
    args=parser.parse_args();root=args.state_dir
    root.mkdir(parents=True,mode=0o700,exist_ok=True)
    if root.is_symlink() or root.stat().st_uid!=os.getuid() or root.stat().st_mode&0o077:
        raise SystemExit("Waiter directory must be private and owned")
    with (root/"manager.lock").open("a") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        config=json.loads((root/"config.json").read_text())
        current=status(root)
        if args.command=="self-test":
            if not current["running"]:raise SystemExit("Start the waiter before its self-test")
            control.atomic(root/"self-test.json",{"nonce":str(uuid.uuid4()),"self_test":True,
                "description":"Authorized local wakeup diagnostic; this is not peer input"})
        elif args.command=="stop":
            if current.get("thread") not in (None,config["thread"]):
                raise SystemExit("Waiter belongs to another goal")
            control.stop_workers(current)
            service.call([config["loop_command"],"stop","--thread",config["thread"]])
            control.atomic(root/"service.json",current)
        elif args.command=="start" and not current["running"]:
            if os.environ.get("HERDR_ENV")!="1":
                raise SystemExit("Start from the naturally inherited managed Herdr context")
            pane=service.call(["herdr","pane","current","--current"])["result"]["pane"]
            if pane.get("agent_session",{}).get("value")!=config["thread"]:
                raise SystemExit("Managed pane does not contain the configured Codex thread")
            with GoalSnapshot(config) as goal:
                if not service.should_wait(goal,config["objective_hash"]):
                    raise SystemExit("Goal is paused/completed/cleared or its objective changed")
            script=Path(__file__).resolve().with_name("waiting_service.py")
            argv=[sys.executable,str(script),"--state-dir",str(root)]
            fd=os.open(root/"service.log",os.O_WRONLY|os.O_APPEND|os.O_CREAT|os.O_NOFOLLOW,0o600)
            try:
                pid=os.posix_spawn(sys.executable,argv,os.environ.copy(),setsid=True,file_actions=[
                    (os.POSIX_SPAWN_OPEN,0,os.devnull,os.O_RDONLY,0),
                    (os.POSIX_SPAWN_DUP2,fd,1),(os.POSIX_SPAWN_DUP2,fd,2),
                    (os.POSIX_SPAWN_CLOSE,fd)])
            finally:os.close(fd)
            identity=control.identity(pid)
            if not identity:raise SystemExit("Waiter failed to start")
            control.atomic(root/"service.json",{"thread":config["thread"],"workers":[
                {"name":"waiter","pid":pid,"argv":argv,"start_ticks":identity["start_ticks"]}]})
            time.sleep(.2)
            if not status(root)["running"]:raise SystemExit("Waiter startup failed; inspect private log")
        result=status(root)
        # Report paths and health, not all notification manifests.
        result["observer"]={k:v for k,v in result["observer"].items()
            if k in {"mode","native_goal_status","last_success_epoch","last_error","notification_status"}}
        print(json.dumps(result,indent=2))

class GoalSnapshot:
    def __init__(self,config):self.config=config
    def __enter__(self):
        self.client=service.GoalClient(self.config["goal_socket"])
        return self.client.rpc("thread/goal/get",{"threadId":self.config["thread"]})["goal"]
    def __exit__(self,*_):self.client.close()

if __name__=="__main__":
    main()
