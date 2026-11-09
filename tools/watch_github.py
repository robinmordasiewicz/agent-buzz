#!/usr/bin/env python3
"""Read-only, bounded active-session GitHub collector. Never posts or ACKs."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

BUDGET_DEADLINE=None
class WindowEnded(RuntimeError):
    pass

ISSUE = """id number title state updatedAt url comments(last:1){nodes{id updatedAt}}"""
THREAD = """nodes{id isResolved comments(last:1){nodes{id updatedAt}}} pageInfo{hasNextPage endCursor}"""
PR_META = ISSUE + """ headRefOid reviewDecision isDraft
 commits(last:1){nodes{commit{statusCheckRollup{state}}}}"""
CHECKS = """nodes{__typename
 ... on CheckRun{id name status conclusion detailsUrl startedAt completedAt checkSuite{app{databaseId slug}}}
 ... on StatusContext{id context state targetUrl creator{login}}
 } pageInfo{hasNextPage endCursor}"""
PR = ISSUE + """ headRefOid reviewDecision isDraft
 reviews(last:1){nodes{id state submittedAt body}}
 reviewThreads(first:100){""" + THREAD + """}
 commits(last:1){nodes{commit{oid statusCheckRollup{state contexts(first:100){"""+CHECKS+"""}}}}}"""
QUERY = """query($owner:String!,$name:String!,$ia:String,$pa:String){
 repository(owner:$owner,name:$name){
 issues(first:50,after:$ia,orderBy:{field:UPDATED_AT,direction:DESC}){
 nodes{""" + ISSUE + """} pageInfo{hasNextPage endCursor}}
 pullRequests(first:50,after:$pa,orderBy:{field:UPDATED_AT,direction:DESC}){
 nodes{""" + PR_META + """} pageInfo{hasNextPage endCursor}}
 } rateLimit{cost remaining resetAt}}"""

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":")).encode()).hexdigest()
def atomic(path,value):
    tmp=path.with_suffix(".tmp")
    with tmp.open("w") as handle:
        handle.write(json.dumps(value,indent=2)+"\n")
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(path)
    descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
def gh(args,payload=None):
    left=30 if BUDGET_DEADLINE is None else BUDGET_DEADLINE-time.monotonic()
    if left<=0:
        raise WindowEnded("Active window expired before API call")
    r=subprocess.run(["gh","api",*args],input=json.dumps(payload) if payload else None,
                     text=True,capture_output=True,timeout=min(30,left))
    if r.returncode:
        raise RuntimeError("GitHub API failed; checkpoint retained (exit "+str(r.returncode)+")")
    return json.loads(r.stdout)
def graphql(query,variables):
    result=gh(["graphql","--input","-"],{"query":query,"variables":variables})
    if result.get("errors"):
        raise RuntimeError("GitHub GraphQL error; checkpoint retained")
    return result["data"]
def pages(endpoint):
    return [item for page in gh(["--paginate","--slurp",endpoint]) for item in page]

def inventory(repo):
    owner,name=repo.split("/")
    variables={"owner":owner,"name":name,"ia":None,"pa":None}
    result=graphql(QUERY,variables)
    data=result["repository"]
    if data is None:
        raise RuntimeError("Repository not visible to this account")
    found={}
    rate=dict(result["rateLimit"])
    total_cost=rate["cost"]
    for connection,kind,cursor in (("issues","issue","ia"),("pullRequests","pr","pa")):
        page=data[connection]
        while True:
            for node in page["nodes"]:
                if kind=="pr":
                    detail=graphql("""query($owner:String!,$name:String!,$number:Int!){
                     repository(owner:$owner,name:$name){pullRequest(number:$number){"""+PR+"""}}
                     rateLimit{cost remaining resetAt}}""",
                     {"owner":owner,"name":name,"number":node["number"]})
                    node=detail["repository"]["pullRequest"]
                    rate=detail["rateLimit"]
                    total_cost+=rate["cost"]
                    commits=node["commits"]["nodes"]
                    if commits and commits[-1]["commit"]["statusCheckRollup"]:
                        commit=commits[-1]["commit"]
                        checks=commit["statusCheckRollup"]["contexts"]
                        contexts=list(checks["nodes"])
                        while checks["pageInfo"]["hasNextPage"]:
                            extra=graphql("""query($owner:String!,$name:String!,$oid:GitObjectID!,$after:String!){
                             repository(owner:$owner,name:$name){object(oid:$oid){... on Commit{
                              statusCheckRollup{contexts(first:100,after:$after){"""+CHECKS+"""}}}}}
                             rateLimit{cost remaining resetAt}}""",
                             {"owner":owner,"name":name,"oid":commit["oid"],"after":checks["pageInfo"]["endCursor"]})
                            checks=extra["repository"]["object"]["statusCheckRollup"]["contexts"]
                            contexts.extend(checks["nodes"])
                            rate=extra["rateLimit"]
                            total_cost+=rate["cost"]
                        commit["statusCheckRollup"]["contexts"]={"nodes":contexts}
                    thread_page=node["reviewThreads"]
                    threads=list(thread_page["nodes"])
                    while thread_page["pageInfo"]["hasNextPage"]:
                        extra=graphql("""query($owner:String!,$name:String!,$number:Int!,$after:String!){
                          repository(owner:$owner,name:$name){pullRequest(number:$number){
                           reviewThreads(first:100,after:$after){"""+THREAD+"""}
                          }} rateLimit{cost remaining resetAt}}""",
                          {"owner":owner,"name":name,"number":node["number"],"after":thread_page["pageInfo"]["endCursor"]})
                        thread_page=extra["repository"]["pullRequest"]["reviewThreads"]
                        rate=extra["rateLimit"]
                        total_cost+=rate["cost"]
                        threads.extend(thread_page["nodes"])
                    node["reviewThreads"]={"nodes":threads}
                found[kind+":"+str(node["number"])]=node
            if not page["pageInfo"]["hasNextPage"]:
                break
            v={**variables,cursor:page["pageInfo"]["endCursor"]}
            extra=graphql(QUERY,v)
            rate=extra["rateLimit"]
            total_cost+=rate["cost"]
            page=extra["repository"][connection]
    return found,{**rate,"cost":total_cost}

def collect(repo,state,root):
    snapshot,rate=inventory(repo)
    events=[]
    def emit(kind,key,value,url,actor=None):
        fingerprint=digest(value)
        prior=state["seen"].get(key)
        if type(prior) is str:  # Migrate an older content-only checkpoint.
            prior={"fingerprint":prior,"event_id":digest([key,prior])}
        if prior and prior["fingerprint"]==fingerprint:
            return
        previous_id=prior["event_id"] if prior else None
        event_id=digest([key,previous_id,fingerprint])
        record={"event_id":event_id,"kind":kind,"key":key,"url":url,"actor":actor,
                "previous_event_id":previous_id,"detected_utc":datetime.now(timezone.utc).isoformat(),"value":value}
        # Durable event file precedes the collector checkpoint.
        path=root/"events"/(event_id+".json")
        if not path.exists():
            atomic(path,record)
        state["seen"][key]={"fingerprint":fingerprint,"event_id":event_id}
        events.append(record)
    for key,node in snapshot.items():
        fingerprint=digest(node)
        if state["snapshots"].get(key)==fingerprint:
            continue
        number=node["number"]
        detail=gh(["repos/"+repo+"/issues/"+str(number)])
        emit("topic",key+":topic",detail,detail["html_url"],detail["user"]["login"])
        for comment in pages("repos/"+repo+"/issues/"+str(number)+"/comments?per_page=100"):
            emit("comment","comment:"+str(comment["id"]),comment,comment["html_url"],comment["user"]["login"])
        if key.startswith("pr:"):
            for review in pages("repos/"+repo+"/pulls/"+str(number)+"/reviews?per_page=100"):
                emit("review","review:"+str(review["id"]),review,review["html_url"],review["user"]["login"])
            for comment in pages("repos/"+repo+"/pulls/"+str(number)+"/comments?per_page=100"):
                emit("review_comment","review_comment:"+str(comment["id"]),comment,comment["html_url"],comment["user"]["login"])
            emit("pr_state",key+":state",node,node["url"])
        state["snapshots"][key]=fingerprint
    state["last_checked_utc"]=datetime.now(timezone.utc).isoformat()
    state["rate_limit"]=rate
    atomic(root/"collector.json",state)
    with (root/"checks.jsonl").open("a") as audit:
        audit.write(json.dumps({"checked_utc":state["last_checked_utc"],"cost":rate["cost"],"remaining":rate["remaining"]})+"\n")
    return events,rate

def main():
    global BUDGET_DEADLINE
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo",default="robinmordasiewicz/agent-buzz")
    parser.add_argument("--state-dir",type=Path,default=Path.home()/".local/state/agent-buzz/github")
    parser.add_argument("--seconds",type=int,default=55)
    parser.add_argument("--stream",action="store_true")
    args=parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+",args.repo) or not 1<=args.seconds<=900:
        parser.error("Invalid repository or window (1..900 seconds)")
    os.umask(0o077)
    root=args.state_dir
    root.mkdir(parents=True,mode=0o700,exist_ok=True)
    if root.is_symlink() or root.stat().st_uid!=os.getuid() or root.stat().st_mode&0o077:
        raise SystemExit("Collector directory must be private and owned by this account")
    lock=(root/"collector.lock").open("a")
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    (root/"events").mkdir(mode=0o700,exist_ok=True)
    state=json.loads((root/"collector.json").read_text()) if (root/"collector.json").exists() else {"repo":args.repo,"snapshots":{},"seen":{}}
    if state["repo"]!=args.repo:
        raise SystemExit("Use a separate state directory for each repository")
    deadline=time.monotonic()+args.seconds
    BUDGET_DEADLINE=deadline
    interval=5
    announced=set()
    while True:
        started=time.monotonic()
        if started>=deadline:
            print(json.dumps({"event":"waiting","last_checked":state.get("last_checked_utc")}),flush=True)
            break
        try:
            events,rate=collect(args.repo,state,root)
            # Show pending events again after reconnect; processing is separate.
            processed=set(json.loads((root/"processed.json").read_text())) if (root/"processed.json").exists() else set()
            for path in sorted((root/"events").glob("*.json")):
                if path.stem in processed or path.stem in announced:
                    continue
                record=json.loads(path.read_text())
                print(json.dumps({"event":"github","event_id":record["event_id"],"kind":record["kind"],
                                  "url":record["url"],"actor":record["actor"],"path":str(path)}),flush=True)
                announced.add(path.stem)
            interval=max(5,rate["cost"]*3600/4000)
            if rate["remaining"]<100:
                interval=60
                print(json.dumps({"event":"rate_backoff","seconds":interval}),flush=True)
            if announced and not args.stream:
                break
        except WindowEnded:
            print(json.dumps({"event":"waiting","last_checked":state.get("last_checked_utc")}),flush=True)
            break
        except (RuntimeError,ValueError,KeyError,OSError,subprocess.TimeoutExpired) as error:
            if time.monotonic()>=deadline:
                print(json.dumps({"event":"waiting","last_checked":state.get("last_checked_utc")}),flush=True)
                break
            interval=min(max(10,interval*2),60)
            print(json.dumps({"event":"poll_error","error":str(error),"backoff_seconds":interval}),flush=True)
        remaining=deadline-time.monotonic()
        if remaining<=0:
            print(json.dumps({"event":"waiting","last_checked":state.get("last_checked_utc")}),flush=True)
            break
        time.sleep(min(max(0,started+interval-time.monotonic()),remaining))
    lock.close()
    BUDGET_DEADLINE=None

if __name__=="__main__":
    main()
