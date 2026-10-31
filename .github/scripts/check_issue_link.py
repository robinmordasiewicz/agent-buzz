#!/usr/bin/env python3
"""Require GitHub's actual same-repository closing-issue relation for PRs."""
import json
import os
from pathlib import Path
import urllib.request

event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
if os.environ["GITHUB_EVENT_NAME"] == "push":
    # Source commits on main have already traversed the protected PR gate.
    print("Post-merge push: issue relationship was checked on the pull request.")
    raise SystemExit(0)
if "pull_request" not in event:
    raise SystemExit("Unsupported event; refusing to skip the required issue gate")
repo = os.environ["GITHUB_REPOSITORY"]
owner, name = repo.split("/")
query = """query($owner:String!,$name:String!,$number:Int!){
 repository(owner:$owner,name:$name){pullRequest(number:$number){
  baseRefName closingIssuesReferences(first:100){nodes{
   number state repository{nameWithOwner}
  }}
 }}
}"""
payload = {"query": query, "variables": {"owner": owner, "name": name, "number": event["number"]}}
request = urllib.request.Request(
    "https://api.github.com/graphql", data=json.dumps(payload).encode(),
    headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"],
             "Content-Type": "application/json", "User-Agent": "agent-buzz-issue-gate"})
with urllib.request.urlopen(request, timeout=30) as response:
    data = json.load(response)
if data.get("errors"):
    raise SystemExit("GitHub issue relationship query failed")
pr = data["data"]["repository"]["pullRequest"]
if pr["baseRefName"] != "main":
    raise SystemExit("This governed PR must target main")
linked = [n for n in pr["closingIssuesReferences"]["nodes"]
          if n["repository"]["nameWithOwner"].lower() == repo.lower() and n["state"] == "OPEN"]
if not linked:
    raise SystemExit("PR must close an open issue in this repository (for example, Closes #1)")
print("Verified GitHub closing-issue references:", ", ".join("#" + str(n["number"]) for n in linked))
