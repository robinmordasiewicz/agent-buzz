# Working together

Start with an issue that states the problem, intended outcome and acceptance
evidence. Use a feature branch and a PR with a closing reference such as
Closes #1. The issue-link check queries GitHub's actual closing relationship,
so mentioning an issue in a code block or linking an unrelated repository does
not satisfy the gate.

The author applies changes. The other person independently reviews the code,
tests, design and limitations. A reviewer's suggestions should be applied by
the author so the latest push still has an independent eligible reviewer.
Workflow and governance changes need the same scrutiny as runtime code.

Main requires one approving review, dismissal of stale approvals, approval of
the latest reviewable push, resolved review discussions, up-to-date tests and
issue-link checks, and linear history. Rules include administrators; force
pushes and deletion are disabled. Required checks are bound to GitHub Actions
after their first execution identifies the expected app.

CI uses a pinned checkout action, read-only permissions and no persistent Git
credentials. Never run untrusted PR code with privileged credentials. Two
trusted collaborators still depend on careful independent review; a repository
owner able to change settings remains a trusted administrator.

Run locally using Python 3.12 or newer:

    python3 -m unittest discover -s . -p 'test_*.py' -v

Unit tests exercise logic using temporary databases and explicit fixture roles.
Passing CI does not prove deployment UID/ACL isolation or hardware durability.
Record live multi-user, crash/restart, concurrency and recovery results on the
issue separately from source approval.

Recover changes through a reviewed revert PR. Do not automatically disable
protections for a blocked merge. Exceptional administrative changes require an
explicit owner decision and a project record.
