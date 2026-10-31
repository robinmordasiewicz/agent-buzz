# Goal-owned development loop

The native Codex goal owns the objective, continuation and evidence-based
completion. The collectors only gather messages/events. Starting a collector
does not invoke a goal, establish peer agreement, approve a PR, or authorize
executing received text.

For this project, the goal is a well-planned, independently reviewed
communication implementation: agree each increment, implement through linked
protected-main PRs, verify end to end, measure results and iterate. Preserve
each owner's permissions and the source/live-acceptance distinction.

Each assistant must explicitly report its actual goal invocation, objective,
observed active/pending status and scope alignment. An intention to start or a
successful email send is insufficient. If owner authorization is missing,
record that blocker rather than claiming activation.

## Linux collector control

    python3 tools/loop_control.py start --thread CODEX_THREAD_UUID
    python3 tools/loop_control.py status
    python3 tools/loop_control.py stop --thread CODEX_THREAD_UUID

A local installation may expose the same command as agent-buzz-loop.
Windows are 1..900 seconds, with a five-second target in the underlying
collectors and error/rate backoff. The native goal renews expired windows
explicitly while work remains authorized. Starting twice reuses live workers;
a different goal cannot take over their control record.

Control state and appended logs are private under
~/.local/state/agent-buzz/loop. Check actual fresh successful email/GitHub poll
timestamps as well as process health. A process may be alive while an API is
failing. The controller does not query native goal status; goal verification is
separate.

Workers are bounded, read-only processes, not a new perpetual AI worker or a
boot scheduler. Goal continuations must stop them before completion, pause,
blocking, or ending the active collaboration window. A window still expires if
the controlling turn is interrupted. The current mail script must be supplied
from the account's own authorized setup; no credentials or other user's mail
configuration are copied.

Start/stop is serialized. Registered worker identity checks include Linux UID,
process start ticks and exact argv; stale records do not signal a reused PID.
Stop targets only registered matching process groups, preserving event stores,
transcripts and journals. The controller rolls back its other worker if one
fails immediately at startup. It does not adopt or stop independently launched
collectors.

On another operating system, use that assistant's supported native goal/loop
facilities or an independently reviewed equivalent. Assess polling, durable
records, bounded work, health and permission boundaries before claiming parity.
