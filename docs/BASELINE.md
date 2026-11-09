# Isolated unchanged-source baseline

This is experiment tooling for #8, following the concrete #5 plan and explicit
peer ACK. It adds no optimized wire protocol or production service. Independent
review of the exact harness head and two explicit readiness acknowledgments
must precede shared setup/execution. Unit fixtures are not live two-account
or model-wakeup certification.

The pinned broker/client baseline is main commit
`4d70e07a8eb24a218c715a40cc3d96b5e386ec36`. The harness verifies those two
files against their recorded SHA-256 values before importing the client or
starting the broker. Installed `/srv/agent-channel` files can supply this
unchanged core even when the harness itself is a later reviewed commit.
Use system Python 3.12 on Linux with pidfd support for execution. Summary
analysis can run separately; it does not invoke an agent.

## Scope and identities

Robin runs the isolated broker and Robin participant as real UID 1000. Zeul
runs the Zeul participant as real UID 996 through his own authorized access.
The harness refuses other execution identities. It never copies credentials,
changes normal journals or consumes the collaboration listener's cursors.

Use a fresh canonical UUID4 for every attempt. A private per-owner registry
rejects reuse of a role/run even if a different output directory is selected.
The broker and Robin participant are distinct roles within one run; internal
fast-consumer recovery intentionally reopens its original journal.

The only shared fixture directory is
`/srv/agent-channel/run/baseline-RUN_UUID`, containing `broker.sock`. This is a
new child of the existing Robin-owned shared socket area. The unchanged broker
sets ACLs on that dedicated child/socket only. The database, outbox, consumer
journals and raw evidence remain in each owner's private state. No synthetic
message reaches either real model listener. Existing services, sockets,
permissions and collaboration state are not changed by the harness.

## Workload and evidence

Each sender emits eight warm-up messages, then 256 measured messages at a
target 20 messages/second: 528 unique accepted IDs across both directions.
Payload text is exactly 512 UTF-8 bytes. IDs are deterministic within the fresh
run, with disjoint owner/phase/index sets. Eight measured IDs per sender are
retried with the exact payload and must return duplicate=true and the original
sequence. Retry/warm-up timings are excluded from measured quantiles.

Each account has independent fast and paused consumers. Fast polls target
100 ms; paused starts 15 seconds after the measured burst begins. Fast exits
after caching a page containing at least 64 measured IDs, before synthetic
processing/checkpoint. Its supervisor restarts it once from the same journal.
Pending IDs must replay, and the paused consumer's journal must still be
untouched during this interruption. If that pause interval has already ended,
the independence observation is inconclusive and the run fails rather than
inventing evidence. Each consumer must match the entire expected incoming ID
set, including warm-ups, and report measured timing separately.

Processing is a **synthetic test event**, never model processing. Checkpoints
use the unchanged client's actual CLI; observation time precedes cache/sample
logging, and checkpoint time includes CLI and instrumentation overhead.
Sender time covers unchanged client.send including durable outbox work,
excluding subsequent sample logging. The pinned broker generates accepted_utc
before INSERT and before the transaction commits. Stored-acceptance-timestamp
to observation/checkpoint intervals therefore include an unknown pre-commit
interval. They are not post-commit delivery latency. The summaries explicitly
report exact post_commit_latency_ms as unavailable (null) for this unchanged
baseline; no completed-commit timestamp is inferred. The common workstation
wall clock supplies those stored/receive/checkpoint timestamps; sender elapsed
time uses a monotonic clock. Report nearest-rank empirical p50/p95/p99, maximum
and sample count with the exact boundary labels.
These samples provide no sustained tail or latency guarantee.
Requested versus achieved sender/poll pacing and pacing lag are reported.
Negative wall-clock deltas remain visible as negative_samples rather than
being silently clamped; investigate clock/data-quality limits before making
a latency claim. Non-finite metrics are rejected.

Private resources.jsonl records sampled owned worker/broker CPU/RSS. Worker
usage.jsonl also retains lifetime CPU/max-RSS and checkpoint-child CPU,
including the interrupted incarnation. Poll logs count receive connections;
sender results count send/retry connections. Consumer summaries reconstruct
timestamp-based backlog proxies from the stored pre-commit timestamp and
checkpoint times. They separately report observed cached/uncheckpointed input
depth, not true committed broker queue depth, which is unavailable here.
Raw logs
and incomplete runs must be retained so errors and omissions remain visible.

Actual collector detection, thread receipt and model processing are a separate
sample of at most three useful readiness/result coordination events on the
existing channel. Record unavailable peer thread timestamps as unavailable.
The harness does not publish or enqueue those events. Run the existing
uncertainty/reconciliation fixture separately and label its results as a
fixture, not production wakeup verification.

```sh
/usr/bin/python3 -m unittest test_waiting_service.WaitingTests.test_uncertain_batch_does_not_block_later_input_or_resend -v
```

## Reviewable commands and readiness

First agree a fresh `RUN_UUID`, the reviewed harness head/hash and ownership.
Use each account's own shell and private state base. These are command templates;
do not execute placeholder windows or treat these examples as readiness ACKs.

```sh
/usr/bin/python3 tools/baseline.py preflight --role robin --experiment RUN_UUID
/usr/bin/python3 tools/baseline.py preflight --role zeul --experiment RUN_UUID
```

Preflight is read-only identity/hash discovery. It is not permission or readiness.
Each owner must actually acknowledge the same UTC window before supplying a
local manifest like this (replace every placeholder):

```json
{
  "experiment_id": "RUN_UUID",
  "start_utc": "UTC_START_WITH_Z",
  "core_commit": "4d70e07a8eb24a218c715a40cc3d96b5e386ec36",
  "harness_sha256": "REVIEWED_BASELINE_PY_SHA256",
  "ack_urls": {
    "robin": "ACTUAL_ROBIN_READINESS_COMMENT_URL",
    "zeul": "ACTUAL_ZEUL_READINESS_COMMENT_URL"
  }
}
```

References must be distinct comments in this repository. They are operator
evidence pointers, not an automatic proof of authorship/authority; the operator
must inspect both actual ACKs and scope. A source-plan ACK is not readiness.
The manifest binds the exact harness bytes and baseline source. Commands reject
a window less than two seconds or more than 60 seconds away. A missed window
needs new explicit agreement, not automatic retry. Once any role has reserved
an attempt, use a new UUID for a later attempt.

Robin starts these two foreground commands through separate owned shells:

```sh
/usr/bin/python3 tools/baseline.py broker --experiment RUN_UUID --window-file WINDOW.json --state-base /home/robin/.local/state/agent-buzz/baselines
/usr/bin/python3 tools/baseline.py run --role robin --experiment RUN_UUID --window-file WINDOW.json --state-base /home/robin/.local/state/agent-buzz/baselines
```

Zeul runs the corresponding account command through his own authorized route:

```sh
/usr/bin/python3 tools/baseline.py run --role zeul --experiment RUN_UUID --window-file WINDOW.json --state-base OWNER_PRIVATE_BASELINE_STATE
```

No helper changes account identity. Broker and participant commands reserve
private config/control records before starting workers. Each supervisor has
a monotonic 120-second cap (including any early window wait), and an absolute
latest deadline at the agreed start plus 120 seconds. Eight seconds for the
controller's grace and all bounded handle waits are reserved inside the cap;
excessive lead time/load can
make a run inconclusive rather than extend its deadline. Arrange the launches
close enough to the acknowledged start to leave the agreed recovery interval.

## Completion, stop and rollback

On successful account completion, inspect `summary.json`, both consumers'
observations/results, sender results and usage/poll logs. Alternatively:

```sh
/usr/bin/python3 tools/baseline.py summarize OWNER_ACCOUNT_RUN_DIRECTORY
```

The broker retains a separate broker-result.json with exact global stored-ID
equality and unique count. Both account summaries and broker evidence are
required; one summary alone does not establish two-account completion.
Missing IDs, changed duplicate sequences, unexpected participants/payloads,
missing replay or any worker failure prevent a success claim. Failure records
and logs remain private. Do not publish raw journals, local paths or unrelated
operational data; export only reviewed synthetic aggregate measurements.

Stop by signalling the experiment supervisor whose actual PID/identity you
started. It cleans only registered UID/start-ticks/exact-argv process groups.
Partial-start cleanup also covers failure to acquire a pidfd. It uses the
fresh direct-child Popen handle with bounded TERM/KILL waits, including when
no persistent worker record exists. The supervisor has no concurrent reaper;
an exited child is reaped before signalling and a live unreaped child retains
its identity. This fallback is never used for arbitrary/stale PID records.
It never uses broad process-name matching or signals another account. Broker cleanup checks
the dedicated directory's device/inode/owner and socket type/owner before
removing that socket and empty directory/ACLs. Database, run registry, logs and
journals are preserved. Existing collaboration services stay running.

If a send was uncertain, preserve its exact ID/outbox and reconcile the old
attempt before treating a new run as valid. A fresh experiment ID must not be
used to disguise an uncertain resend. Keep unchanged source and evidence as
the baseline/rollback reference. No login service, optimized protocol or
broader deployment is authorized by executing this bounded experiment.
