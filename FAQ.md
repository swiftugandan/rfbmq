# rfbmq FAQ

Common questions about using rfbmq for AI agent workflows — task management,
context passing, and persistent memory — answered in terms of the primitives
rfbmq already provides.

Every answer here follows the project's principles: the filesystem is enough,
compose with Unix pipes, no daemons, no cleverness until measurements demand
it.

---

## Task Management

### How do I wait for a message instead of polling?

Use `inotifywait` (Linux) or `fswatch` (macOS) as your event loop:

```bash
while true; do
  path=$(rfbmq pop "$QUEUE" 2>/dev/null) && break
  inotifywait -qq -e moved_to "$QUEUE/pending/"
done
cat "$path"
```

rfbmq intentionally has no blocking pop — *"No background threads. No hidden
garbage collection."* The kernel's filesystem notification API is the event
loop.

### How do I express task dependencies (task B waits for task A)?

Use the `Depends-On` header and `rfbmq ready`:

```bash
A=$(echo "task A" | rfbmq push "$QUEUE")
B=$(echo "task B" | rfbmq push "$QUEUE" -d "$A")
C=$(echo "task C" | rfbmq push "$QUEUE" -d "$A" -d "$B")
```

`rfbmq ready` lists only messages whose dependencies are all in `done/`:

```bash
rfbmq ready "$QUEUE"   # prints A only (B and C have unmet deps)

# Process A...
CLAIMED=$(rfbmq pop "$QUEUE")
rfbmq ack "$QUEUE" "$CLAIMED"

rfbmq ready "$QUEUE"   # now prints B (A is in done/)
```

`ready` is a query — it doesn't touch `pop` or change the queue. Your
scheduler decides what to do with the list. Enforcement stays in the
shell — *"Explicit over automatic."*

### How do I fan out a plan into parallel sub-tasks?

Push N messages in a loop with a shared `Correlation-Id`:

```bash
PLAN_ID=$(uuidgen)
for subtask in "implement auth" "write tests" "update docs"; do
  echo "$subtask" | rfbmq push "$WORKER_QUEUE" --correlation-id "$PLAN_ID"
done
```

Each sub-task is independently claimable, retryable, and ackable. The
orchestrator collects results by scanning `done/` for the correlation ID:

```bash
grep -rl "Correlation-Id: $PLAN_ID" "$WORKER_QUEUE/done/"
```

rfbmq is a work queue: one message, one consumer. Fan-out is N pushes.

### How do I route tasks to specific agents?

Use **one queue per task type** instead of selective consume from a single
queue:

```
queues/
  coding/        ← coding agent pops here
  review/        ← review agent pops here
  planning/      ← orchestrator pops here
```

Routing is the pusher's responsibility. Priority subdirs handle urgency
within each queue. This follows the Unix model — route at write time, not
read time.

### How do I implement request-reply (send a task, get a result)?

Use two queues, `Correlation-Id`, and `Reply-To`:

```bash
# Orchestrator pushes a task with a reply queue
echo "Summarize this document" \
  | rfbmq push tasks/ --correlation-id req-001 --reply-to results/

# Worker pops, does the work, pushes the result to the reply queue
TASK=$(rfbmq pop tasks/)
CORR=$(grep '^Correlation-Id:' "$TASK" | cut -d' ' -f2)
REPLY_Q=$(grep '^Reply-To:' "$TASK" | cut -d' ' -f2)
echo "Here is the summary..." | rfbmq push "$REPLY_Q" --correlation-id "$CORR"
rfbmq ack tasks/ "$TASK"

# Orchestrator watches for the reply
inotifywait -qq -e moved_to results/pending/
REPLY=$(rfbmq pop results/)
```

No special mechanism needed — `Reply-To` carries the queue path, two queues
and a shared ID do the rest.

---

## Context & Memory

### How do I pass large context (conversation history, code, embeddings)?

Use the **message body**. Headers are for routing metadata (limited to
fixed-size buffers); the body supports up to 64 MiB of Markdown content:

```bash
cat <<'EOF' | rfbmq push "$QUEUE" -p normal
# Refactor auth module

## Context

The current implementation uses session cookies. Migrate to JWT.

## Files to modify

- src/auth/login.ts
- src/auth/middleware.ts

## Conversation history

User: "The auth module needs to support SSO"
Agent: "I'll add SAML support to the login flow..."
EOF
```

If you need structured data, put JSON or YAML in the body. The RFC 822
headers handle routing; the Markdown body carries the payload.

### How do I store intermediate results while processing a task?

Don't mutate the message in `processing/`. Use **sidecar files**:

```bash
TASK=$(rfbmq pop "$QUEUE")
TASK_ID=$(basename "$TASK" .md | sed 's/^[0-9]*\.//')

# Write intermediate results alongside the task
echo "$tool_output" > "$QUEUE/processing/${TASK_ID}.step1.md"
echo "$analysis"    > "$QUEUE/processing/${TASK_ID}.step2.md"

# Clean up sidecars when done
rm -f "$QUEUE/processing/${TASK_ID}".step*.md
rfbmq ack "$QUEUE" "$TASK"
```

This preserves the single-write atomicity model — no partial writes, no
append API, no corruption risk.

### How do I use `done/` as agent memory / audit log?

Completed messages persist in `done/` until purged. Query them with standard
tools:

```bash
# Find all completed tasks for a correlation ID
grep -rl "Correlation-Id: plan-42" "$QUEUE/done/"

# Find tasks tagged with "coding"
grep -rl "Tags:.*coding" "$QUEUE/done/"

# Read the body of a completed task
rfbmq cat "$QUEUE/done/a3f2e1b4...md"

# Count completed tasks from the last hour
find "$QUEUE/done/" -name '*.md' -mmin -60 | wc -l
```

Control retention with `rfbmq purge`:

```bash
# Keep 7 days of history (default)
rfbmq purge "$QUEUE" -a 604800

# Keep 24 hours
rfbmq purge "$QUEUE" -a 86400
```

### How do I look up a specific message by ID?

The message ID is embedded in every filename. Use `find`:

```bash
find "$QUEUE" -name "*a3f2e1b4c5d6a7b8c9d0e1f2a3b4c5d6*"
```

This searches all lifecycle directories (`pending/`, `processing/`, `done/`,
`failed/`) and returns the current location. The filesystem is the index.

### How do I inspect failed tasks to learn from errors?

`failed/` is the dead-letter queue. Browse it like any directory:

```bash
ls "$QUEUE/failed/"
rfbmq inspect "$QUEUE/failed/a3f2e1b4...md"   # metadata
rfbmq cat "$QUEUE/failed/a3f2e1b4...md"        # body

# Retry a specific failed task (re-enqueue to preserve FIFO ordering)
rfbmq cat "$QUEUE/failed/a3f2e1b4...md" | rfbmq push "$QUEUE"
rm "$QUEUE/failed/a3f2e1b4...md"
```

The `Retry-Count` header shows how many attempts were made before
dead-lettering.

---

## Operations

### Will `done/` grow unbounded and slow things down?

Run `rfbmq purge` on a schedule:

```crontab
* * * * * rfbmq reap /var/queue/jobs -l 300
0 * * * * rfbmq purge /var/queue/jobs -a 604800
```

The first job reclaims stale processing leases every minute. The second
purges `done/` entries older than the configured age (604800 seconds =
7 days) every hour. ext4 with
`dir_index` handles 100K+ entries per directory — measure before optimizing.

### Can multiple agents share the same queue safely?

Yes. That is the core design. Each agent calls `rfbmq pop` independently.
`rename(2)` serializes access — exactly one consumer wins, others get ENOENT
and retry automatically. No locks, no coordination protocol.

Each consumer calls `rfbmq pop` independently — cross-process
concurrency is safe by design.

### Can I use rfbmq across multiple machines?

rfbmq operates on a single filesystem. For multi-host agents, your options:

- **Shared filesystem** (NFS, EFS, GlusterFS) — rfbmq works if `rename(2)`
  is atomic on the mount (NFS v3+ with same-directory renames)
- **Thin HTTP shim** — wrap rfbmq CLI in a REST endpoint
- **Sync script** — `rsync` completed results between hosts

If you need true multi-node replication, you need a distributed broker.
The manifesto is explicit: *"If you need messages replicated across machines,
you need a distributed broker."*

### How do I handle backpressure?

`max_pending` (set at `rfbmq init --max-pending N`) returns exit code 2 when
the queue is full. Producers check the exit code:

```bash
if ! echo "task" | rfbmq push "$QUEUE"; then
  echo "Queue full, backing off" >&2
  sleep 5
fi
```

Default is 10,000 pending messages. Use `--max-pending 0` for unlimited.

### What metadata can I put in the `Custom:` header?

The `Custom:` block supports RFC 822 continuation lines (indented lines
append to the block), up to 4096 bytes:

```markdown
Custom:
  agent: claude-3.5
  model-temperature: 0.7
  tool-calls: 3
  last-error: rate limit exceeded
```

For structured data larger than 4KB, use the message body instead.
