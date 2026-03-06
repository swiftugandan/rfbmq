# rfbmq

A filesystem-based message queue. Queues are directories, messages are files, claims are `rename(2)`.

## Quick start

```bash
cargo build --release

# Create a queue
rfbmq init /tmp/myqueue

# Push a message
echo "hello world" | rfbmq push /tmp/myqueue

# Pop the next message (prints the claimed file path)
TASK=$(rfbmq pop /tmp/myqueue)

# Read the message body
rfbmq cat "$TASK"

# Acknowledge (move to done/)
rfbmq ack /tmp/myqueue "$TASK"
```

## Commands

| Command   | Description                                    |
|-----------|------------------------------------------------|
| `init`    | Create a new queue directory structure         |
| `push`    | Enqueue a message (reads body from stdin)      |
| `pop`     | Claim the oldest pending message               |
| `ack`     | Mark a claimed message as completed            |
| `nack`    | Return a claimed message for retry or fail it  |
| `depth`   | Print pending + processing count               |
| `reap`    | Reclaim expired leases and TTL-expired messages|
| `purge`   | Delete old completed messages from `done/`     |
| `sync`    | Flush deferred directory fsyncs                |
| `ready`   | List messages with all dependencies satisfied  |
| `inspect` | Print human-readable message metadata          |
| `cat`     | Print message body only                        |
| `version` | Print version                                  |

## Queue layout

```
myqueue/
  pending/       # messages waiting to be claimed
  processing/    # claimed messages (active leases)
  done/          # successfully completed messages
  failed/        # dead-lettered messages (max retries exceeded)
  .tmp/          # atomic write staging area
  .meta/         # queue configuration (max_pending)
```

With `--priority`, `pending/` contains subdirectories:

```
pending/
  0-critical/
  1-high/
  2-normal/
  3-low/
```

## Message format

Messages are Markdown files with RFC 822-style headers:

```
Id: a3f2e1b4c5d6a7b8c9d0e1f2a3b4c5d6
Created-At: 2025-01-15T10:30:00.000000000Z
Created-By: 1234@hostname
Priority: normal
Retry-Count: 0
TTL: 0
Tags: coding, urgent
Correlation-Id: plan-42

Your message body here (up to 64 MiB of Markdown).
```

## Exit codes

| Code | Meaning                                    |
|------|--------------------------------------------|
| 0    | Success                                    |
| 1    | Not found (empty queue, no ready messages) |
| 2    | Error (queue full, I/O failure, etc.)      |

## Further reading

- [Manifesto](Manifesto.md) -- design philosophy and principles
- [FAQ](FAQ.md) -- usage patterns for AI agent workflows
- [DeepWiki](https://deepwiki.com/swiftugandan/rfbmq) -- AI-generated documentation
