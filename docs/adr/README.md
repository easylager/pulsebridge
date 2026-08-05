# Architecture decision records

Each record states the problem, the decision, **the alternatives that were
rejected and why**, and the consequences the team has to live with. The rejected
alternatives are the part worth reading: a decision without them is just a
description of the code.

| # | Decision | Why it exists |
|---|---|---|
| [0001](0001-hexagonal-gateway.md) | Hexagonal layering in the gateway | Business rules must be testable without Redis and must not leak FastAPI types |
| [0002](0002-redis-streams-bridge.md) | Redis Streams as the transport | Durable handoff with consumer groups, without standing up Kafka for a demo |
| [0003](0003-go-worker-pool.md) | Bounded goroutine pool in the worker | Predictable in-flight work; back-pressure lands in Redis, not in the heap |
| [0004](0004-dead-letter-and-retries.md) | Reclaim, retry budget, dead-letter stream | Failed and poison messages were stranded or silently dropped |
| [0005](0005-at-least-once-semantics.md) | At-least-once with an idempotent handler | Exactly-once is not achievable at the transport; close the gap where it can be closed |
| [0006](0006-idempotency-rollback.md) | Release the idempotency claim on publish failure | A failed publish left events permanently marked as duplicates |
| [0007](0007-distributed-tracing.md) | W3C trace context on the stream entry | A stream breaks causality; metrics aggregate away the interesting cases |

## Format

```markdown
# ADR NNNN: Title

## Status
Accepted | Superseded by ADR NNNN

## Context
What forced a decision. Include the failure that prompted it, if there was one.

## Decision
What was chosen, and the reasoning that is not obvious from the code.

## Alternatives rejected
Each option, what it would have bought, and the specific reason it lost.

## Consequences
What this costs, what it constrains for future contributors, what it leaves open.
```
