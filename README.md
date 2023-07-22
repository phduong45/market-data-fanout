# market-data-fanout

A small C++ simulation of a market-data fanout pipeline.

## Problem

A market-data feed usually has one source of events, but several downstream
consumers: pricing logic, risk checks, strategy workers, logging, or metrics.

A simple shared queue makes every consumer compete for the same messages. That
is not a broadcast: one worker can take a tick and the others never see it.

For fanout, each worker needs its own stream.

## Approach

This project gives each worker its own SPSC queue:

```text
Feed
  -> queue for worker 0
  -> queue for worker 1
  -> queue for worker 2
```

The feed thread publishes every tick to every worker queue. Each worker thread
drains only its own queue, so the consumer side stays independent.

The project tracks:

- ticks published by the feed
- ticks dropped when a worker queue is full
- ticks received by each worker
- average and max consume latency

## Why SPSC

Each worker queue has exactly one producer and one consumer:

- producer: feed thread
- consumer: one worker thread

That matches the single-producer single-consumer queue contract and avoids a
shared mutex on the hot path.

## Build

```bash
make
```

## Test

```bash
make test
```
