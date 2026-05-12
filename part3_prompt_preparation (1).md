# Part 3: Prompt Preparation

**PR selected for this section:** [aiokafka #217 — Added lightweight batching interface to `AIOKafkaProducer`](https://github.com/aio-libs/aiokafka/pull/217)

---

## 3.1.1 Repository Context

`aiokafka` is a Python library for working with Apache Kafka. It is built on top of Pythons framework. This library helps Python applications to send and receive messages from a Kafka cluster. It does this without stopping the event loop. The older `kafka-python` library cannot do this.

The library has two classes: `AIOKafkaProducer` and `AIOKafkaConsumer`.

* `AIOKafkaProducer` helps to send records to Kafka topics.

* `AIOKafkaConsumer` reads records from one or more topics. It can also work with a group of consumers for processing.

The library is designed for Python developers who build event-driven services. These services use real-time data pipelines or stream processing applications on top of Kafka. A typical user might be writing a microservice. This microservice receives records from a Kafka topic changes them and sends results to another topic. All of this happens inside async functions running on `asyncio`.

The library is used in production data-pipeline scenarios. So it is very important that it works correctly and quickly. If messages are lost or not in the order it can cause problems in downstream systems.

The `aiokafka` library is on GitHub under the `libs` organisation. This organisation has libraries that work with `asyncio`. The library uses `kafka-python` for some lower-level Kafka tasks.. It replaces blocking network calls with `asyncio`-based ones.

The code is organised into a package with sub-modules. These sub-modules handle tasks.

* There is a sub-module for the consumer.

* There is a sub-module for the producer.

* There are sub-modules, for message accumulator, fetcher, coordinator and protocol handling.

---

## 3.1.2 Pull Request Description

PR #217 adds a lightweight manual batching interface to `AIOKafkaProducer` through two new public methods: `create_batch()` and `send_batch()`.

Before this change the only way to send messages was by using `send()` or `send_and_wait()`. These methods automatically group records by topic and partition. They also respect the `max_batch_size` and how to wait before sending a batch. This works well for cases but it does not work well when you need to control exactly what goes into each batch and when it is sent.

For example if you need to confirm that messages are delivered before you can move on using `send_and_wait()` in a loop is too slow. This is especially true when you have a lot of messages coming in quickly and you need to make sure they are all delivered.

This change introduces `create_batch()`, which gives you a `BatchBuilder` object. This object is set up with the producers compression and `max_batch_size`. You can add messages to the BatchBuilder using its `append` method. When the buffer is full the append method will return `None`, which's, like a signal to stop and send the batch.

Now AIOKafkaProducer has a way to build and send batches to a specific partition. The old `send` and `send_and_wait` methods still work the way. The new method is an addition. One thing to note is that `send_batch()` does not use the serializer and partitioner so you have to give it pre-encoded keys and values and choose the partition yourself.



---

## 3.1.3 Acceptance Criteria

- When `create_batch()` is called on a started producer, it returns a `BatchBuilder` instance configured with the producer's current `compression_type` and `max_batch_size`.

- When `batch.append(key, value, timestamp)` is called and the buffer still has room, it returns a non-`None` metadata object and the message is staged in the batch.

- When `batch.append(key, value, timestamp)` is called and the buffer is full (i.e., adding the record would exceed `max_batch_size`), it returns `None` and does not modify the batch — the caller can then call `send_batch()` and start a new batch.

- When `send_batch(batch, topic, partition=N)` is called with a completed `BatchBuilder`, all records in the batch are delivered to the specified topic-partition as a single Kafka batch, and the returned future resolves only after the broker acknowledges delivery.

- When `send_batch()` is called for a partition that already has an in-flight batch, the call blocks (or queues) until the earlier batch is acknowledged before submitting the new one, preserving per-partition message ordering.

- When `send_batch()` is called with a topic or partition that does not exist or is otherwise invalid, it raises a clear exception (such as `KafkaTimeoutError` or `UnknownTopicOrPartitionError`) rather than silently dropping the batch.

- Any code that does not call `create_batch()` or `send_batch()` is completely unaffected — all existing `send()` and `send_and_wait()` behaviour remains identical.

- The `BatchBuilder.append()` method correctly handles byte keys/values; it must not attempt to apply the producer's key or value serialiser — those are bypassed by design.

---

## 3.1.4 Edge Cases

**1. Appending after the batch is full and `None` has been returned**

If a caller ignores the `None` return from `append()` and continues calling `append()` on a full batch, the implementation must consistently return `None` for every subsequent call without corrupting the already-staged records. The batch should remain in its last valid state so it can still be dispatched correctly with `send_batch()`.

**2. Calling `send_batch()` on an empty `BatchBuilder`**

If a caller creates a batch, appends nothing to it, and then calls `send_batch()`, the implementation should either reject the call with a clear error or, if it proceeds, produce a valid but empty Kafka batch that the broker accepts. Silently succeeding with undefined behaviour (e.g., hanging forever waiting for an ack that never comes) is not acceptable.

**3. Network failure or timeout during `send_batch()` delivery**

If the broker is unreachable or the request exceeds `request_timeout_ms` while `send_batch()` is waiting for acknowledgement, the method should raise `KafkaTimeoutError` (or the underlying connection error) to the caller. The caller must be able to detect failure and decide whether to retry — the implementation must not silently swallow the error or mark the batch as delivered when it was not.

**4. Calling `create_batch()` or `send_batch()` before `producer.start()`**

If either method is called on a producer that has not been started yet, the implementation should raise a clear error immediately rather than hanging indefinitely or producing a cryptic `AttributeError` from an uninitialised internal state. This is consistent with how the existing `send()` method behaves before start.

**5. Large batches that exceed broker-side `message.max.bytes`**

The `max_batch_size` on the producer is a client-side limit, but the broker also has its own maximum message size. If a caller fills a `BatchBuilder` to the client-side `max_batch_size` and that size exceeds the broker's configured limit, `send_batch()` will receive a broker-side error. The implementation should propagate this as a meaningful exception rather than a silent failure or an infinite retry loop.

---

## 3.1.5 Initial Prompt

```
You are implementing two new public methods — `create_batch()` and `send_batch()` —
for the `AIOKafkaProducer` class in the `aiokafka` library
(https://github.com/aio-libs/aiokafka). This is a Python asyncio-based Kafka client.
All network operations must be implemented as coroutines; no blocking calls are permitted.

---

REPOSITORY CONTEXT

`AIOKafkaProducer` is defined in `aiokafka/producer.py`. Its internal batching and
delivery logic lives in `aiokafka/message_accumulator.py` (the `MessageAccumulator`
class). When `send()` is called, individual records are appended to the accumulator,
which groups them into `MessageBatch` objects per topic-partition and flushes them to
the broker based on `linger_ms` and `max_batch_size`. The producer communicates with
Kafka brokers via `AIOKafkaClient` (in `aiokafka/client.py`), which exposes a `send()`
coroutine that accepts Kafka protocol request objects and returns response objects.

The producer is initialised with configuration including `compression_type`,
`max_batch_size`, `request_timeout_ms`, and `acks`. These settings govern how the
accumulator builds and delivers batches.

---

WHAT TO IMPLEMENT

1. `create_batch()` on `AIOKafkaProducer`
   - Returns a new `BatchBuilder` instance configured with the producer's current
     `compression_type` and `max_batch_size`.
   - This is a synchronous method (no `await` needed).
   - Must raise a clear error if called before `producer.start()`.

2. `async def send_batch(self, batch, topic, *, partition)` on `AIOKafkaProducer`
   - Accepts a completed `BatchBuilder`, a topic name (str), and a required keyword
     argument `partition` (int).
   - Validates that the topic and partition are reachable.
   - Submits the pre-built batch directly to the `MessageAccumulator` for the specified
     topic-partition, bypassing the normal per-message `send()` path entirely.
   - Returns (or awaits) a future that resolves when the broker acknowledges delivery.
   - If another batch for the same partition is already in flight, blocks until that
     earlier batch is acknowledged before dispatching the new one, to preserve ordering.
   - Must raise a clear error if called before `producer.start()`.

3. `BatchBuilder` class (add to `aiokafka/message_accumulator.py`)
   - Constructor accepts `compression_type` and `max_batch_size`.
   - `append(key, value, timestamp)` method:
     - `key` and `value` must be raw bytes (or None); do NOT apply the producer's
       serialisers.
     - Returns a non-None metadata object on success.
     - Returns `None` (without raising) when the buffer is full.
     - After returning `None` once, every subsequent call must also return `None`
       without modifying the batch.
   - The internal buffer must be compatible with the format expected by
     `MessageAccumulator.add_batch()` (see below).

4. `add_batch()` path in `MessageAccumulator`
   - Add a method that accepts a pre-built `BatchBuilder` and enqueues it for the
     target topic-partition, bypassing the per-record `add_message()` logic.

---

ACCEPTANCE CRITERIA YOUR IMPLEMENTATION MUST SATISFY

- `create_batch()` returns a `BatchBuilder` configured with the producer's
  `compression_type` and `max_batch_size`.
- `batch.append()` returns `None` (not an exception) when the buffer is full; the
  already-staged records remain intact.
- `send_batch()` delivers all staged records to exactly the specified topic-partition
  as a single batch; the broker acknowledges them before the future resolves.
- If another batch for the same partition is already in flight, `send_batch()` queues
  rather than fails.
- `send_batch()` raises a clear exception on network failure or timeout — no silent
  data loss.
- Calling either method before `producer.start()` raises a clear error immediately.
- All existing `send()` and `send_and_wait()` behaviour remains identical — the new
  API is purely additive.
- `BatchBuilder.append()` does NOT invoke the producer's key/value serialisers;
  callers are responsible for supplying pre-encoded bytes.

---

EDGE CASES TO HANDLE

- Appending to a full batch: after `append()` returns `None`, all further calls must
  also return `None` without corrupting existing records.
- Empty batch sent via `send_batch()`: either reject clearly or produce a valid empty
  Kafka batch — do not hang.
- Network failure during `send_batch()`: propagate `KafkaTimeoutError` or the
  connection error to the caller; do not swallow.
- Pre-start usage: both `create_batch()` and `send_batch()` must fail fast with a
  descriptive error if `start()` has not been called.
- Broker-side size rejection: if the batch exceeds the broker's `message.max.bytes`,
  surface the broker error as a meaningful exception rather than retrying indefinitely.

---

FILES TO MODIFY

- `aiokafka/producer.py` — add `create_batch()` and `send_batch()` to
  `AIOKafkaProducer`
- `aiokafka/message_accumulator.py` — add `BatchBuilder` class and the `add_batch()`
  path to `MessageAccumulator`
- `examples/batch_produce.py` — add a new example script demonstrating the full
  create → append → send loop with partition selection
- `tests/test_producer.py` — add integration tests (see below)

---

TESTING REQUIREMENTS

Use the existing pytest + Docker Kafka infrastructure (see `Makefile` → `make test`).
Each new test should start a real producer against a live broker. Required test cases:

1. Normal batch delivery: produce known records via `create_batch()` / `send_batch()`,
   then consume from the same partition and verify every record arrives in order.
2. Full-buffer signal: fill a `BatchBuilder` until `append()` returns `None`, then
   send the full batch and a second batch, verifying all records are delivered.
3. Same-partition in-flight ordering: call `send_batch()` twice on the same partition
   in quick succession without awaiting the first; verify both batches are delivered
   in order and neither is dropped.
4. Unstarted producer: call `create_batch()` or `send_batch()` before `start()` and
   assert a clear exception is raised immediately.
5. Invalid partition: call `send_batch()` targeting a partition that does not exist
   and assert a `KafkaTimeoutError` or `UnknownTopicOrPartitionError` is raised.

Do not add any new external dependencies. All async test code must use `await`;
no `loop.run_until_complete()` inside library code.
```
