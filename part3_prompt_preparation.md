# Part 3: Prompt Preparation

### PR selected: **aiokafka #217 — Added lightweight batching interface to `AIOKafkaProducer`**

**Link**: https://github.com/aio-libs/aiokafka/pull/217

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

✓ When `create_batch()` is called on a started producer, it returns a `BatchBuilder` instance configured with the producer's current `compression_type` and `max_batch_size`.

✓ When `batch.append(key, value, timestamp)` is called and the buffer still has room, it returns a non-`None` metadata object and the message is staged in the batch.

✓ When `batch.append(key, value, timestamp)` is called and the buffer is full (i.e., adding the record would exceed `max_batch_size`), it returns `None` and does not modify the batch — the caller can then call `send_batch()` and start a new batch.

✓ When `send_batch(batch, topic, partition=N)` is called with a completed `BatchBuilder`, all records in the batch are delivered to the specified topic-partition as a single Kafka batch, and the returned future resolves only after the broker acknowledges delivery.

✓ When `send_batch()` is called for a partition that already has an in-flight batch, the call blocks (or queues) until the earlier batch is acknowledged before submitting the new one, preserving per-partition message ordering.

✓ When `send_batch()` is called with a topic or partition that does not exist or is otherwise invalid, it raises a clear exception (such as `KafkaTimeoutError` or `UnknownTopicOrPartitionError`) rather than silently dropping the batch.

✓ Any code that does not call `create_batch()` or `send_batch()` is completely unaffected — all existing `send()` and `send_and_wait()` behaviour remains identical.

✓ The `BatchBuilder.append()` method correctly handles byte keys/values; it must not attempt to apply the producer's key or value serialiser — those are bypassed by design.

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

You are working inside the `aiokafka` library — a Python asyncio-based Kafka client
(https://github.com/aio-libs/aiokafka). The producer class `AIOKafkaProducer` lives in
`aiokafka/producer.py`. Its batching logic is handled by `MessageAccumulator` in
`aiokafka/message_accumulator.py`. The producer currently only exposes `send()` and
`send_and_wait()`, which hand all batching decisions to the accumulator automatically.
There is no way for a caller to build a batch manually and dispatch it as a single unit.

Your task is to implement a manual batching interface by adding two new public methods
and one new class across two files. No blocking code is permitted — this is a fully
async library.

**What to implement:**

In `aiokafka/message_accumulator.py`, add a `BatchBuilder` class. Its constructor
takes `compression_type` and `max_batch_size`. It exposes one method —
`append(key, value, timestamp)` — which accepts raw bytes (no serialisation), returns
a metadata object on success, and returns `None` silently when the buffer is full.
Once `None` is returned, all further `append()` calls must also return `None` without
altering the staged records. Also add an `add_batch()` method to `MessageAccumulator`
that accepts a completed `BatchBuilder` and enqueues it directly for a given
topic-partition, bypassing the per-record path.

In `aiokafka/producer.py`, add `create_batch()` — a synchronous method that returns a
`BatchBuilder` configured with the producer's `compression_type` and `max_batch_size`.
Also add `async def send_batch(self, batch, topic, *, partition)`, which validates the
target partition, submits the pre-built batch directly to the accumulator, and awaits
broker acknowledgement before returning. If a batch for the same partition is already
in flight, it must wait for that one to finish first to preserve ordering.

**Acceptance criteria your implementation must satisfy:**

- `create_batch()` reflects the producer's live configuration, not hardcoded defaults.
- `append()` returns `None` on a full buffer without raising and without corrupting already-staged records.
- `send_batch()` delivers all records to exactly the specified partition and resolves only after broker acknowledgement.
- Per-partition ordering is preserved when two batches are sent in quick succession.
- Both methods raise a descriptive error immediately if called before `producer.start()`.
- All existing `send()` and `send_and_wait()` behaviour is completely unchanged.

**Edge cases to handle:**

- After `append()` returns `None`, subsequent calls must keep returning `None` — no silent extra appends.
- Calling `send_batch()` on an empty `BatchBuilder` should either fail clearly or send a valid empty batch — it must not hang.
- Any network failure during `send_batch()` must propagate to the caller as a `KafkaTimeoutError` or connection error — do not swallow it.

**Files to modify:** `aiokafka/producer.py`, `aiokafka/message_accumulator.py`,
`tests/test_producer.py`, and `examples/batch_produce.py`.

**Testing:** Use the existing pytest + Docker Kafka setup (`make test`). Write
integration tests against a live broker covering: normal batch delivery with ordering
verified by a consumer, the full-buffer `None` signal flow, in-flight ordering across
two consecutive `send_batch()` calls on the same partition, a pre-start error check,
and a bad-partition error check. No new dependencies. Use `await` throughout — no
`loop.run_until_complete()` in library code.
