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

PR #217 adds a way to manually batch messages to `AIOKafkaProducer`. It does this by adding two methods: `create_batch` and `send_batch`.

Before this change you could only send messages using send or send_and_wait. These methods group records by topic and partition automatically. They also follow the `max_batch_size` rule. Know how to wait before sending a batch. This works well in some cases. It does not work well when you need to control exactly what goes into each batch and when it is sent.

For example if you need to make sure messages are delivered before you can move on using `send_and_wait` in a loop is too slow. This is especially true when you have a lot of messages coming in quickly and you need to make sure they are all delivered.

This change introduces create_batch, which gives you a BatchBuilder object. This object is set up with the producers compression and `max_batch_size`. You can add messages to the BatchBuilder using its append method. When the buffer is full the append method will return None, which's like a signal to stop and send the batch.

Now `AIOKafkaProducer` has a way to build and send batches to a partition. The old send and send_and_wait methods still work the way. The new method is an addition, to `AIOKafkaProducer`. One thing to note is that send_batch does not use the serializer and partitioner so you have to give it pre-encoded keys and values and choose the partition for `AIOKafkaProducer` yourself.


---

## 3.1.3 Acceptance Criteria

✓ When you call `create_batch()` on a producer that has already started it gives you a `BatchBuilder` instance. This instance is set up with the producers settings for `compression_type` and `max_batch_size`.

✓ If you call `batch.append(key, value timestamp)` and there is still space in the buffer it returns some metadata. Adds the message to the batch.

✓. If you call `batch.append(key, value timestamp)` and the buffer is full. That is adding the record would make it too big. It returns `None` and does not change the batch. You can then call `send_batch()`. Start a new batch.

✓ When you call `send_batch(batch, topic, partition=N)` with a completed `BatchBuilder` all the records in the batch are sent to the specified topic and partition as one Kafka batch. The function only continues after the broker confirms it got the batch.

✓ If you call `send_batch()` for a partition that already has a batch being sent the call waits until the first batch is confirmed before sending the one. This keeps the order of messages for each partition.

✓ If you call `send_batch()` with a topic or partition that does not exist or is invalid it raises an error, like `KafkaTimeoutError` or `UnknownTopicOrPartitionError`. It does not just drop the batch.

✓ Any code that does not use `create_batch()` or `send_batch()` works the same as before. The existing `send()` and `send_and_wait()` behaviour does not change.

✓ The `BatchBuilder.append()` method handles byte keys and values correctly. It does not try to use the producers value serializer because that is not needed here.

---

## 3.1.4 Edge Cases

**1. Adding to the batch after it is full and `None` has been returned**

When someone keeps adding to the batch after it's full and `None` has been returned the system must always return `None` for every call after that. This is so the batch does not get messed up and the records that are already in the batch can still be sent with `send_batch()`. The batch should stay the same so it can be sent correctly.

**2. Calling `send_batch()` on a BatchBuilder`**

If someone makes a batch does not add anything to it and then tries to send it with `send_batch()` the system should either say no with a clear error or send an empty batch that Kafka can handle. It is not okay if the system just does nothing and does not say what is happening.

**3. Network. Timeout during `send_batch()` delivery**

If the Kafka server is not available or it takes long to get a response while `send_batch()` is waiting the method should say there is a `KafkaTimeoutError` or a connection error. This is so the person using the system can see what went wrong and decide what to do. The system must not just hide the error. Say the batch was sent when it was not.

**4. Calling `create_batch()` or `send_batch()` before `producer.start()`**

If someone tries to make a batch or send a batch before the producer is started the system should say there is an error away. It should not just wait forever. Give a strange error because something is not set up right. This is the same as what happens when someone tries to send a message before the producer is started.

**5. Large batches that exceed the Kafka servers `message.max.bytes`**

The `max_batch_size` on the producer is a limit on the client side. The Kafka server also has its own limit for how big a message can be. If someone fills a `BatchBuilder` to the client-side limit and that is bigger than the Kafka servers limit, `send_batch()` will get an error, from the Kafka server. The system should pass on this error in a way that makes sense than just failing quietly or trying again forever.

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
