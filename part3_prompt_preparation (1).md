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

To implement a batching interface for the AIOKafkaProducer class in the aiokafka library we will add two new public methods, create_batch and send_batch along with a supporting BatchBuilder class.

The AIOKafkaProducer class lives in aiokafka/producer.py. When you call send today it hands the record off to a MessageAccumulator, which quietly groups records into batches per topic-partition and flushes them based on linger_ms and max_batch_size.

The problem with the API is that it gives you no control over batch boundaries. If you need all the records from one chunk to land in a single Kafka batch calling send_and_wait in a loop per message is way too slow.

## A bit of background on the codebase

The aiokafka library is a Python asyncio library. Every network call you write must be a coroutine. No blocking code is allowed anywhere in the implementation.

The AIOKafkaClient in aiokafka/client.py has a send coroutine that takes a Kafka protocol request and gives back a response.

## What you need to implement

### 1. Create_batch on

This is a plain synchronous method. It returns a BatchBuilder pre-configured with the producers compression_type and max_batch_size. If someone calls this before calling producer.start raise an error right away.

### 2. Send_batch on AIOKafkaProducer

This takes a completed BatchBuilder, a topic name and a partition number. It does a sanity check that the topic and partition actually exist and are reachable. Then it submits the -built batch directly into the MessageAccumulator for that topic-partition completely bypassing the normal per-message send path.

It returns a future that resolves once the broker has acknowledged the batch. If theres already a batch in flight for the partition wait for that one to finish before dispatching the new one.

### 3. BatchBuilder class

The constructor takes compression_type. Max_batch_size. Its main method is append, which takes key, value and timestamp. Key and value should be bytes. On success return a metadata object. When the buffer is full return None without raising an exception.

### 4. Add_batch method on MessageAccumulator

Add a method that accepts a -built BatchBuilder and slots it directly into the queue for the target topic-partition.

## What I need the implementation to get right

create_batch must return a BatchBuilder that reflects the producers compression_type and max_batch_size. After append returns None, the records that were already staged must be intact and deliverable. Send_batch must deliver everything in the batch to the partition you specify.

If a batch for the partition is already in flight the new one must wait its turn. Preserve ordering. On any network failure or timeout raise the error to the caller. Both new methods must fail fast. Clearly if start hasn't been called yet.

## Edge cases worth thinking about

buffer behaviour. Once append has returned None it should keep returning None for every subsequent call. Batch. If someone calls send_batch on a BatchBuilder they never appended anything to either reject it with a meaningful error or send a valid empty batch.

Network problems -send. If the broker goes away or the request times out while send_batch is waiting for acknowledgement the exception should bubble up to the caller cleanly. Using the API before start. Both create_batch and send_batch should blow up immediately with a message if start hasn't been called.

## Files to change

We need to change the following files:

* aiokafka/producer.py. Add create_batch and send_batch to AIOKafkaProducer

* aiokafka/message_accumulator.py. Add the BatchBuilder class and the add_batch method to MessageAccumulator

* examples/batch_produce.py. Example script showing the full create → append → send loop with partition selection

* tests/test_producer.py. New integration tests

## Tests I want to see

We need to cover the following test cases:

1. Happy path. Create a batch append a known set of records send it to a partition then start a consumer on that partition and verify every record arrives in the exact order it was appended.

2. Full-buffer flow. Keep calling append until it returns None then send that batch and create an one for the remaining records send that too and verify all records land.

3. In-flight ordering. Call send_batch twice on the partition in quick succession without awaiting the first. Once both settle confirm both batches arrived in the order and neither was dropped.

4. Pre-start error. Call create_batch. Send_batch on a producer that hasn't been started. Assert that a clear descriptive exception is raised away.

5. Bad partition. Call send_batch targeting a partition that doesn't exist on the topic. Assert that a KafkaTimeoutError or UnknownTopicOrPartitionError is raised than hanging.

All async test code should use await. No loop.run_until_complete, in the library itself.

---