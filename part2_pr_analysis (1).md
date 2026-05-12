# Part 2: Pull Request Analysis

**Repository selected:** [aio-libs/aiokafka](https://github.com/aio-libs/aiokafka)

**PRs selected:** [#143](https://github.com/aio-libs/aiokafka/pull/143) and [#217](https://github.com/aio-libs/aiokafka/pull/217)

---

## 1. PR #143 — Added metadata change listener if `group_id` is None

**Link:** https://github.com/aio-libs/aiokafka/pull/143

---

### PR Summary

When a consumer is created without a `group_id` it works on its own. The consumer operates in a mode. This means it does not have a Group Coordinator and it handles its partition assignments.
Before this change the standalone consumer was not telling the Kafka client when something changed. The `GroupCoordinator` usually takes care of this. Checks if the partitions need to be reassigned.. Since the standalone consumer does not use the `GroupCoordinator` nobody was watching for changes.
So what happened was that if a topic got partitions after the consumer started the consumer would not know about them and would just ignore the new data.
This change fixes that problem, by making sure the consumer can find partitions even when there is no group_id. It does this by adding a metadata listener to the consumer when there is no group so the consumer can always find partitions. This way the consumer works correctly whether or not a `group_id` is provided.

---

### Technical Changes

- **`aiokafka/consumer.py`**
  - In the `start()` method, a branch is added for the `group_id is None` path.
  - A metadata change callback is registered with `self._client.add_listener(...)` so the consumer reacts to cluster metadata updates even without a coordinator.
  - The corresponding `stop()` method removes the listener to prevent memory leaks.

- **`aiokafka/fetcher.py`**
  - Minor adjustments to how the fetcher handles partition state when metadata-driven reassignment fires outside the normal rebalance flow.

- **`tests/`** (integration tests)
  - A test case is added to verify that a no-group consumer correctly picks up new partitions after the broker's topic metadata changes.

---

### Implementation Approach

The main fix is to use the listener registration in a specific way. Now the `GroupCoordinator` is the only thing that registers a metadata listener, which is the `_handle_metadata_update` function. This update adds a version of that listener directly to the `AIOKafkaConsumer.start()` function when there is no `group_id`.
This new listener looks at the metadata and checks the partitions for the topics we are interested in. It then compares that to the partitions the consumer is currently using. If it finds partitions it tells the fetcher to start getting data from them.
We did this in a simple way, on purpose. Of copying all the `GroupCoordinator` code we just added a small callback that does the one thing a consumer needs when the metadata changes. It updates the partitions. Since `asyncio` callbacks only happen one at a time we do not have to worry about things trying to change the listener at the same time.
When we call `stop()` we remove the listener, which's the same way the `GroupCoordinator` cleans up after itself. This keeps everything working in a way. The `AIOKafkaClient` listener registration is used in a way to make this work. The `AIOKafkaConsumer` is what uses the `listener registration.

---

### Potential Impact

This change is important for people who use `AIOKafkaConsumer` without a `group_id`. This is usually the case when someone is in control of how the consumer works with partitions. For example this could be a tool that replays something or a reader for audit logs.
The update makes these consumers pay attention to changes, from the broker that they did not notice before. There is not much to worry about for consumers that're part of a group because the new way of registering listeners only happens when there is no `group_id`. The usual way of doing things with a coordinator is not affected.

---

---

## 2. PR #217 — Added lightweight batching interface to `AIOKafkaProducer`

**Link:** https://github.com/aio-libs/aiokafka/pull/217

---

### PR Summary

Before this update the only way to send messages with `AIOKafkaProducer` was through `send()` or `send_and_wait()`. Both of these methods let the internal message accumulator handle batching. This works well in cases.

However there are situations where you need to build a batch yourself. For example when you're reading from a stream that can't move forward until delivery is confirmed. In cases you need to decide when the batch is full and submit it as a unit.

Calling `send_and_wait()` in a loop for each message is too slow. This update introduces two methods: `create_batch()` and `send_batch()`. These methods let you create a `BatchBuilder` object. You can add messages to it manually. Then you can send the batch to a specific partition at once.

This is a lower-level API. It does not use the partitioner and serializer.. It gives you direct control over what goes into each batch and when it gets sent. You have control, over `AIOKafkaProducer` and `BatchBuilder`. You can use `create_batch()` and `send_batch()` with `AIOKafkaProducer`.

---

### Technical Changes

- **`aiokafka/producer.py`**
  - New method `create_batch()` added to `AIOKafkaProducer`. Returns a `BatchBuilder` instance tied to the producer's current compression settings and `max_batch_size`.
  - New async method `send_batch(batch, topic, *, partition)` added. Takes a completed `BatchBuilder`, validates the target partition, and submits it directly to the message accumulator for the specified topic-partition.

- **`aiokafka/message_accumulator.py`**
  - Added an `add_batch()` path that accepts a pre-built `BatchBuilder` rather than individual records, bypassing the per-message append logic.
  - `BatchBuilder` class introduced here (or exposed from here) — wraps the underlying record buffer and provides an `append(key, value, timestamp)` method that returns `None` when the batch is full, signalling the caller to flush and start a new one.

- **`examples/batch_produce.py`**
  - New example script demonstrating the full create → append → send loop with dynamic partition selection.

- **`tests/test_producer.py`** (integration tests)
  - Tests covering successful batch delivery, behaviour when `send_batch` is called against the same partition while one is already in-flight (should queue, not fail), and `KafkaTimeoutError` propagation.

---

### Implementation Approach

The design focuses on a `BatchBuilder` object. It acts as an area for a group of records. The group size is fixed.

When you call `create_batch()` you get a `BatchBuilder` object. It is already set up with a compression codec and a maximum batch size limit.

You then add records to the batch in a loop. You call `batch.append(key, value timestamp)`. This returns some metadata if it works.. It returns `None` if the buffer is full.

When you get `None` you stop adding records. You call `send_batch()` with the batch. Then you create a batch to continue.

The `send_batch()` function works differently. It does not use the send()` path. It does not decide which partition to send to. It also does not change the key and value.

Instead it sends the batch directly to the accumulator for the target partition. It waits for the batch to be delivered. If another batch, for the partition is being sent, `send_batch()` waits until that batch is sent.

This approach does not change the existing `send()` path. It adds a way to send batches. This keeps the risk of problems.

---

### Potential Impact

The main users of this feature are applications that require control over batch boundaries. For example stream processors need all the records from one chunk to be in a Kafka batch. Load testing tools also use this feature because they need to produce records at a rate.

Applications that do not use `create_batch()` or `send_batch()` are not affected by this feature. One important thing to note about `send_batch()` is that it skips some steps so the person using it has to handle some work. They have to choose which partition to use and make sure their keys and values are in the format. This means the code is a bit longer and more complicated. It gives the user more control over what is happening which is a good trade-off for Kafka batch and the applications that use it, like stream processors and load testing tools.

---
