# Part 2: Pull Request Analysis

**Repository selected:** [aio-libs/aiokafka](https://github.com/aio-libs/aiokafka)

**PRs selected:** [#143](https://github.com/aio-libs/aiokafka/pull/143) and [#217](https://github.com/aio-libs/aiokafka/pull/217)

---

## PR #143 — Added metadata change listener if `group_id` is None

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

The core fix is a targeted use of the `AIOKafkaClient`'s listener registration API. In the existing code, only the `GroupCoordinator` registered a metadata listener (`_handle_metadata_update`). The PR adds a lightweight equivalent directly inside `AIOKafkaConsumer.start()` when `group_id is None`. The listener callback inspects the new metadata snapshot — specifically the partition set for the subscribed topics — and compares it to the consumer's currently assigned partitions. If new partitions appear, it calls the fetcher's partition-update path to start fetching from them.

The approach is deliberately minimal: rather than duplicating the full `GroupCoordinator` logic, the PR inserts a single callback that does only the one thing a standalone consumer actually needs on a metadata change — updating its partition assignment. Because `asyncio` callbacks are single-threaded, there are no concurrency concerns around the listener itself. The corresponding `stop()` teardown removes the listener, which matches how the `GroupCoordinator` already cleans itself up, keeping the lifecycle symmetric.

---

### Potential Impact

This fix directly affects all users running `AIOKafkaConsumer` without a `group_id` — typically cases where a consumer is manually managing its own partition assignments (e.g., a replay tool or an audit log reader). The change makes those consumers responsive to broker-side changes that previously went undetected. There is minimal risk to group-based consumers because the new listener registration is entirely inside the `if self._group_id is None` branch; the standard coordinator-based path is untouched.

---

---

## PR #217 — Added lightweight batching interface to `AIOKafkaProducer`

**Link:** https://github.com/aio-libs/aiokafka/pull/217

---

### PR Summary

Before this PR, the only way to send messages with `AIOKafkaProducer` was through `send()` or `send_and_wait()`, both of which hand over control of batching entirely to the internal message accumulator. That works well for most cases, but there are situations — like when you're reading from an incoming stream that can't advance until delivery is confirmed — where you genuinely need to build a batch yourself, decide when it's full, and submit it as a unit. Calling `send_and_wait()` in a loop for each message is far too slow for that. This PR introduces two new methods, `create_batch()` and `send_batch()`, that let callers construct a `BatchBuilder` object, append messages to it manually, and then dispatch the whole thing to a specific partition in one shot. It's a lower-level API that skips the automatic partitioner and serializer, but in exchange gives you direct control over exactly what goes into each batch and when it gets sent.

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

The design centres on a `BatchBuilder` object that acts as a staging area for a fixed-size group of records. When you call `create_batch()`, the producer hands back a `BatchBuilder` pre-configured with the producer's compression codec and the `max_batch_size` limit. You then call `batch.append(key, value, timestamp)` in a loop — this returns a future-like metadata object on success, or `None` when the buffer is full. That `None` return is the signal to the caller to stop appending, call `send_batch()` with the current batch, and then create a fresh one to continue.

Under the hood, `send_batch()` bypasses the normal `send()` path — it does not invoke the partitioner, does not apply key/value serializers, and does not go through the per-message accumulator logic. Instead it drops the pre-built buffer directly into the accumulator for the target partition and waits for the delivery future. If another batch for that partition is already in flight, the call blocks until that earlier batch is acknowledged before submitting the new one, which keeps ordering guarantees intact.

This approach keeps the existing `send()` path completely untouched. It's an additive API that sits alongside the automatic path rather than refactoring it, which keeps the risk of regressions low.

---

### Potential Impact

The main users of this are applications that need deterministic control over batch boundaries — things like stream processors where you want all the records from one upstream chunk to land in a single Kafka batch, or load testing tools that need to produce at a specific rate. Any code that doesn't call `create_batch()` or `send_batch()` is completely unaffected. One thing worth noting is that because `send_batch()` skips the key serializer and partitioner, callers have to handle their own partition selection and pre-encode their keys and values as bytes — it's more verbose to use, but that's a reasonable trade-off for the control it gives.

---
