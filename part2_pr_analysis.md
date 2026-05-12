# Part 2: Pull Request Analysis

**Repository selected:** [aio-libs/aiokafka](https://github.com/aio-libs/aiokafka)

**PRs selected:** [#143](https://github.com/aio-libs/aiokafka/pull/143) and [#193](https://github.com/aio-libs/aiokafka/pull/193)

---

## PR #143 — Added metadata change listener if `group_id` is None

**Link:** https://github.com/aio-libs/aiokafka/pull/143

---

### PR Summary

When a consumer is created **without** a `group_id`, it operates in a standalone mode — it has no Group Coordinator and manages its own partition assignments. Before this PR, that standalone consumer was never registering a metadata change listener with the Kafka client. The `GroupCoordinator` normally handles metadata-change events and triggers a partition-reassignment check, but since a standalone consumer skips the `GroupCoordinator`, nothing was watching for metadata updates. The result: if a topic gained new partitions after the consumer started, the consumer would never discover them and would silently ignore the new data. This PR plugs that gap by wiring a metadata listener directly into the consumer when there is no group, so partition discovery works correctly regardless of whether a `group_id` is provided.

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

## PR #193 — Added `seek_to_beginning` and `seek_to_end` API

**Link:** https://github.com/aio-libs/aiokafka/pull/193

---

### PR Summary

Before this PR, `AIOKafkaConsumer` only offered a low-level `seek(partition, offset)` method that required the caller to already know the exact numeric offset to jump to. There was no convenient way to reset a consumer to the very first available message in a partition or fast-forward it to the latest position without first making a separate network call to resolve the offset. This PR introduces two high-level async methods — `seek_to_beginning(*partitions)` and `seek_to_end(*partitions)` — which handle the broker communication internally, resolve the actual boundary offsets (earliest and latest respectively), and then update the consumer's internal fetch position in one call. This mirrors the API that Java's Kafka client has offered for some time and closes a usability gap for Python developers building replay, catchup, or startup-positioning logic.

---

### Technical Changes

- **`aiokafka/consumer.py`**
  - New public async method `seek_to_beginning(*partitions)` added to `AIOKafkaConsumer`.
  - New public async method `seek_to_end(*partitions)` added to `AIOKafkaConsumer`.
  - Both methods validate input partitions against the current assignment, then delegate to the fetcher.

- **`aiokafka/fetcher.py`**
  - Internal logic added to issue `ListOffsetsRequest` to the broker with the appropriate sentinel timestamp (`-2` for earliest, `-1` for latest).
  - After receiving broker responses, the fetcher updates its per-partition offset position using the existing `seek_to()` path.

- **`docs/`**
  - API reference documentation updated to describe both methods, their arguments, and the expected behaviour when called before or after `start()`.

- **`tests/test_consumer.py`** (integration tests)
  - Test cases verify that after calling `seek_to_beginning`, the next `getone()` returns the partition's earliest stored message.
  - Test cases verify that after calling `seek_to_end`, no messages are received until new ones are produced.

---

### Implementation Approach

The two methods follow a fetch-then-seek pattern. When `seek_to_beginning(tp)` is called, the method builds a `ListOffsetsRequest` with the special timestamp value `-2` (Kafka's wire-protocol sentinel for "earliest offset") for each requested `TopicPartition`. It sends this request to the relevant broker — determined by the partition's current leader from the cluster metadata — and awaits the response. Once the broker replies with the actual earliest offset number for each partition, the method calls `self._fetcher.seek_to(tp, offset)` for each one, overwriting whatever the current fetch position is.

`seek_to_end` is identical except it uses timestamp `-1` (Kafka's "latest offset" sentinel). If no partitions are passed as arguments, both methods default to applying the seek across the entire current assignment of the consumer.

This design keeps the implementation clean because the heavy lifting (partition leader lookup, request routing, network I/O) was already in the fetcher layer. The PR essentially adds a public-facing convenience layer that translates a user intent ("start from the beginning") into the existing low-level machinery, without duplicating network logic.

---

### Potential Impact

Any code that currently works around the absence of these methods — for example, manually calling `beginning_offsets()` followed by `seek()` in a loop — can be simplified. More importantly, applications that need reliable replay semantics (e.g., re-processing a full topic on startup) gain a race-condition-free path: previously, the gap between fetching offsets and calling `seek()` could let the consumer read a few records it then skipped. Existing consumers that do not call these methods are entirely unaffected.
