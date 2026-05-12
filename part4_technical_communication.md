# Part 4: Technical Communication

## Task 4.1: Scenario Response

**question:** "Why did you choose this specific PR over the others? What made it comprehensible to you, and what challenges do you anticipate in implementing it?"

---

I chose PR #217, which's about `create_batch` and `send_batch` because the problem it solves is easy to understand. The current `send()` API does not give you control over batch boundaries. If you need to make sure that all records from one chunk are sent to Kafka as an unit before moving on there is no good way to do that right now. The difference between what we have and what we want is clear and the new API is simple enough to understand completely. That made it seem manageable than some of the other PRs, where the problem was spread across many parts of the system or required a deep understanding of the Kafka group coordination protocol.

My experience helps in a few ways. I am comfortable with Python that uses asyncio, which includes things like coroutines, `await` background tasks and the threaded execution model. So when I read through `producer.py` and `message_accumulator.py` I did not have to learn about the concurrency model from scratch. I also know enough about Kafka concepts on the producer side, such as batching, compression, partition assignment and acknowledgement modes to understand what the `MessageAccumulator` does and where the add_batch()` path needs to fit in without disrupting the existing `add_message()` flow.

I think there are three challenges. The first one is making sure the buffer is compatible. The `BatchBuilder` needs to create a buffer that the accumulator can handle as if it built the batch itself. If the wire format or the metadata structure does not match what the sender loop expects records will. Be malformed or cause a protocol error at the broker. I would spend time carefully reading how existing `MessageBatch` objects are constructed before writing any code for `BatchBuilder`.

The second challenge is making sure the ordering is correct when sending batches quickly. When `send_batch()` is called twice on the partition in a row the second call must wait for the first broker acknowledgement before sending. Getting this right without causing a deadlock especially if the first batch fails and the future never resolves cleanly requires handling of the wait logic and the error propagation path.

The third challenge is the contract that says `append()` returns `None`. It seems simple. Once a batch is full every subsequent `append()` must keep returning `None` without touching the records that are already staged. If the internal buffer check has an off-, by-one error you could end up with a batch that appears full but has one record appended past the limit. I would write a targeted unit test for boundary behaviour before putting everything together.

---

**I declare that all written content in this assessment is my own work, created without
the use of AI language models or automated writing tools. All technical analysis and
documentation reflects my personal understanding and has been written in my own words.**
