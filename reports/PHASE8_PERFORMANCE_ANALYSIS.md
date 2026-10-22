# Phase 8 - Batch vs Streaming Performance and Scalability

## Experimental design

This is a controlled local scalability experiment, not a distributed-cluster
benchmark. Deterministic OHLCV workloads of 10,000, 25,000, 50,000, and 100,000
unique logical events were generated across six configured symbols and three sectors.
The identical business events were processed by Batch and replayed through Kafka to
Spark Structured Streaming. Streaming received one additional control event per run
to advance the watermark; that event did not produce a finalized analytical window.

The primary workload used fixed 5, 15, and 30-minute windows. This representative set
was selected because running ten independent stateful queries for all five supported
durations would make 24 isolated local runs disproportionately expensive. Support for
1 and 60-minute windows remains unchanged. One 1,000-event warm-up and three measured
repetitions per workload and architecture were performed. Every Streaming run used a
unique topic, checkpoint, and output directory with a constant five-second trigger.

Processing latency is measured from the Kafka record timestamp to `processing_time`
in the committed valid sink. It does not use historical event time. Because each
bounded workload was produced before Spark consumption began, this latency includes
queue residence and Spark startup and should not be interpreted as steady-state live
latency. Total Streaming duration additionally measures production through final
analytical output completion.

## Mean results

| Events | Batch duration (s) | Batch rows/s | Streaming duration (s) | Streaming events/s | Streaming p95 latency (ms) | Batch peak memory (MiB) | Streaming peak memory (MiB) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 77.13 | 316.03 | 118.40 | 97.30 | 17,782.67 | 967.91 | 2,127.44 |
| 25,000 | 68.21 | 768.75 | 150.06 | 186.19 | 113,043.00 | 1,050.90 | 2,258.79 |
| 50,000 | 90.03 | 1,192.04 | 313.72 | 180.17 | 261,725.33 | 1,066.77 | 2,376.55 |
| 100,000 | 99.90 | 2,028.61 | 650.70 | 165.15 | 558,115.00 | 1,096.87 | 2,449.92 |

Batch startup averaged 9.70-15.44 seconds, while measured Batch processing averaged
31.83-53.25 seconds. This fixed JVM/Spark cost explains why total duration is not
monotonic at the smallest levels and why Batch throughput increases substantially as
the workload grows. Streaming throughput improved from 10,000 to 25,000 events but
then flattened and declined. The 100,000-event workload took approximately 6.5 times
as long as Batch and retained about 2.45 GiB at peak in the Spark container.

Producer throughput (16.6k-38.9k events/s) was much higher than Spark throughput
(97-186 events/s). The bounded producer-first design therefore created backlog by
construction. All final Kafka lags were zero, no events were dropped by the watermark,
and no run failed, but increasing p95 latency shows that Spark did not consume at the
burst replay rate. The maximum observed state-store size remained near 10,113 rows,
consistent with the configured 10,000 offsets per trigger and watermark eviction.

## Correctness and interpretation

All 24 measured runs completed. Each had the expected input count, zero invalid
events, zero logical duplicates, zero duplicate analytical keys, and existing output.
Batch and Streaming generated identical analytical row counts for equivalent inputs:
4,518; 11,259; 22,509; and 45,018 rows respectively. One interrupted 50,000-event run
was identified as contaminated during audit, retained outside the consolidated sample,
and replaced by a clean run with a new topic and state paths.

For bounded historical data already at rest, Batch achieved higher throughput and
simpler completion semantics. Streaming provides continuous incremental availability,
but pays for Kafka transport, independent queries, checkpoints, watermark management,
and state. The experiment does not establish that one architecture is universally
better. It characterizes their trade-offs under a burst replay on one machine.

Batch CPU is explicitly unavailable for this campaign because the initial host
sampler did not reliably attribute Java child-process CPU. Batch peak memory and
Docker-level Streaming CPU/memory remain usable. Other limitations include one Kafka
broker, one `local[*]` Spark container, eight logical CPUs, OS scheduling noise,
Docker/WSL2 overhead, synthetic replay rather than exchange traffic, no explicit
container memory limit, and no basis for extrapolation to a production cluster.
