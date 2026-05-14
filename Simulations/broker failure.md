# Kafka Incident Simulation — Broker Failure and ISR Degradation

## Incident Scenario

### PagerDuty Alert

```text
[P1] Kafka Under-Replicated Partitions Critical

Environment: PROD
Cluster: payments-east-kafka
Metric: UnderReplicatedPartitions
Current Value: > 0
Expected Value: 0

Description:
One or more Kafka partitions have lost in-sync replicas.
Replication durability is degraded.

Potential Impact:
If another broker fails before ISR recovery, data durability and partition availability may be impacted.

Runbook:
KB-KAFKA-007 ISR and Broker Failure Investigation
```

---

## Business / Platform Escalation

```text
Kafka replication degradation detected.
Broker instability suspected.
Please investigate ISR shrink and broker health.
```

---

# Investigation Workflow

## Step 1 — Morning Operational Health Check

```bash
./admin/scripts/morning-check.sh
```

### Baseline Findings

* All brokers healthy
* Full ISR on all partitions
* Topics available
* Cluster healthy
* No restart loops

### Baseline ISR Example

```text
Replicas: 1,4,2
Isr: 4,2,1
```

Meaning:
All replicas are healthy and in-sync.

---

# Simulated Failure Condition

## Step 2 — Simulate Broker Failure

```bash
docker compose stop kafka-2
```

---

# Investigation

## Step 3 — Inspect Topic Replication State

```bash
kx kafka-topics --bootstrap-server kafka-1:29092 \
--describe --topic orders
```

---

## Findings

### Example Partition State

```text
Replicas: 1,4,2
Isr: 4,1
```

### Operational Interpretation

* Broker 2 is down.
* Broker 2 dropped out of ISR.
* Replication durability is degraded.
* Kafka remains available because enough ISR replicas remain alive.
* Cluster risk increased.

---

# Important Operational Reasoning

## Why This Matters

ISR represents the replicas that are fully caught up.

If ISR shrinks:

* replication durability decreases
* another broker failure becomes dangerous
* producers using acks=all may begin failing
* partition availability risk increases

---

# Risk Assessment

## Current Cluster State

| Condition                | Status   |
| ------------------------ | -------- |
| Cluster serving traffic  | YES      |
| Topic available          | YES      |
| Replication healthy      | DEGRADED |
| Durability risk elevated | YES      |
| Immediate outage         | NO       |

---

# Mitigation

## Step 4 — Recover Failed Broker

```bash
docker compose start kafka-2
```

---

# Recovery Validation

## Step 5 — Watch ISR Recovery

```bash
watch -n 3 "docker exec -e KAFKA_OPTS='' kafka-1 kafka-topics \
--bootstrap-server kafka-1:29092 \
--describe --topic orders | grep Isr"
```

### Healthy Recovery Indicators

```text
Replicas: 1,4,2
Isr: 4,2,1
```

Meaning:
Broker 2 has fully caught up and rejoined ISR.

---

# Post-Recovery Operational Action

## Step 6 — Preferred Leader Election

```bash
kx kafka-leader-election --bootstrap-server kafka-1:29092 \
--election-type preferred --all-topic-partitions
```

---

# Why Preferred Leader Election Matters

Broker failures can cause leader imbalance.

Some brokers may temporarily host too many leaders after failover.

Preferred leader election:

* redistributes leadership
* restores preferred partition leadership
* helps rebalance broker workload
* reduces hot broker risk

---

# Root Cause Analysis

## Root Cause

Intentional broker outage simulation.

Broker kafka-2 was stopped.

## Resulting Impact

* ISR shrink occurred on affected partitions.
* Replication redundancy temporarily reduced.
* Kafka remained operational due to surviving ISR replicas.

---

# Lessons Learned

## Operational Signals

| Signal                             | Interpretation               |
| ---------------------------------- | ---------------------------- |
| Broker down                        | Infrastructure degradation   |
| ISR shrink                         | Replication degraded         |
| Replicas > ISR count               | Missing in-sync replicas     |
| Topic still available              | Enough ISR replicas survived |
| Preferred leader election executed | Leadership rebalanced        |

---

# Key Operational Takeaways

* ISR health is one of the most critical Kafka operational indicators.
* Kafka can tolerate broker failures if replication factor and ISR are healthy.
* Under-replicated partitions significantly increase operational risk.
* Broker recovery must be validated before considering the incident resolved.
* Preferred leader election is important after recovery events.

---

# Real Production Mindset

During ISR incidents, platform engineers should ask:

* Which broker failed?
* How many partitions lost ISR members?
* Is the active controller healthy?
* Are producers impacted?
* Is acks=all failing?
* Are partitions under-replicated?
* Is another broker at risk?
* Is leadership balanced after recovery?

---

# Operational Maturity Insight

Strong operators do not stop at:

```text
Broker restarted successfully
```

They continue until:

* ISR fully restored
* cluster stable
* leadership balanced
* replication healthy
* risk normalized

---

# Platform Engineering Value

This simulation demonstrates:

* broker failure handling
* ISR investigation
* replication durability analysis
* operational risk assessment
* controlled recovery
* leadership rebalancing
* production-style troubleshooting workflow
