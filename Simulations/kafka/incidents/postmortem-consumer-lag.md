# Kafka Incident Simulation — Consumer Lag Investigation

## Incident Scenario

### PagerDuty Alert

```text
[P1] Kafka Consumer Lag Critical

Environment: PROD
Cluster: payments-east-kafka
Consumer Group: slow-consumer-group
Topic: orders
Current Lag: 100,000+
Threshold: 25,000
Duration: 10 minutes

Description:
Consumer lag for group slow-consumer-group has exceeded critical threshold.
Messages are accumulating faster than they are being consumed.

Potential Impact:
Delayed order/payment processing.
Downstream applications may experience stale data.

Runbook:
KB-KAFKA-004 Consumer Lag Investigation
```

---

## Business Escalation

```text
Application team reporting delayed order processing.
Kafka consumer backlog suspected.
Please investigate consumer health and broker status.
```

---

# Investigation Workflow

## Step 1 — Morning Operational Health Check

```bash
./admin/scripts/morning-check.sh
```

### Operational Observations

* Brokers healthy
* ISR healthy
* Topics available
* Cluster responding normally
* No broker restart loops

### Initial Conclusion

Kafka brokers appear healthy.
Likely issue is consumer-side rather than broker-side.

---

## Step 2 — Investigate Consumer Group

```bash
kx kafka-consumer-groups \
--bootstrap-server kafka-1:29092 \
--describe \
--group slow-consumer-group
```

### Findings

```text
Consumer group 'slow-consumer-group' has no active members.
```

### Lag Findings

```text
LAG ~100,000 messages
```

### Partition Distribution

Lag distributed fairly evenly across partitions.

### Operational Interpretation

* Producers still writing normally
* Consumers not active
* Even lag distribution
* No hot partition indicators
* Strong indication of consumer outage or crash

---

# Simulated Incident Condition

## Generate Producer Load

```bash
kx kafka-producer-perf-test \
--topic orders \
--num-records 100000 \
--record-size 200 \
--throughput -1 \
--producer-props bootstrap.servers=kafka-1:29092 acks=1
```

### Result

Backlog generated successfully.
Consumer lag increased significantly.

---

# Mitigation

## Start Consumer

```bash
kx kafka-console-consumer \
--bootstrap-server kafka-1:29092 \
--topic orders \
--group slow-consumer-group \
> /tmp/drain.log 2>&1 &
```

---

# Validation

## Monitor Lag Drain

```bash
watch -n 3 "docker exec -e KAFKA_OPTS='' kafka-1 kafka-consumer-groups \
--bootstrap-server kafka-1:29092 \
--describe \
--group slow-consumer-group"
```

### Healthy Recovery Indicators

```text
CURRENT-OFFSET == LOG-END-OFFSET
LAG = 0
CONSUMER-ID present
```

---

# Root Cause Analysis

## Root Cause

Consumer process was not running while producers continued publishing messages.

## Why Kafka Was Not the Root Cause

* Brokers healthy
* ISR healthy
* Replication healthy
* Producers functioning
* Metadata responding normally

## Actual Fault Domain

Consumer-side outage.

---

# Lessons Learned

## Operational Signals

| Signal                     | Interpretation            |
| -------------------------- | ------------------------- |
| No active consumer members | Consumer outage/crash     |
| Growing lag                | Consumers not keeping up  |
| Even lag across partitions | Systemic consumer issue   |
| ISR healthy                | Replication healthy       |
| Producers healthy          | Kafka cluster operational |

---

# Key Operational Takeaways

* Kafka lag is often a symptom, not the root cause.
* First determine whether the issue is broker-side or application-side.
* ISR health is critical during incident analysis.
* Avoid blindly restarting brokers before gathering evidence.
* Operational troubleshooting is about reducing uncertainty methodically.

---

# Real Production Mindset

During incidents, platform engineers should ask:

* What changed?
* Is the issue infrastructure or application related?
* What is the blast radius?
* Are brokers healthy?
* Are consumers active?
* Is lag growing or stabilizing?
* Is replication healthy?
* Is there customer impact?

---

# GitOps / Documentation Value

This simulation demonstrates:

* operational runbooks
* incident response
* evidence gathering
* mitigation validation
* postmortem documentation
* Kafka platform operations workflow
