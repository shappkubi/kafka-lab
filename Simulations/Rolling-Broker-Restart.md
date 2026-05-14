# Kafka Incident Simulation — Rolling Broker Restart During Planned Maintenance

## Task / Change Request

### Change Ticket

```text
CRQ-2026-0514

Title:
Planned rolling restart of Kafka brokers for maintenance and JVM parameter update.

Environment:
PROD

Risk:
Medium

Objective:
Restart Kafka brokers sequentially without impacting cluster availability or replication durability.

Validation Requirements:
- No sustained ISR degradation
- No unavailable partitions
- No producer outage
- Consumer lag remains stable
```

---

# Pre-Checks / Initial Investigation

## Morning Operational Validation

Executed:

```bash
./admin/scripts/morning-check.sh
```

### Baseline Findings

* All brokers healthy
* Full ISR on all partitions
* Topics healthy
* Cluster responding normally
* Consumer group present
* No restart loops

Example healthy ISR state:

```text
Replicas: 1,4,2
Isr: 4,1,2
```

Meaning:
All replicas are healthy and fully synchronized.

---

# Maintenance Execution

## Broker 1 Restart

Executed:

```bash
docker compose restart kafka-1
```

### Observed Behavior

* Broker 1 temporarily dropped from ISR
* Partition leadership shifted to surviving brokers
* Cluster remained available
* Consumer process running inside restarted broker exited during restart

Example degraded state:

```text
Replicas: 1,4,2
Isr: 4,2
```

Operational Interpretation:

* Replication temporarily degraded
* Kafka remained operational because sufficient ISR replicas survived
* Leadership failover occurred correctly

---

## Broker 2 Restart

Executed:

```bash
docker compose restart kafka-2
```

Observed:

* temporary ISR shrink
* successful recovery after broker returned

---

## Broker 3 Restart

Executed:

```bash
docker compose restart kafka-3
```

Observed:

* temporary ISR shrink
* partition leadership movement
* successful recovery

---

## Broker 4 Restart

Executed:

```bash
docker compose restart kafka-4
```

Observed:

* temporary ISR shrink
* cluster remained healthy
* ISR eventually restored

---

# ISR Degradation Detection

After broker restart, ISR degradation was validated using:

```bash
kx kafka-topics --bootstrap-server kafka-2:29093 \
--describe --topic orders
```

Example degraded ISR output:

```text
Topic: orders Partition: 1
Replicas: 1,4,2
Isr: 4,2
```

Additional degraded examples observed:

```text
Replicas: 2,1,3
Isr: 3,2

Replicas: 4,1,2
Isr: 4,2
```

Operational Interpretation:

* restarted broker temporarily removed from ISR
* replication durability reduced temporarily
* cluster availability maintained
* partition leadership shifted automatically

---

# Real-Time Recovery Monitoring

ISR recovery was continuously monitored using:

```bash
watch -n 3 "docker exec -e KAFKA_OPTS='' kafka-3 kafka-topics \
--bootstrap-server kafka-3:29094 \
--describe --topic orders | grep Isr"
```

Purpose:

* validate ISR restoration
* confirm broker recovery
* ensure cluster stability before continuing maintenance

---

# Recovery Validation

Healthy recovery state observed:

```text
Replicas: 1,4,2
Isr: 1,2,4
```

Operational Meaning:

* restarted broker successfully rejoined ISR
* replication fully restored
* cluster durability normalized
* maintenance could safely continue

---

# Critical Operational Discipline

Key operational rule reinforced during this simulation:

```text
restart one broker
wait for ISR recovery
validate cluster health
proceed to next broker
```

Reason:
Restarting multiple brokers too quickly significantly increases risk of:

* under-replicated partitions
* partition unavailability
* producer failures
* cluster instability

---

# Post-Change Validation

Executed:

```bash
./admin/scripts/morning-check.sh
```

Confirmed:

* all brokers running
* cluster healthy
* full ISR restored on all partitions
* no under-replicated partitions remaining

Final healthy cluster examples:

```text
Replicas: 1,4,2
Isr: 1,2,4

Replicas: 2,1,3
Isr: 1,2,3
```

---

# Post-Change Operational Action

Executed preferred leader election:

```bash
kx kafka-leader-election --bootstrap-server kafka-1:29092 \
--election-type preferred --all-topic-partitions
```

Purpose:

* rebalance partition leadership
* restore preferred leaders
* reduce uneven broker leadership distribution after maintenance

---

# Postmortem / Lessons Learned

## Summary

A planned rolling broker restart was successfully completed across all Kafka brokers without causing cluster outage or partition unavailability.

## Key Findings

* Restarting brokers temporarily removes them from ISR.
* Kafka maintained availability because sufficient ISR replicas survived.
* Partition leadership automatically failed over during broker restart.
* Preferred leader election helped rebalance leadership after recovery.

## Operational Takeaways

* ISR health is one of the most critical Kafka operational indicators.
* Rolling maintenance requires strict sequential execution discipline.
* Recovery validation between broker restarts is mandatory.
* Maintenance operations require the same discipline as incident response workflows.
* Kafka durability risk increases significantly during ISR degradation events.

## Real Production Relevance

This simulation reflects real enterprise platform engineering activities including:

* rolling broker maintenance
* JVM/configuration updates
* infrastructure patching
* controlled change windows
* replication validation
* operational risk management
* production change execution discipline
