# Kafka Lab — Multi-Broker Cluster with Monitoring

A hands-on Apache Kafka practice environment running a 3-broker cluster with full monitoring stack, designed for learning Kafka administration through realistic scenarios — including broker failures, partition reassignment, consumer lag, security, and disaster recovery.

This project was built specifically as preparation for a Kafka admin role interview, with emphasis on the operational scenarios that distinguish admin-level experience from basic Kafka usage.

---

## Table of Contents

- [Why This Project](#why-this-project)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
  - [Option A: GitHub Codespaces (Recommended)](#option-a-github-codespaces-recommended)
  - [Option B: Local Linux/WSL with Docker Desktop](#option-b-local-linuxwsl-with-docker-desktop)
- [Project Structure](#project-structure)
- [Configuration Files Explained](#configuration-files-explained)
- [Daily Operations](#daily-operations)
- [Verification](#verification)
- [Web UI Access](#web-ui-access)
- [The lab-up.sh Resume Script](#the-lab-upsh-resume-script)
- [Hands-On Scenarios](#hands-on-scenarios)
  - [Scenario 1: Broker Failure & Recovery](#scenario-1-broker-failure--recovery)
  - [Scenario 2: Consumer Lag](#scenario-2-consumer-lag)
  - [Scenario 3: Adding a Broker & Partition Reassignment](#scenario-3-adding-a-broker--partition-reassignment)
  - [Scenario 4: Rolling Restart](#scenario-4-rolling-restart)
  - [Scenario 5: Topic Configuration Changes](#scenario-5-topic-configuration-changes)
  - [Scenario 6: SASL Authentication & ACLs](#scenario-6-sasl-authentication--acls)
  - [Scenario 7: GC Pauses & Heap Tuning](#scenario-7-gc-pauses--heap-tuning)
  - [Scenario 8: Disaster Recovery (MirrorMaker 2)](#scenario-8-disaster-recovery-mirrormaker-2)
  - [Scenario 9: KRaft Mode (No ZooKeeper)](#scenario-9-kraft-mode-no-zookeeper)
  - [Scenario 10: Performance Benchmarking](#scenario-10-performance-benchmarking)
- [Troubleshooting](#troubleshooting)
- [Command Reference](#command-reference)
- [Key Metrics for Admins](#key-metrics-for-admins)
- [Interview-Ready Concepts](#interview-ready-concepts)
- [Resources](#resources)

---

## Why This Project

Kafka admin roles consistently filter out candidates whose only experience is producing/consuming messages. Hiring managers want to hear concrete operational stories: rolling restarts, partition reassignment, broker failure recovery, ISR debugging, JMX monitoring, security configuration.

This lab provides a production-shaped environment to practice all of those skills in a safe, isolated setting where you can deliberately break things and watch the cluster react.

**Hardware constraint:** This lab is designed to run in GitHub Codespaces (4GB free tier RAM is enough for the full stack), making it accessible even on low-spec laptops where local multi-broker Kafka isn't feasible.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Docker Compose Network                     │
│                                                                  │
│  ┌──────────────┐                                               │
│  │  ZooKeeper   │←─────── manages cluster metadata              │
│  │  :2181       │                                                │
│  └──────┬───────┘                                                │
│         │                                                        │
│         │           ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│         └──────────→│ kafka-1  │  │ kafka-2  │  │ kafka-3  │   │
│                     │ :9092    │←→│ :9093    │←→│ :9094    │   │
│                     │ JMX:9991 │  │ JMX:9992 │  │ JMX:9993 │   │
│                     └────┬─────┘  └────┬─────┘  └────┬─────┘   │
│                          │              │              │         │
│  ┌──────────────┐        │              │              │         │
│  │  Kafka UI    │←───────┼──────────────┼──────────────┤         │
│  │  :8080       │        │              │              │         │
│  └──────────────┘        │              │              │         │
│                          │              │              │         │
│  ┌──────────────┐        │              │              │         │
│  │ Prometheus   │←───────┴──────────────┴──────────────┘         │
│  │  :9090       │  scrapes JMX exporter on each broker          │
│  └──────┬───────┘                                                │
│         │                                                        │
│  ┌──────┴───────┐                                                │
│  │   Grafana    │                                                │
│  │   :3000      │                                                │
│  └──────────────┘                                                │
└─────────────────────────────────────────────────────────────────┘
```

**Component summary:**

| Service | Port | Purpose |
|---|---|---|
| ZooKeeper | 2181 | Cluster metadata and controller election |
| Kafka brokers (×3) | 9092, 9093, 9094 | Message storage and serving |
| JMX exporters (per broker) | 9991, 9992, 9993 | Expose Kafka JMX metrics in Prometheus format |
| Kafka UI | 8080 | Visual cluster inspection |
| Prometheus | 9090 | Time-series metrics collection |
| Grafana | 3000 | Dashboards and alerting |

---

## Prerequisites

**For Codespaces path (recommended):**
- A GitHub account (free tier: 60 hours/month of Codespaces)
- A web browser

**For local path:**
- Windows 10/11 with WSL 2 (or native Linux/Mac)
- Docker Desktop with at least 8GB RAM allocated
- Git

---

## Quick Start

If you've already cloned the repo and have Docker available:

```bash
docker compose up -d
docker compose ps                          # confirm 7 services running
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 \
  --create --topic orders --partitions 6 --replication-factor 3
```

Then open the web UIs (ports 8080, 9090, 3000) and you're ready to start [Scenario 1](#scenario-1-broker-failure--recovery).

For full setup-from-scratch instructions, continue to [Detailed Setup](#detailed-setup).

---

## Detailed Setup

### Option A: GitHub Codespaces (Recommended)

Best for laptops with limited RAM (under 16GB). The entire stack runs in GitHub's cloud.

**1. Create a Codespace:**

- Go to your repo on GitHub
- Click the green **Code** button → **Codespaces** tab → **Create codespace on main**
- Wait 1-2 minutes for the environment to provision

**2. Verify Docker is preinstalled:**

```bash
docker --version
docker compose version
free -h
nproc
```

You should see Docker 24+, Compose v2+, around 7-8GB RAM, and 2 CPUs.

**3. Bring up the stack:**

```bash
cd /workspaces/kafka-lab
docker compose up -d
```

First run downloads ~3GB of images (5-10 minutes). Subsequent runs are fast.

**4. Verify all 7 services:**

```bash
docker compose ps
```

All should show `Up`.

### Option B: Local Linux/WSL with Docker Desktop

Best if you have a laptop with 16GB+ RAM and want to work offline.

**1. Enable WSL 2 (Windows only):**

In PowerShell as Administrator:

```powershell
wsl --install
```

Restart when prompted.

**2. Install Docker Desktop:**

Download from https://www.docker.com/products/docker-desktop/. During install:
- Check "Use WSL 2 instead of Hyper-V"
- After install: Settings → Resources → WSL Integration → enable for Ubuntu
- Settings → Resources → Advanced → set Memory to at least 8GB

**3. Configure WSL memory in `~/.wslconfig`:**

```
[wsl2]
memory=8GB
processors=4
swap=2GB
```

Apply with `wsl --shutdown` from PowerShell, then reopen WSL.

**4. Install supporting tools in WSL:**

```bash
sudo apt update
sudo apt install -y git python3 python3-pip kafkacat jq curl
```

**5. Clone the repo and bring up the stack:**

```bash
cd ~
git clone <your-repo-url> kafka-lab
cd kafka-lab
docker compose up -d
```

---

## Project Structure

```
kafka-lab/
├── README.md                                # This file
├── docker-compose.yml                       # Full stack definition
├── lab-up.sh                                # Resume script for daily use
├── .gitignore
│
├── prometheus/
│   └── prometheus.yml                       # Scrape config for JMX exporters
│
├── jmx-exporter/
│   ├── jmx_prometheus_javaagent.jar         # JMX exporter Java agent
│   └── kafka-jmx.yml                        # JMX → Prometheus name mapping rules
│
├── grafana/
│   └── provisioning/
│       ├── dashboards/                      # (auto-provisioned dashboards go here)
│       └── datasources/
│           └── prometheus.yml               # Auto-connect Grafana to Prometheus
│
└── clients/                                 # (optional) Producer/consumer scripts
```

---

## Configuration Files Explained

### `docker-compose.yml`

Defines 7 services on a shared Docker network:

- **ZooKeeper**: Single-node ZK for cluster metadata. Production uses 3+ ZK nodes.
- **kafka-1, kafka-2, kafka-3**: Three brokers with unique broker IDs and ports. Each loads the JMX exporter as a Java agent on a unique port (9991/9992/9993).
- **kafka-ui**: Provectus Kafka UI for visual cluster inspection.
- **prometheus**: Scrapes the three JMX exporter endpoints every 15s.
- **grafana**: Dashboards backed by Prometheus.

**Critical broker config notes:**

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3            # internal __consumer_offsets topic
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3    # for transactions
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2               # quorum for transactions
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"             # production-style: explicit topic creation
KAFKA_HEAP_OPTS: "-Xmx512M -Xms512M"                 # tight heap for Codespaces
```

**Dual-listener pattern:**

Each broker exposes two listeners:
- `PLAINTEXT` on the host-mapped port (9092/9093/9094) — for connections from the host
- `PLAINTEXT_INTERNAL` on internal port (29092/29093/29094) — for broker-to-broker traffic

This is the most common docker-compose Kafka gotcha. If you only have one listener, either host clients or internal cluster traffic will fail.

### `prometheus/prometheus.yml`

Scrape configuration:

```yaml
scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets:
          - 'kafka-1:9991'
          - 'kafka-2:9992'
          - 'kafka-3:9993'
```

Prometheus pulls metrics from each broker's JMX exporter HTTP endpoint every 15 seconds.

### `jmx-exporter/kafka-jmx.yml`

Translates JMX metric names to Prometheus-friendly names using regex rules:

```yaml
rules:
  - pattern: kafka.server<type=(.+), name=(.+)PerSec\w*><>Count
    name: kafka_server_$1_$2_total
```

This rule turns JMX `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec` into Prometheus `kafka_server_brokertopicmetrics_messagesin_total`.

### `grafana/provisioning/datasources/prometheus.yml`

Auto-provisions the Prometheus datasource at Grafana startup so you don't have to configure it manually:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
```

---

## Daily Operations

### Starting the lab

```bash
docker compose up -d           # first time / after `down`
docker compose start            # if containers exist but are stopped
```

### Stopping the lab

```bash
docker compose stop             # preserves containers and data
docker compose down             # removes containers (data persists in volumes)
docker compose down -v          # removes containers AND data (destructive)
```

### Checking status

```bash
docker compose ps               # show running services
docker compose ps -a            # include stopped/exited containers
docker compose logs <service>   # logs from one service
docker compose logs -f <service># follow logs in real time
```

### Convenient aliases (add to `~/.bashrc`)

The Confluent Kafka images make CLI tools inherit `KAFKA_OPTS`, which conflicts with the JMX exporter when running `docker exec`. Wrap exec calls to clear the variable:

```bash
alias kx='docker exec -e KAFKA_OPTS="" kafka-1'
alias kxi='docker exec -i -e KAFKA_OPTS="" kafka-1'
```

After this:

```bash
kx kafka-topics --bootstrap-server kafka-1:29092 --list
echo "msg" | kxi kafka-console-producer --bootstrap-server kafka-1:29092 --topic orders
```

---

## Verification

### Cluster health

```bash
kx kafka-broker-api-versions --bootstrap-server kafka-1:29092 | head -5
```

If it returns broker info, the cluster is up.

### Create your first topic

```bash
kx kafka-topics --bootstrap-server kafka-1:29092 \
  --create --topic orders --partitions 6 --replication-factor 3

kx kafka-topics --bootstrap-server kafka-1:29092 --describe --topic orders
```

You should see 6 partitions, each with `Replicas: 1,2,3` and `Isr: 1,2,3` in some order.

### End-to-end produce/consume

```bash
echo '{"order":"test"}' | \
  kxi kafka-console-producer --bootstrap-server kafka-1:29092 --topic orders

kx kafka-console-consumer --bootstrap-server kafka-1:29092 \
  --topic orders --from-beginning --max-messages 1
```

---

## Web UI Access

### In Codespaces

Click the **PORTS** tab in the bottom panel of VS Code. You'll see auto-forwarded ports. Hover any row → click the **🌐 globe icon** to open in browser.

### Locally

Open `http://localhost:<port>` in your browser.

### What each UI is for

**Kafka UI (port 8080):**
- Browse topics, partitions, consumer groups
- Inspect messages in topics
- See ISR status and partition leaders visually
- Best for "show me what's happening right now"

**Prometheus (port 9090):**
- Status → Targets: confirm broker scrapes are healthy
- Query → run PromQL like `kafka_server_replicamanager_underreplicatedpartitions`
- Best for raw metric inspection and ad-hoc queries

**Grafana (port 3000):** (login `admin` / `admin`)
- Dashboards visualizing the metrics Prometheus collects
- Best for time-series views and incident monitoring
- Build custom dashboards by adding panels with PromQL queries

---

## The lab-up.sh Resume Script

After a Codespace pause/resume or local laptop reboot, the containers stop. Use this script to restart everything in one command:

```bash
#!/bin/bash
set -e

cd /workspaces/kafka-lab

echo "Starting Kafka lab..."

if ! docker compose start 2>/dev/null; then
  echo "  Containers not found — running 'docker compose up -d'..."
  docker compose up -d
fi

sleep 8
docker compose ps
docker exec -e KAFKA_OPTS="" kafka-1 kafka-broker-api-versions \
  --bootstrap-server kafka-1:29092 2>/dev/null | head -1 \
  && echo "  Broker 1 responding" \
  || echo "  Broker 1 not ready — retry in 30s"
```

Run with `./lab-up.sh`.

---

## Hands-On Scenarios

These scenarios are designed in priority order for interview preparation. Each builds skills that compound into the next.

### Scenario 1: Broker Failure & Recovery

**The most fundamental admin scenario.** What happens when a broker dies, and how to recover.

**Setup:** With the cluster healthy, start a continuous producer:

```bash
while true; do
  echo "{\"id\":\"$RANDOM\"}" | \
    kxi kafka-console-producer --bootstrap-server kafka-1:29092 --topic orders 2>/dev/null
  sleep 0.2
done
```

**Action: Kill kafka-2:**

```bash
docker compose stop kafka-2
```

**Observe within 10-30 seconds:**

```bash
kx kafka-topics --bootstrap-server kafka-1:29092 --describe --topic orders
```

- ISR shrinks from `1,2,3` to `1,3` for every partition
- Partitions previously led by broker 2 get new leaders (1 or 3)
- The producer keeps working — no data lost

**Grafana shows:**
- Under-Replicated Partitions: jumps to 56 (6 from `orders` + 50 from `__consumer_offsets`)
- Brokers Up: drops 3 → 2
- Active Controllers: stays at 1

**Recovery: Restart kafka-2:**

```bash
docker compose start kafka-2
```

Watch ISR grow back to `1,2,3` and URP return to 0.

**Final step: Rebalance leadership (often forgotten):**

```bash
kx kafka-leader-election --bootstrap-server kafka-1:29092 \
  --election-type preferred --all-topic-partitions
```

After a broker comes back, leadership doesn't auto-rebalance. Run preferred leader election to redistribute it.

**Interview soundbite:** "When a broker fails, ISR shrinks for every partition replicated on that broker. Producers transparently fail over as long as `min.insync.replicas` is met. After recovery, I always run preferred replica election to rebalance leadership — otherwise surviving brokers stay overloaded with extra leadership work."

### Scenario 2: Consumer Lag

**The most-asked production question in admin interviews.**

**Setup:** Fast producer + slow consumer.

```bash
# Terminal 1 — fast producer
while true; do
  echo "{\"id\":\"$RANDOM\"}" | \
    kxi kafka-console-producer --bootstrap-server kafka-1:29092 --topic orders 2>/dev/null
done

# Terminal 2 — intentionally slow consumer
kx kafka-console-consumer --bootstrap-server kafka-1:29092 \
  --topic orders --group slow-consumer-group \
  --consumer-property "max.poll.records=10"

# Terminal 3 — monitor lag
watch -n 2 "docker exec -e KAFKA_OPTS='' kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 --describe --group slow-consumer-group"
```

**Watch:** The LAG column climbs continuously.

**Solutions to demonstrate:**
1. Add more consumers in the same group (parallelism caps at partition count)
2. Tune `max.poll.records` and `fetch.max.bytes` for batch efficiency
3. Add more partitions to the topic
4. Move heavy processing off the consumer thread

**Interview soundbite:** "Consumer lag is usually one of three things: too-slow per-message processing, not enough consumers in the group, or rebalance churn. I check `kafka-consumer-groups --describe` first, look at lag per partition (uneven distribution suggests a hot key or stuck consumer), then triage."

### Scenario 3: Adding a Broker & Partition Reassignment

**Capacity expansion.** Bring up a new broker and move existing partitions onto it.

**Steps:**

1. Add `kafka-4` to docker-compose.yml (broker ID 4, ports 9095/9994).
2. `docker compose up -d kafka-4`
3. Verify the new broker registers with the cluster.
4. Generate a reassignment plan that includes broker 4:

```bash
cat > /tmp/move.json << 'EOF'
{"version":1,"topics":[{"topic":"orders"}]}
EOF

kx kafka-reassign-partitions --bootstrap-server kafka-1:29092 \
  --topics-to-move-json-file /tmp/move.json \
  --broker-list "1,2,3,4" --generate
```

5. Save the proposed assignment, then execute:

```bash
kx kafka-reassign-partitions --bootstrap-server kafka-1:29092 \
  --reassignment-json-file /tmp/proposed.json --execute
```

6. Verify completion:

```bash
kx kafka-reassign-partitions --bootstrap-server kafka-1:29092 \
  --reassignment-json-file /tmp/proposed.json --verify
```

**Interview soundbite:** "Adding a broker is two steps: register it (just bring it up), then reassign partitions. In production I throttle reassignment with `--throttle 50000000` to avoid impacting production traffic. After completion, I run preferred leader election."

### Scenario 4: Rolling Restart

**The most common operational task.** Patching, config changes, version upgrades all need this.

**Rule:** One broker at a time. Wait for ISR to fully recover before touching the next.

**Steps:**

```bash
# Verify baseline
kx kafka-topics --bootstrap-server kafka-1:29092 --describe --topic orders

# Restart broker 1, wait for ISR recovery
docker compose restart kafka-1
watch -n 2 "docker exec -e KAFKA_OPTS='' kafka-2 kafka-topics \
  --bootstrap-server kafka-2:29093 --describe --topic orders | grep Isr"

# Once recovered: broker 2
docker compose restart kafka-2
# Wait for ISR
# Then broker 3

# Final: rebalance leadership
kx kafka-leader-election --bootstrap-server kafka-1:29092 \
  --election-type preferred --all-topic-partitions
```

**Interview soundbite:** "For rolling restarts I confirm full ISR first, restart one broker, watch ISR recover (could be seconds or minutes), then move to the next. Never overlap restarts — that destroys your replication safety margin. Finish with preferred leader election."

### Scenario 5: Topic Configuration Changes

**Storage and lifecycle management.**

**Retention:**

```bash
# Topic with 60-second retention
kx kafka-topics --bootstrap-server kafka-1:29092 \
  --create --topic events --partitions 3 --replication-factor 3 \
  --config retention.ms=60000

# View config
kx kafka-configs --bootstrap-server kafka-1:29092 \
  --entity-type topics --entity-name events --describe

# Change retention
kx kafka-configs --bootstrap-server kafka-1:29092 \
  --entity-type topics --entity-name events \
  --alter --add-config retention.ms=3600000
```

**Log compaction (state-store pattern):**

```bash
kx kafka-topics --bootstrap-server kafka-1:29092 \
  --create --topic user-state --partitions 3 --replication-factor 3 \
  --config cleanup.policy=compact

# Same key updates — compaction keeps only the latest
for i in {1..5}; do
  echo "user1:state-v$i" | kxi kafka-console-producer \
    --bootstrap-server kafka-1:29092 --topic user-state \
    --property "parse.key=true" --property "key.separator=:" 2>/dev/null
done
```

**Interview soundbite:** "I tune retention per topic by use case. Time-series events get short retention. State store topics use `cleanup.policy=compact` so only the latest value per key is kept. Disk usage is the biggest cost driver, so I monitor it per broker."

### Scenario 6: SASL Authentication & ACLs

**Production security.** Plan to spend a half-day on this.

**Tasks:**
1. Enable SASL/SCRAM-SHA-256 listener in docker-compose
2. Create users with `kafka-configs --add-config 'SCRAM-SHA-256=[password=...]'`
3. Set ACLs per principal: `kafka-acls --add --allow-principal User:alice --operation Read --topic orders --group alice-group`
4. Test that wrong principals are denied
5. Configure clients with SASL_PLAINTEXT and JAAS

**Interview soundbite:** "Every application gets its own service account with ACLs scoped to only its topics. I never let applications use the broker's bootstrap admin credentials."

### Scenario 7: GC Pauses & Heap Tuning

**JVM tuning.**

```bash
# Look at current GC behavior
docker compose logs kafka-1 2>&1 | grep -i "gc\|garbage\|memory" | head -20

# Lower heap deliberately to cause issues
# Edit docker-compose.yml: KAFKA_HEAP_OPTS to "-Xmx256M -Xms256M"
docker compose up -d kafka-1

# Send heavy load — broker may struggle
# Check logs for OutOfMemoryError or long GC pauses
```

**Interview soundbite:** "I size broker heap to 4-6GB in production. Larger than 8GB risks long GC pauses that cause ISR shrinks. I monitor `jvm_gc_pause_seconds` and alert on max pause > 1 second."

### Scenario 8: Disaster Recovery (MirrorMaker 2)

**Cross-cluster replication.** Advanced — defer until you've done everything else.

Stand up a second 3-broker cluster, configure MirrorMaker 2 to replicate `orders` from primary → DR, simulate primary failure, verify failover.

### Scenario 9: KRaft Mode (No ZooKeeper)

**Forward-looking.** Apache Kafka is moving away from ZooKeeper.

Build a separate docker-compose with KRaft-mode brokers (no ZooKeeper service). Compare metadata storage and controller behavior.

### Scenario 10: Performance Benchmarking

```bash
kx kafka-producer-perf-test \
  --topic perf-test \
  --num-records 100000 \
  --record-size 1024 \
  --throughput 10000 \
  --producer-props bootstrap.servers=kafka-1:29092 acks=all

kx kafka-consumer-perf-test \
  --bootstrap-server kafka-1:29092 \
  --topic perf-test \
  --messages 100000
```

Output gives records/sec, MB/sec, average and p99 latency.

---

## Troubleshooting

### Issue: "container kafka-X is not running"

**Cause:** Codespace was paused, containers stopped.

**Fix:**

```bash
docker compose start
docker compose ps
```

If a specific container shows `Exited (1)` (crashed), check why:

```bash
docker compose logs --tail 100 kafka-X
```

Then restart it:

```bash
docker compose start kafka-X
```

### Issue: Prometheus targets show DOWN with "no such host"

**Cause:** Prometheus has stale DNS for brokers that restarted out of order.

**Fix:**

```bash
docker compose restart prometheus
```

Wait 20 seconds, refresh Prometheus → Status → Targets. All should be UP.

**Production lesson:** Use service discovery (`kubernetes_sd_configs` or `dns_sd_configs`) instead of static targets to avoid this class of problem.

### Issue: `kafka-broker-api-versions` fails with "Address already in use"

**Cause:** Confluent Kafka images make CLI tools inherit `KAFKA_OPTS`, including the JMX agent. The CLI tries to start its own JMX exporter on a port the broker is already using.

**Fix:** Always clear `KAFKA_OPTS` for exec calls:

```bash
docker exec -e KAFKA_OPTS="" kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:29092
```

Or use the `kx` alias.

### Issue: ISR shows fewer than 3 brokers for every partition

**Cause:** A broker is missing or lagging.

**Diagnosis:**

```bash
docker compose ps -a            # see if a container exited
docker compose logs <broker>    # find why it crashed
```

Common root causes:
- OOM (heap too small for load)
- ZooKeeper session timeout (network issue or long GC pause)
- Disk full

### Issue: Grafana panels show "No data"

**Cause:** Either Prometheus isn't scraping (check Targets), the metric name doesn't match what's available, or the time range is wrong.

**Diagnosis:**
1. Open Prometheus → run the same query → does it return data?
2. If yes, Grafana datasource is misconfigured
3. If no, the metric name is wrong or no data exists for that time range

### Issue: Producer hangs / connection timeout

**Common causes:**
- Wrong bootstrap port (use 9092/9093/9094 from host, 29092/29093/29094 from inside containers)
- Broker advertised listeners don't match how the client connects (the dual-listener gotcha)
- Network ACL or firewall blocking port

### Issue: Producer transient error rate > 0 during normal operation

**Common cause:** A broker is slow or marginal. Check ISR. If a broker keeps falling out of ISR and rejoining, it's likely:
- GC pauses (tune heap)
- Disk I/O saturation (check `iostat`)
- Network saturation (check broker network metrics)

---

## Command Reference

### Topics

```bash
# List all topics
kx kafka-topics --bootstrap-server kafka-1:29092 --list

# Describe a topic (partitions, replicas, ISR, leaders)
kx kafka-topics --bootstrap-server kafka-1:29092 --describe --topic orders

# Create
kx kafka-topics --bootstrap-server kafka-1:29092 --create \
  --topic orders --partitions 6 --replication-factor 3

# Delete
kx kafka-topics --bootstrap-server kafka-1:29092 --delete --topic orders

# Alter partitions (can only increase, never decrease)
kx kafka-topics --bootstrap-server kafka-1:29092 \
  --alter --topic orders --partitions 12
```

### Topic Configs

```bash
# View
kx kafka-configs --bootstrap-server kafka-1:29092 \
  --entity-type topics --entity-name orders --describe

# Change
kx kafka-configs --bootstrap-server kafka-1:29092 \
  --entity-type topics --entity-name orders \
  --alter --add-config retention.ms=86400000

# Remove a config (revert to broker default)
kx kafka-configs --bootstrap-server kafka-1:29092 \
  --entity-type topics --entity-name orders \
  --alter --delete-config retention.ms
```

### Consumer Groups

```bash
# List
kx kafka-consumer-groups --bootstrap-server kafka-1:29092 --list

# Describe (lag, current offset, latest offset)
kx kafka-consumer-groups --bootstrap-server kafka-1:29092 \
  --describe --group <group-name>

# Reset offsets to beginning (consumer must be stopped)
kx kafka-consumer-groups --bootstrap-server kafka-1:29092 \
  --group <group-name> --reset-offsets --to-earliest \
  --topic orders --execute
```

### Partition Reassignment

```bash
# Generate plan
kx kafka-reassign-partitions --bootstrap-server kafka-1:29092 \
  --topics-to-move-json-file /tmp/topics.json \
  --broker-list "1,2,3,4" --generate

# Execute (with throttle)
kx kafka-reassign-partitions --bootstrap-server kafka-1:29092 \
  --reassignment-json-file /tmp/plan.json \
  --throttle 50000000 --execute

# Verify
kx kafka-reassign-partitions --bootstrap-server kafka-1:29092 \
  --reassignment-json-file /tmp/plan.json --verify
```

### Leader Election

```bash
# Force leader rebalance to preferred replicas
kx kafka-leader-election --bootstrap-server kafka-1:29092 \
  --election-type preferred --all-topic-partitions

# Force unclean leader election (data-loss risk — emergency only)
kx kafka-leader-election --bootstrap-server kafka-1:29092 \
  --election-type unclean --all-topic-partitions
```

### ACLs (after SASL setup)

```bash
# Add ACL
kx kafka-acls --bootstrap-server kafka-1:29092 \
  --add --allow-principal User:alice \
  --operation Read --topic orders --group alice-group

# List ACLs
kx kafka-acls --bootstrap-server kafka-1:29092 --list

# Remove ACL
kx kafka-acls --bootstrap-server kafka-1:29092 \
  --remove --allow-principal User:alice \
  --operation Read --topic orders
```

### Inspecting Offsets

```bash
# Show latest offset per partition
kx kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka-1:29092 --topic orders

# Show earliest offset per partition
kx kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka-1:29092 --topic orders --time -2
```

---

## Key Metrics for Admins

These are the metrics worth memorizing for interviews:

| Metric | Healthy Value | Bad Means |
|---|---|---|
| `kafka_server_replicamanager_underreplicatedpartitions` | 0 | Broker failing or replica falling behind |
| `kafka_controller_kafkacontroller_offlinepartitionscount` | 0 | Data loss risk |
| `kafka_controller_kafkacontroller_activecontrollercount` | 1 | 0 = no controller, 2+ = split-brain |
| `kafka_server_brokertopicmetrics_messagesinpersec` | depends on workload | Sudden drop = producer or broker issue |
| `kafka_network_requestmetrics_requesthandleravgidlepercent` | > 0.3 | Broker overloaded |
| Consumer lag per group | depends on workload | Consumer can't keep up |
| Disk usage % | < 80% | Risk of broker stop |
| JVM GC pause max | < 1s | Long pauses cause ISR shrinks |

**Useful PromQL queries:**

```promql
# Total under-replicated partitions across cluster
sum(kafka_server_replicamanager_underreplicatedpartitions)

# Active controller (should always be exactly 1)
sum(kafka_controller_kafkacontroller_activecontrollercount)

# Messages per second across all brokers
sum(rate(kafka_server_brokertopicmetrics_messagesin_total[1m]))

# Bytes in per second per broker
sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesin_total[1m]))

# Brokers up
count(up{job="kafka"} == 1)
```

---

## Interview-Ready Concepts

These are the concepts to internalize for talking to a Kafka admin hiring manager:

**Replication and durability:**
- `replication.factor` is per-topic, set at creation
- `min.insync.replicas` controls write durability — producers with `acks=all` block if ISR drops below this
- Setting `unclean.leader.election.enable=false` prevents data loss but trades availability

**Partition keys and ordering:**
- Same key → same partition → ordered
- No key → round-robin distribution → no cross-topic ordering
- Choose keys carefully — high-volume keys create hot partitions

**Internal topics:**
- `__consumer_offsets` (50 partitions by default) — tracks consumer group positions
- `__transaction_state` — for transactional producers
- These are replicated 3x by default and contribute to URP counts when brokers fail

**Controller:**
- One broker is the active controller at any time
- Manages partition leadership, broker registration, topic deletion
- Election happens when the current controller fails or loses ZK session

**ZooKeeper vs KRaft:**
- ZooKeeper: external metadata store, being deprecated
- KRaft: internal Raft-based consensus, the future of Kafka

**Producer durability levels:**
- `acks=0`: fire and forget (fastest, least durable)
- `acks=1`: leader confirmation only
- `acks=all`: all in-sync replicas confirm (slowest, most durable)

**Consumer semantics:**
- At-most-once: commit before processing (can lose)
- At-least-once: commit after processing (can duplicate) — most common
- Exactly-once: requires transactions and idempotent producer

---

## Resources

**Documentation:**
- Apache Kafka: https://kafka.apache.org/documentation/
- Confluent Platform: https://docs.confluent.io/
- Kafka UI by Provectus: https://github.com/provectus/kafka-ui

**Free learning:**
- Confluent Developer (courses): https://developer.confluent.io/
- Stephane Maarek's Kafka YouTube series

**Books worth owning:**
- *Kafka: The Definitive Guide* (O'Reilly) — comprehensive
- *Designing Event-Driven Systems* (O'Reilly) — architectural patterns

**Tools:**
- `kafkacat` / `kcat` — Swiss Army knife CLI
- `kafdrop` / `kafka-ui` — visual inspection
- Burrow — consumer lag monitoring at scale

---

## License

This is a personal learning project. Feel free to fork and adapt for your own Kafka practice.
# Kafka Lab
   
   Local Kafka practice environment.
