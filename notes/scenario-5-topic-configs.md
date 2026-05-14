# Scenario 5 — Topic Configuration Changes

- Updated orders topic retention dynamically with retention.ms=3600000.
- Created user-state topic with cleanup.policy=compact.
- Learned that retention deletes old messages by time, while compaction keeps the latest value per key.
- Compaction is asynchronous, so older key versions may still appear until the log cleaner runs.
