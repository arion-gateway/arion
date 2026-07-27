# Timezone Configuration

Orion supports configuring a local timezone, which is primarily used to format timestamps correctly in access logs and other log entries. By default, Orion operates in UTC, but you can configure it to use any valid IANA timezone (e.g., `Asia/Shanghai`, `Europe/Rome`, `America/New_York`).

## Configuration

You can configure the local timezone in your Orion configuration file by adding a `timezone` section:

```yaml
timezone:
  local: "Asia/Shanghai"
```

* `local`: The IANA timezone string. If left empty or set explicitly to `"UTC"`, Orion will default to UTC.

## How It Works

Orion uses the `chrono-tz` crate to handle timezone parsing and offsets, but it optimizes the process to avoid computing timezone differences on every single log line. Here is how it works under the hood:

1. **Initialization**: At startup, the timezone is parsed. If it's valid, Orion determines whether the timezone observes Daylight Saving Time (DST) during the current year by checking all 12 months.
2. **Static Offset (Non-DST)**: If the configured timezone has a constant offset (it does not observe DST), Orion calculates the local offset once, caches it, and avoids spawning any background tasks.
3. **Dynamic Offset (DST)**: If the timezone does observe Daylight Saving Time, Orion spawns a lightweight `tokio` background task. This task wakes up every second to check if the offset has changed. When a transition occurs, it automatically updates the cached offset.
