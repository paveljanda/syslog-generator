# syslog-generator

Stress test tool that sends syslog messages toward a specified endpoint at a configurable rate over TCP or UDP.

## Install

```bash
go build -o syslog-generator .
```

## Configuration

Create a `config.yaml` file:

```yaml
endpoint: localhost:1514
protocol: tcp
framing: newline
messages_per_second: 10
workers: 1
duration: 60s
max_messages: 0
messages:
  - "<34>1 2026-06-16T10:15:03.789Z appserver01 nginx 1234 - - 192.168.1.50 - - [16/Jun/2026:10:15:03 +0000] \"GET /api/v1/health HTTP/1.1\" 200 2 \"-\" \"curl/8.7\""
  - "<34>1 2026-06-16T10:15:04.012Z appserver01 nginx 1235 - - 192.168.1.51 - - [16/Jun/2026:10:15:04 +0000] \"POST /api/v1/pipelines HTTP/1.1\" 201 156 \"-\" \"Logproxy-Client/1.0\""
  - "<132>1 2026-06-16T10:15:05.003Z lb01 caddy 5678 - - {\"ts\":1718538905.003,\"logger\":\"http.log.access\",\"msg\":\"handled request\",\"request\":{\"method\":\"GET\",\"uri\":\"/dashboard\"},\"status\":200}"
  # ... add more messages as needed
```

### Fields

| Field | Description |
|---|---|
| `endpoint` | Target host:port to send syslog messages to |
| `protocol` | `tcp` or `udp` |
| `framing` | Message framing: `none` (raw), `newline` (appends `\n`), or `octet-counting` (prefixes `<len> ` per RFC 6587) |
| `messages_per_second` | Target send rate. Split evenly across workers |
| `workers` | Number of concurrent goroutines sending messages |
| `duration` | How long to run before stopping (`0s` = run until Ctrl+C). Accepts Go duration strings (`30s`, `5m`, `1h`) |
| `max_messages` | Stop after sending this many messages (`0` = unlimited). Used together with `duration` — whichever limit is hit first wins |
| `messages` | Array of syslog messages. One is chosen at random each send. The timestamp is replaced with the current time |

## Usage

```bash
./syslog-generator
```

### CLI Flags

All config values can be overridden from the command line:

```bash
./syslog-generator --config config.yaml
./syslog-generator --endpoint 192.168.1.10:514 --protocol udp --rate 1000
./syslog-generator --duration 5m --workers 8
./syslog-generator --max-messages 50000 --rate 500 --framing octet-counting
```

| Flag | Default | Description |
|---|---|---|
| `--config` | `config.yaml` | Path to config file |
| `--endpoint` | (from config) | Override target endpoint |
| `--protocol` | (from config) | Override protocol (`tcp`/`udp`) |
| `--framing` | (from config) | Override framing (`none`/`newline`/`octet-counting`) |
| `--rate` | (from config) | Override messages per second |
| `--workers` | (from config) | Override number of workers |
| `--duration` | (from config) | Override duration |
| `--max-messages` | (from config) | Override max messages |

## Output

### Periodic Stats

Every 10 seconds the app prints a status line:

```
[stats] sent=1.2k errors=3 bytes=85.3KiB
```

Values are human-readable: `1.5k`, `2.3M`, `1.1B` for counts; `512B`, `1.2MiB`, `3.5GiB` for bytes.

### Final Summary

On exit (Ctrl+C, duration reached, or max messages hit), a summary is printed:

```
--- summary ---
  messages sent:  12.0k
  errors:         3
  bytes sent:     852.3KiB
  protocol:       tcp
  framing:        newline
  endpoint:       localhost:1514
  workers:        4
```

## Timestamp Handling

Each time a message is sent, the syslog timestamp in the format `<PRI>VERSION TIMESTAMP ...` is replaced with the current UTC time in RFC 3339 Nano format. This ensures every message appears fresh.

## Stop Conditions

The generator stops when **any** of these is reached:

- **Ctrl+C** (SIGINT)
- **Duration** expires (if `duration` > 0)
- **Max messages** sent (if `max_messages` > 0)

If neither `duration` nor `max_messages` is set, it runs until interrupted.