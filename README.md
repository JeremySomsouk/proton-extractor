# proton-extractor

CLI tool to sum calendar event hours from Proton Calendar `.ics` exports.

Parses one or more ICS files, groups events by month, and displays per-event durations with monthly and grand totals.

## Install

```bash
cargo install --path .
```

## Usage

```bash
# Single file
proton-extractor calendar.ics

# Multiple files (e.g. all .ics in Downloads)
proton-extractor ~/Downloads/*.ics

# Filter to current month only
proton-extractor ~/Downloads/*.ics -m current

# Filter to previous month
proton-extractor ~/Downloads/*.ics -m previous
```

### Options

| Flag | Values | Default | Description |
|------|--------|---------|-------------|
| `-m, --month` | `all`, `current`, `previous` | `all` | Filter events by month |

## Example output

```
--- January 2026 ---
  2h      Garde X & Y [Z]
  2h      Garde X & Y [A]
  1h 30m  Rencontre enfants & signature
  ------
  5h 30m  TOTAL

--- February 2026 ---
  2h 30m  Garde X & Y [Z]
  2h 30m  Garde X & Y [A]
  ------
  5h      TOTAL

=== Grand Total: 10h 30m ===
```
