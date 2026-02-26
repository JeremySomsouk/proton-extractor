# proton-extractor

CLI tool to sum calendar event hours from Proton Calendar `.ics` exports.

Parses one or more ICS files, groups events by month, and displays per-event durations with monthly and grand totals. Supports recurring events with safety limits to prevent unbounded expansion.

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
  1h 30m  Meeting & signature [Person A]
  2h      Childcare [Person A]
  2h      Childcare [Person A]
  2h      Childcare [Person B]
  2h      Childcare [Person B]
  2h      Childcare [Person A]
  2h      Childcare [Person A]
  2h      Childcare [Person B]
  2h      Childcare [Person B]
  ------
  8h      Person B
  9h 30m  Person A
  17h 30m  TOTAL

--- February 2026 ---
  2h 30m  Childcare [Person A]
  2h 30m  Childcare [Person A]
  2h 30m  Childcare [Person B]
  2h 30m  Childcare [Person A]
  2h 30m  Childcare [Person A]
  4h      Childcare [Person B]
  2h 30m  Childcare [Person B]
  2h 30m  Childcare [Person A]
  2h 30m  Childcare [Person A]
  3h      Childcare [Person B]
  2h 30m  Childcare [Person B]
  ------
  14h 30m  Person B
  15h     Person A
  29h 30m  TOTAL

=== Grand Total: 47h ===

=== Hours per person ===
  22h 30m  Person B
  24h 30m  Person A
```

## Features

- **Recurring events**: Expands RRULE-based events (DAILY, WEEKLY) with a 5-year look-ahead limit
- **Person extraction**: Parses `[PersonName]` from event summaries
- **EXDATE handling**: Respects exclusion dates in recurring events
- **Event filtering**: Skips events with invalid (zero or negative) durations
- **Multiple files**: Process several ICS files at once, gracefully skips unreadable files
