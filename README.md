# 🦀 Proton Extractor

> **Track your time without the spreadsheet. Extract hours from Proton Calendar exports.**

CLI tool that parses Proton Calendar `.ics` exports and generates clean, readable time reports — grouped by month, person, and event type.

![Rust](https://img.shields.io/badge/Rust-1.70+-orange.svg)
![macOS](https://img.shields.io/badge/macOS-native-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## ⚡ What It Does

```
$ proton-extractor ~/Downloads/calendar-2026-*.ics

--- February 2026 ---
  2h 30m  Childcare [Lulu]
  2h 30m  Childcare [Jeremy]
  4h      Meeting & Signature [Client]
  ------
  9h      Lulu
  9h      Jeremy
  18h     TOTAL

=== Grand Total: 47h ===
=== Hours per person ===
  22h 30m  Lulu
  24h 30m  Jeremy
```

No exports. No spreadsheets. No manual counting.

## 🚀 Quick Start

```bash
# Install
cargo install --git https://github.com/JeremySomsouk/proton-extractor

# Run on your Proton Calendar exports
proton-extractor ~/Downloads/calendar.ics

# Filter to current month
proton-extractor ~/Downloads/*.ics -m current

# Previous month only
proton-extractor ~/Downloads/*.ics -m previous
```

## 💡 Features

### 📅 Smart Parsing
- Parses standard `.ics` files from Proton Calendar exports
- Extracts **person names** from `[PersonName]` in event titles
- Handles **recurring events** (RRULE) with 5-year look-ahead
- Respects **exclusion dates** (EXDATE)
- Skips zero/negative duration events automatically

### 📊 Time Reports
- **Per-month breakdown** with event details
- **Per-person totals** — see who's working on what
- **Grand totals** across all files
- **Multiple files** supported — glob patterns work

### 🎯 Use Cases

```
# Freelancers: Track client time
proton-extractor client-calendar.ics

# Babysitter: Log hours for parents
proton-extractor babysitter-calendar.ics -m previous

# Families: Split childcare between parents
proton-extractor ~/Downloads/*-calendar.ics
```

## ⚙️ Usage

| Flag | Values | Description |
|------|--------|-------------|
| `-d, --date` | `all`, `current`, `previous`, `today` | Filter by date period |
| `-f, --format` | `text`, `json`, `csv`, `markdown`, `ical` | Output format |
| `--exclude-person` | name | Exclude events by person |
| `-p, --person` | name | Filter by person |
| `--from` | YYYY-MM-DD | Start date |
| `--to` | YYYY-MM-DD | End date |
| `--list-persons` | | List all unique persons found in events |

### Event Format

Proton Extractor looks for person names in brackets:

```
✅ "Meeting with [Alice] at 2pm"     → Person: Alice
✅ "Childcare [Jeremy]"               → Person: Jeremy  
✅ "Stand-up"                        → Person: (none)
```

**Tip:** Use Proton Calendar's description field to tag events with person names in brackets.

## 🛠️ Installation

### From Source
```bash
git clone git@github.com:JeremySomsouk/proton-extractor.git
cd proton-extractor
cargo install --path .
```

### Requirements
- Rust 1.70+
- Proton Calendar export (`.ics` file)

## 📝 Example Output

```
$ proton-extractor babysitter-feb.ics -m february

--- February 2026 ---
  4h 00m  BabySitting [Emma]
  3h 00m  BabySitting [Lucas]
  2h 30m  BabySitting [Emma]
  3h 30m  BabySitting [Lucas]
  ------
  7h 30m  Emma
  6h 30m  Lucas
  ------
  14h 00m  TOTAL

=== Grand Total: 14h ===
```

Event titles with `[Name]` format extract person hours automatically.

## 🤝 Contributing

Open issues or PRs welcome.

## License

MIT
