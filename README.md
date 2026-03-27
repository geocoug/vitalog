<p align="center">
  <img src="logo.svg" alt="Vitalog" width="400">
</p>

<p align="center">
  <a href="https://github.com/geocoug/vitalog/actions/workflows/ci-cd.yml"><img src="https://github.com/geocoug/vitalog/actions/workflows/ci-cd.yml/badge.svg" alt="ci/cd"></a>
  <a href="https://codecov.io/gh/geocoug/vitalog"><img src="https://codecov.io/gh/geocoug/vitalog/graph/badge.svg" alt="codecov"></a>
  <a href="https://pypi.org/project/vitalog/"><img src="https://img.shields.io/pypi/v/vitalog" alt="PyPI"></a>
  <a href="https://pypi.org/project/vitalog/"><img src="https://img.shields.io/pypi/pyversions/vitalog" alt="Python"></a>
  <a href="https://www.gnu.org/licenses/gpl-3.0"><img src="https://img.shields.io/badge/License-GPLv3-blue.svg" alt="License: GPL v3"></a>
</p>

A CLI tool for managing personal health data from [Apple Health](https://www.apple.com/health/) and [SleepCycle](https://www.sleepcycle.com/).
Load data into a local DuckDB database, generate AI-powered health narratives
with [Claude](https://www.anthropic.com/claude), and build interactive static HTML dashboards — no database server required.

## Features

- **`vitalog load`** — Extract Apple Health XML exports and SleepCycle CSV data into a local DuckDB database
- **`vitalog narrative`** — Generate AI-written health journal entries using Claude, with trend analysis and period-over-period comparisons
- **`vitalog dashboard`** — Build self-contained static HTML dashboards with D3.js charts, Leaflet workout route maps, dark mode, and interactive tooltips

## Installation

```bash
pip install vitalog
```

Or install from source with [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/geocoug/vitalog.git
cd vitalog
uv sync --all-extras
```

## Usage

### Load data

```bash
# Load an Apple Health export (ZIP from iPhone → Health → Export)
vitalog load apple --file data/export.zip

# Load SleepCycle app data (CSV export)
vitalog load sleep --file data/sleepdata.csv

# Load both at once
vitalog load all --apple data/export.zip --sleep data/sleepdata.csv

# Apple Health only (SleepCycle is optional)
vitalog load all --apple data/export.zip
```

Both commands write to `vitalog.duckdb` in the current directory by default. Override with `--db path/to/file.duckdb`.

### Generate a health narrative

```bash
# Last week summary
vitalog narrative --period last-week

# Custom date range, saved to file
vitalog narrative --start 2025-01-01 --end 2025-03-31 --output q1-narrative.md

# Ask a specific question about your health data
vitalog narrative --question "How has my sleep quality changed?"

# Question with a specific time period
vitalog narrative --period last-month -q "Am I exercising enough?"
```

### Set your profile (optional)

```bash
# Provide demographics for personalized health context in narratives
vitalog profile --age 35 --weight 175 --height 70 --sex male

# View current profile
vitalog profile --show
```

Demographics are stored in the DuckDB database and used to contextualize metrics in narratives
(e.g., age-appropriate resting HR norms, BMI, VO2Max percentiles).

### Generate a health narrative

Requires `ANTHROPIC_API_KEY` set in your environment or `.env` file.
Optionally set `ANTHROPIC_MODEL` to choose a specific Claude model (defaults to `claude-sonnet-4-20250514`).

### Build a dashboard

```bash
# Last year dashboard (opens in browser)
vitalog dashboard --period last-year

# Custom range, no auto-open
vitalog dashboard --start 2024-01-01 --end 2024-12-31 --output 2024.html --no-open
```

Generates a self-contained HTML file with 20+ interactive charts across 6 tabs:
Overview, Vitals, Sleep, Training, Activity, and Routes.

## Data Sources

### [Apple Health](https://www.apple.com/health/) Export

**How to export:** iPhone → Health app → profile icon → Export All Health Data → save ZIP file.

**Expected format:** A ZIP archive containing:

- `apple_health_export/export.xml` — All health records, workouts, and activity summaries
- `apple_health_export/workout-routes/*.gpx` — GPS tracks for outdoor workouts

**What gets extracted:**

| Category | Metrics |
|----------|---------|
| Activity | Steps, flights climbed, walking speed, distance |
| Heart | Heart rate, resting HR, HRV, VO2Max |
| Body | SpO2, respiratory rate |
| Workouts | Type, duration, distance, running pace, power, cycling stats |
| Sleep | Apple Health sleep analysis (duration) |
| Activity Rings | Move, Exercise, Stand — values and goals |
| Routes | GPS trackpoints with elevation, speed, accuracy |

### [SleepCycle](https://sleepcycle.com/) CSV

**How to export:** SleepCycle app → Profile → More → Database → Export Database → save CSV file.

**Expected format:** Semicolon-delimited CSV (`;`) with headers including:

```
Start;End;Sleep Quality;Regularity;Deep (seconds);Light (seconds);Dream (seconds);
Awake (seconds);Heart rate (bpm);Steps;...
```

**What gets extracted:** Sleep duration, quality percentage, stage breakdown (deep/light/REM/awake), heart rate, environmental data (air pressure, temperature, weather), and snore time.

## What Gets Produced

### DuckDB Database

A local `vitalog.duckdb` file with staging tables and analytical views:

| Table | Contents |
|-------|----------|
| `stg_records` | All Apple Health records (steps, HR, VO2Max, etc.) with a `record_type` discriminator |
| `stg_workouts` | Workout sessions (type, duration, distance) |
| `stg_activity_summary` | Daily activity ring data (move, exercise, stand) |
| `stg_workout_routes` | GPS trackpoints from workout GPX files |
| `stg_sleep_cycle` | SleepCycle app data |

Plus analytical views: `daily_steps`, `daily_heart_rate`, `daily_resting_hr`, `daily_summary`, `workout_summary`, `sleep_combined`.

### Dashboard

A single self-contained HTML file (~700KB) with:

- 6 tabbed sections with 20+ interactive charts built with [D3.js](https://d3js.org/)
- Summary cards with sparkline trends
- Personal records section
- Monthly summary table with conditional formatting
- Workout route maps with GPS tracks built with [Leaflet](https://leafletjs.com/)
- Dark/light mode toggle
- Print-friendly CSS

### Narrative

A markdown document with a 3-5 paragraph AI-generated health journal entry covering steps, heart rate, sleep, workouts, and activity rings — with trend analysis vs. the prior equivalent period.

## Development

```bash
# Clone and install
git clone https://github.com/geocoug/vitalog.git
cd vitalog
uv sync --all-extras

# Run tests
just test

# Lint + format
just lint

# Run pre-commit hooks
just pre-commit

# See all available recipes
just
```

## License

[GNU General Public License v3.0](LICENSE)
