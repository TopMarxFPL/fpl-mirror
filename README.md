# FPL

Fantasy Premier League data, fetched directly from the official FPL API.

Data is available as raw JSON (exactly as returned by the API) and as
processed CSVs for easy use in spreadsheets, notebooks, and analysis tools.

---

## What's included

### JSON (raw API data)

| File | Description |
|---|---|
| `data/{season}/fpl-bootstrap_{season}.json` | Players, teams, gameweeks, game settings |
| `data/{season}/fpl-fixtures_{season}.json` | All season fixtures with scores |
| `data/{season}/players/{fpl_id}_{first}_{second}_{opta_id}.json` | Per-player match history |
| `data/{season}/gameweeks/gw{N}/live.json` | Live points and stats per GW |
| `data/{season}/gameweeks/gw{N}/dream-team.json` | GW dream team |
| `data/{season}/fpl-dream-team_{season}.json` | Season dream team (updated each GW) |
| `data/{season}/fpl-set-piece-notes_{season}.json` | Set piece taker notes (updated each GW) |
| `data/{season}/fpl-regions_{season}.json` | FPL region/nationality reference |

### CSV (processed)

| File | Description |
|---|---|
| `data/{season}/csv/players.csv` | All players with season stats and metadata |
| `data/{season}/csv/teams.csv` | All 20 Premier League teams |
| `data/{season}/csv/fixtures.csv` | All fixtures with scores and difficulty ratings |
| `data/{season}/csv/players/history/{fpl_id}_{first}_{second}_{opta_id}.csv` | Per-player match history |
| `data/{season}/csv/players/history_past/{fpl_id}_{first}_{second}_{opta_id}.csv` | Player season histories |
| `data/{season}/csv/gameweeks.csv` | Gameweek summary data |
| `data/{season}/csv/live.csv` | Per-player points and stats for each GW |
| `data/{season}/csv/dream-teams.csv` | GW and season dream teams |
| `data/{season}/csv/set-piece-notes.csv` | Set piece taker notes per team |
| `data/{season}/csv/regions.csv` | Region/nationality reference |

### Latest (always current season)

The `latest/` directory always reflects the most recently fetched data,
without needing to know the current season year:

| File | Description |
|---|---|
| `latest/fpl-bootstrap.json` | Current bootstrap |
| `latest/fpl-fixtures.json` | Current fixtures |
| `latest/players-{team_opta_id}.json` | Per-team player histories (one file per team) |
| `latest/fetch-manifest.json` | Fetch metadata and completion status |
| `latest/players-history-past.json` | Previous-season summaries per player (regenerated on GW closure) |

---

## Update schedule

The repository updates automatically via GitHub Actions:

- **Daily at 1am UTC** — fetches bootstrap, fixtures, and player data for any
  teams that played the previous day
- **On gameweek closure** — full fetch of all players once a gameweek is
  confirmed complete (`finished` and `data_checked` both true in the bootstrap),
  plus live points, dream team, season dream team, set-piece notes, and regions

On most days with no matches, only bootstrap and fixtures are refreshed.
Player element-summary files are only fetched when needed, keeping API
usage polite and minimal.

If match data is still being processed at 1am, retries run automatically at 2am
and 3am UTC.

---

## Player files

Each player has their own JSON file named:

```
{fpl_id}_{first_name}_{second_name}_{opta_id}.json
```

The file contains the player's match-by-match history for the current season
(`history`) and season summaries for previous seasons (`history_past`), exactly
as returned by the FPL API `element-summary/{fpl_id}/` endpoint.

Player files are organised by season:

```
data/2025/players/
  442_Mohamed_Salah_56322.json
  328_Erling_Haaland_223094.json
  ...
```

---

## Season format

Seasons are identified by their start year. The 2025/26 season is `2025`.

Historical data from before this repository was created is available at
[vaastav/Fantasy-Premier-League](https://github.com/vaastav/Fantasy-Premier-League).

---

## Fetch manifest

Each season directory contains a `fetch-manifest.json` recording the state
of the most recent fetch:

```json
{
  "season": 2025,
  "last_run": "2026-04-11T01:15:32Z",
  "current_gw": 32,
  "fetch_type": "active_gw",
  "expected_count": 60,
  "fetched_count": 60,
  "failed_ids": [],
  "completed": true,
  "last_closed_gw": 31
}
```

`fetch_type` values:

| Value | Meaning |
|---|---|
| `active_gw` | Fetched players for teams that played yesterday |
| `gw_closure` | Full fetch of all players on GW closure |
| `forced` | Manual full fetch via workflow dispatch |
| `none` | No matches yesterday, nothing to fetch |
| `waiting` | GW finished but data not yet checked |
| `blocked` | Match data still processing, retrying next run |

---

## API endpoints covered

All data is sourced from the public FPL API at
`https://fantasy.premierleague.com/api/`:

| Endpoint | Data |
|---|---|
| `bootstrap-static/` | Players, teams, gameweeks |
| `fixtures/` | All season fixtures |
| `element-summary/{id}/` | Per-player match history |
| `event/{gw}/live/` | Live GW points and stats |
| `dream-team/{gw}/` | GW dream team |
| `dream-team/` | Season dream team |
| `team/set-piece-notes/` | Set piece taker notes |
| `regions/` | Nationality/region reference |

Manager, league, and authenticated endpoints are not included.

---

## Using the data

### Python

```python
import json
import urllib.request

# Latest bootstrap
url = "https://raw.githubusercontent.com/TopMarx/fpl/main/latest/fpl-bootstrap.json"
with urllib.request.urlopen(url) as r:
    bootstrap = json.loads(r.read())

players = bootstrap["elements"]
teams = bootstrap["teams"]
```

### Direct download

Raw files are available at:
```
https://raw.githubusercontent.com/TopMarx/fpl/main/data/{season}/fpl-bootstrap_{season}.json
https://raw.githubusercontent.com/TopMarx/fpl/main/latest/fpl-bootstrap.json
```

### Clone the repo

```bash
git clone https://github.com/TopMarx/fpl.git
```

For the latest data only (faster):

```bash
git clone --depth 1 https://github.com/TopMarx/fpl.git
```

---

## Repo structure

```
├── data/
│   └── {season}/
│       ├── fpl-bootstrap_{season}.json
│       ├── fpl-fixtures_{season}.json
│       ├── fpl-dream-team_{season}.json
│       ├── fpl-set-piece-notes_{season}.json
│       ├── fpl-regions_{season}.json
│       ├── fetch-manifest.json
│       ├── players/
│       │   └── {fpl_id}_{first}_{second}_{opta_id}.json
│       ├── gameweeks/
│       │   └── gw{N}/
│       │       ├── live.json
│       │       └── dream-team.json
│       └── csv/
│           ├── players.csv
│           ├── teams.csv
│           ├── fixtures.csv
│           ├── gameweeks.csv
│           ├── live.csv
│           ├── dream-teams.csv
│           ├── set-piece-notes.csv
│           ├── regions.csv
│           └── players/
│               ├── history/
│               │   └── {fpl_id}_{first}_{second}_{opta_id}.csv
│               └── history_past/
│                   └── {fpl_id}_{first}_{second}_{opta_id}.csv
│
├── latest/
│   ├── fpl-bootstrap.json
│   ├── fpl-fixtures.json
│   ├── players-{team_opta_id}.json
│   ├── players-history-past.json
│   └── fetch-manifest.json
│
├── scripts/
│   ├── fetch.py
│   ├── generate_csv.py
│   └── generate_latest.py
│
├── requirements.txt
└── .github/
    └── workflows/
        └── fpl-fetch.yml
```

---

## A note on Opta data

Some fields available via the FPL API — including expected goals (xG),
expected assists (xA), and related metrics — are sourced from Opta and
subject to Stats Perform's commercial licensing terms.

This repository excludes these fields entirely. They are stripped from
all JSON files before being saved, and are not included in any of the
processed CSVs.

If you need xG and related data, Opta publish some metrics publicly via
[The Analyst](https://theanalyst.com).

---

## Contributing

Issues and pull requests are welcome. If you notice missing data, incorrect
values, or have suggestions for additional endpoints or CSV fields, please
open an issue.

---

## Disclaimer

This repository is an unofficial community resource and is not affiliated
with or endorsed by the Premier League or Fantasy Premier League. All data
is sourced from the publicly available FPL API.
