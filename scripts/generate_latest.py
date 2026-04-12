"""
FPL Latest Generator

Generates the latest/ directory from the current season's fetched data.
The latest/ directory provides a stable location for consumers who always
want the most recent data without knowing the current season year.

Reads from:
  data/{season}/fpl-bootstrap_{season}.json
  data/{season}/fpl-fixtures_{season}.json
  data/{season}/fetch-manifest.json
  data/{season}/players/{fpl_id}_{first}_{second}_{opta_id}.json

Writes to:
  latest/fpl-bootstrap.json
  latest/fpl-fixtures.json
  latest/fetch-manifest.json
  latest/players-{team_opta_id}.json   — one file per team, current season
                                          history only, all players on that team

Player file format:
  {
    "team_opta_id": 43,
    "team_name": "Arsenal",
    "generated_at": "2026-04-11T01:23:45Z",
    "players": [
      {
        "fpl_id": 123,
        "opta_id": 456789,
        "first_name": "Bukayo",
        "second_name": "Saka",
        "web_name": "Saka",
        "position": "MID",
        "history": [...]
      }
    ]
  }

Usage:
  python3 scripts/generate_latest.py --season 2025
  python3 scripts/generate_latest.py --season 2025 --data data/2025 --output latest
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


POSITION_MAP = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD", 5: "MGR"}


# ─── Helpers ──────────────────────────────────────────────────

def load_json(path: Path) -> dict | list | None:
    if not path.exists():
        print(f"  WARNING: {path} not found, skipping")
        return None
    with open(path) as f:
        return json.load(f)


def write_json(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ─── Main ─────────────────────────────────────────────────────

def run(args):
    data_dir = Path(args.data)
    latest_dir = Path(args.output)
    generated_at = datetime.now(timezone.utc).isoformat()

    print(f"FPL Latest Generator — Season {args.season}/{str(args.season + 1)[-2:]}")
    print(f"Source: {data_dir}")
    print(f"Output: {latest_dir}")
    print("=" * 60)

    # ── Load source data ──────────────────────────────────────
    bootstrap = load_json(data_dir / f"fpl-bootstrap_{args.season}.json")
    fixtures = load_json(data_dir / f"fpl-fixtures_{args.season}.json")
    manifest = load_json(data_dir / "fetch-manifest.json")

    if not isinstance(bootstrap, dict):
        print("FATAL: Bootstrap data not found or invalid. Run fetch.py first.")
        raise SystemExit(1)

    players_dir = data_dir / "players"
    if not players_dir.exists():
        print("FATAL: Players directory not found. Run fetch.py first.")
        raise SystemExit(1)

    latest_dir.mkdir(parents=True, exist_ok=True)

    # ── Build lookups from bootstrap ──────────────────────────
    # team fpl_id → {opta_id, name}
    team_lookup: dict[int, dict] = {}
    for t in bootstrap.get("teams", []):
        team_lookup[t["id"]] = {
            "opta_id": t["code"],
            "name": t["name"],
        }

    # element fpl_id → element dict
    element_lookup: dict[int, dict] = {}
    for el in bootstrap.get("elements", []):
        element_lookup[el["id"]] = el

    # ── Copy bootstrap ────────────────────────────────────────
    write_json(latest_dir / "fpl-bootstrap.json", bootstrap)
    print(f"\n  → fpl-bootstrap.json")

    # ── Copy fixtures ─────────────────────────────────────────
    if fixtures:
        write_json(latest_dir / "fpl-fixtures.json", fixtures)
        print(f"  → fpl-fixtures.json")

    # ── Copy manifest ─────────────────────────────────────────
    if manifest:
        write_json(latest_dir / "fetch-manifest.json", manifest)
        print(f"  → fetch-manifest.json")

    # ── Build per-team player files ───────────────────────────
    print(f"\n  Building per-team player files from {players_dir}/...")

    # Group player files by team opta_id
    team_players: dict[int, list[dict]] = {}

    player_files = list(players_dir.glob("*.json"))
    print(f"  Found {len(player_files)} player file(s)")

    for player_file in player_files:
        # Parse fpl_id from filename: {fpl_id}_{first}_{second}_{opta_id}.json
        parts = player_file.stem.split("_")
        if len(parts) < 2:
            print(f"  SKIP: unexpected filename {player_file.name}")
            continue

        try:
            fpl_id = int(parts[0])
        except ValueError:
            print(f"  SKIP: cannot parse FPL ID from {player_file.name}")
            continue

        el = element_lookup.get(fpl_id)
        if not el:
            print(f"  SKIP: FPL ID {fpl_id} not found in bootstrap")
            continue

        team_fpl_id = el.get("team")
        if not isinstance(team_fpl_id, int):
            print(f"  SKIP: FPL ID {fpl_id} has no valid team ID")
            continue
        team_info = team_lookup.get(team_fpl_id)
        if not team_info:
            print(f"  SKIP: team FPL ID {team_fpl_id} not found in bootstrap")
            continue

        team_opta_id = team_info["opta_id"]

        # Load player file and extract current season history only
        player_data = load_json(player_file)
        if not isinstance(player_data, dict):
            continue

        player_entry = {
            "fpl_id": fpl_id,
            "opta_id": el.get("code"),
            "first_name": el.get("first_name"),
            "second_name": el.get("second_name"),
            "web_name": el.get("web_name"),
            "known_name": el.get("known_name") or "",
            "position": POSITION_MAP.get(el.get("element_type", 0), ""),
            "history": player_data.get("history", []),
        }

        if team_opta_id not in team_players:
            team_players[team_opta_id] = []
        team_players[team_opta_id].append(player_entry)

    # Write one file per team
    for team_opta_id, players in sorted(team_players.items()):
        team_fpl_id = next(
            (fid for fid, info in team_lookup.items() if info["opta_id"] == team_opta_id),
            None
        )
        team_name = team_lookup.get(team_fpl_id, {}).get("name", "") if team_fpl_id else ""

        output_data = {
            "team_opta_id": team_opta_id,
            "team_name": team_name,
            "season": args.season,
            "generated_at": generated_at,
            "players": sorted(players, key=lambda p: p["second_name"]),
        }

        filename = f"players-{team_opta_id}.json"
        write_json(latest_dir / filename, output_data)
        print(f"  → {filename} ({len(players)} players)")

    print(f"\n  {len(team_players)} team file(s) written to {latest_dir}/")
    print("\nDone!")


def main():
    parser = argparse.ArgumentParser(
        description="Generate latest/ directory from current season FPL data"
    )
    parser.add_argument("--season", required=True, type=int,
                        help="Season start year (e.g. 2025 for 2025/26)")
    parser.add_argument("--data", default=None,
                        help="Source data directory (default: data/{season})")
    parser.add_argument("--output", default="latest",
                        help="Output directory (default: latest)")
    args = parser.parse_args()

    args.data = args.data or f"data/{args.season}"
    run(args)


if __name__ == "__main__":
    main()
