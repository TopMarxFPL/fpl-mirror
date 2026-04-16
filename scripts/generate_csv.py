"""
FPL CSV Generator

Generates CSV files from fetched FPL API JSON data.

Reads from:
  data/{season}/fpl-bootstrap_{season}.json
  data/{season}/fpl-fixtures_{season}.json
  data/{season}/gameweeks/gw{N}/live.json
  data/{season}/gameweeks/gw{N}/dream-team.json
  data/{season}/fpl-dream-team_{season}.json
  data/{season}/fpl-set-piece-notes_{season}.json
  data/{season}/fpl-regions_{season}.json

Writes to:
  data/{season}/csv/players.csv
  data/{season}/csv/teams.csv
  data/{season}/csv/fixtures.csv
  data/{season}/csv/gameweeks.csv
  data/{season}/csv/live.csv
  data/{season}/csv/dream-teams.csv
  data/{season}/csv/set-piece-notes.csv
  data/{season}/csv/regions.csv
  data/{season}/csv/players/history/{fpl_id}_{first}_{second}_{opta_id}.csv
  data/{season}/csv/players/history_past/{fpl_id}_{first}_{second}_{opta_id}.csv

Usage:
  python3 scripts/generate_csv.py --season 2025
  python3 scripts/generate_csv.py --season 2025 --output data/2025
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


# ─── Helpers ──────────────────────────────────────────────────

def load_json(path: Path) -> dict | list | None:
    if not path.exists():
        print(f"  WARNING: {path} not found, skipping")
        return None
    with open(path) as f:
        return json.load(f)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def player_display_name(el: dict) -> str:
    known = el.get("known_name", "")
    if known:
        return known
    first = el.get("first_name", "")
    second = el.get("second_name", "")
    return f"{first} {second}".strip()


# ─── Generators ───────────────────────────────────────────────

def generate_players(bootstrap: dict, team_lookup: dict, csv_dir: Path) -> None:
    """Generate players.csv from bootstrap elements."""
    fieldnames = [
        "fpl_id", "opta_id", "first_name", "second_name", "web_name", "known_name",
        "team_fpl_id", "team_opta_id", "team_name", "team_short_name", "element_type",
        "position", "status", "news", "total_points", "minutes", "goals_scored",
        "assists", "clean_sheets", "goals_conceded", "own_goals", "penalties_saved",
        "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus", 
        "bps", "influence", "creativity", "threat", "ict_index",
        "now_cost", "selected_by_percent",
    ]

    position_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD", 5: "MGR"}

    rows = []
    for el in bootstrap.get("elements", []):
        teamup = team_lookup.get(el.get("team"), {})
        rows.append({
            "fpl_id": el.get("id"),
            "opta_id": el.get("code"),
            "first_name": el.get("first_name"),
            "second_name": el.get("second_name"),
            "web_name": el.get("web_name"),
            "known_name": el.get("known_name") or "",
            "team_fpl_id": el.get("team"),
            "team_opta_id": el.get("team_code"),
            "team_name": teamup.get("name"),
            "team_short_name": teamup.get("short_name"),
            "element_type": el.get("element_type"),
            "position": position_map.get(el.get("element_type", 0), ""),
            "status": el.get("status"),
            "news": el.get("news") or "",
            "total_points": el.get("total_points", 0),
            "minutes": el.get("minutes", 0),
            "goals_scored": el.get("goals_scored", 0),
            "assists": el.get("assists", 0),
            "clean_sheets": el.get("clean_sheets", 0),
            "goals_conceded": el.get("goals_conceded", 0),
            "own_goals": el.get("own_goals", 0),
            "penalties_saved": el.get("penalties_saved", 0),
            "penalties_missed": el.get("penalties_missed", 0),
            "yellow_cards": el.get("yellow_cards", 0),
            "red_cards": el.get("red_cards", 0),
            "saves": el.get("saves", 0),
            "bonus": el.get("bonus", 0),
            "bps": el.get("bps", 0),
            "influence": el.get("influence", 0),
            "creativity": el.get("creativity", 0),
            "threat": el.get("threat", 0),
            "ict_index": el.get("ict_index", 0),
            "now_cost": el.get("now_cost"),
            "selected_by_percent": el.get("selected_by_percent"),
        })

    count = write_csv(csv_dir / "players.csv", rows, fieldnames)
    print(f"  → players.csv ({count} rows)")


def generate_teams(bootstrap: dict, csv_dir: Path) -> None:
    """Generate teams.csv from bootstrap teams."""
    fieldnames = [
        "fpl_id", "opta_id", "name", "short_name",
        "strength", "strength_overall_home", "strength_overall_away",
        "strength_attack_home", "strength_attack_away",
        "strength_defence_home", "strength_defence_away",
    ]

    rows = []
    for t in bootstrap.get("teams", []):
        rows.append({
            "fpl_id": t.get("id"),
            "opta_id": t.get("code"),
            "name": t.get("name"),
            "short_name": t.get("short_name"),
            "strength": t.get("strength"),
            "strength_overall_home": t.get("strength_overall_home"),
            "strength_overall_away": t.get("strength_overall_away"),
            "strength_attack_home": t.get("strength_attack_home"),
            "strength_attack_away": t.get("strength_attack_away"),
            "strength_defence_home": t.get("strength_defence_home"),
            "strength_defence_away": t.get("strength_defence_away"),
        })

    count = write_csv(csv_dir / "teams.csv", rows, fieldnames)
    print(f"  → teams.csv ({count} rows)")


def generate_gameweeks(bootstrap: dict, csv_dir: Path) -> None:
    """Generate gameweeks.csv from bootstrap events."""
    fieldnames = [
        "id", "name", "deadline_time",
        "average_entry_score", "highest_score",
        "finished", "data_checked",
        "is_previous", "is_current", "is_next",
    ]

    rows = []
    for ev in bootstrap.get("events", []):
        rows.append({
            "id": ev.get("id"),
            "name": ev.get("name"),
            "deadline_time": ev.get("deadline_time"),
            "average_entry_score": ev.get("average_entry_score"),
            "highest_score": ev.get("highest_score"),
            "finished": ev.get("finished"),
            "data_checked": ev.get("data_checked"),
            "is_previous": ev.get("is_previous"),
            "is_current": ev.get("is_current"),
            "is_next": ev.get("is_next"),
        })

    count = write_csv(csv_dir / "gameweeks.csv", rows, fieldnames)
    print(f"  → gameweeks.csv ({count} rows)")


def generate_fixtures(fixtures: list, team_lookup: dict, csv_dir: Path) -> None:
    """Generate fixtures.csv from fixtures data."""
    fieldnames = [
        "fpl_id", "opta_id", "gameweek",
        "team_h_fpl_id", "team_h_opta_id",
        "team_h_name", "team_h_short_name",
        "team_a_fpl_id", "team_a_opta_id",
        "team_a_name", "team_a_short_name",
        "team_h_score", "team_a_score",
        "kickoff_time", "finished", "started",
        "team_h_difficulty", "team_a_difficulty",
    ]

    rows = []
    for fix in fixtures:
        rows.append({
            "fpl_id": fix.get("id"),
            "opta_id": fix.get("code"),
            "gameweek": fix.get("event"),
            "team_h_fpl_id": fix.get("team_h"),
            "team_h_opta_id": team_lookup.get(fix.get("team_h"), {}).get("code"),
            "team_h_name": team_lookup.get(fix.get("team_h"), {}).get("name"),
            "team_h_short_name": team_lookup.get(fix.get("team_h"), {}).get("short_name"),
            "team_a_fpl_id": fix.get("team_a"),
            "team_a_opta_id": team_lookup.get(fix.get("team_a"), {}).get("code"),
            "team_a_name": team_lookup.get(fix.get("team_a"), {}).get("name"),
            "team_a_short_name": team_lookup.get(fix.get("team_a"), {}).get("short_name"),
            "team_h_score": fix.get("team_h_score"),
            "team_a_score": fix.get("team_a_score"),
            "kickoff_time": fix.get("kickoff_time"),
            "finished": fix.get("finished"),
            "started": fix.get("started"),
            "team_h_difficulty": fix.get("team_h_difficulty"),
            "team_a_difficulty": fix.get("team_a_difficulty"),
        })

    count = write_csv(csv_dir / "fixtures.csv", rows, fieldnames)
    print(f"  → fixtures.csv ({count} rows)")


def generate_live(gameweeks_dir: Path, csv_dir: Path) -> None:
    """Generate live.csv from gameweeks/gw{N}/live.json files."""
    fieldnames = [
        "gw", "fpl_id",
        "minutes", "starts", "goals_scored", "assists", "clean_sheets",
        "goals_conceded", "own_goals", "penalties_saved", "penalties_missed",
        "yellow_cards", "red_cards", "saves", "bonus", "bps",
        "influence", "creativity", "threat", "ict_index",
        "clearances_blocks_interceptions", "recoveries", "tackles",
        "defensive_contribution", "total_points", "in_dreamteam",
    ]

    rows = []
    for gw_dir in sorted(gameweeks_dir.glob("gw*")):
        live_path = gw_dir / "live.json"
        live = load_json(live_path)
        if not isinstance(live, dict):
            continue
        try:
            gw = int(gw_dir.name[2:])
        except ValueError:
            continue
        for el in live.get("elements", []):
            stats = el.get("stats", {})
            rows.append({
                "gw": gw,
                "fpl_id": el.get("id"),
                "minutes": stats.get("minutes", 0),
                "starts": stats.get("starts", 0),
                "goals_scored": stats.get("goals_scored", 0),
                "assists": stats.get("assists", 0),
                "clean_sheets": stats.get("clean_sheets", 0),
                "goals_conceded": stats.get("goals_conceded", 0),
                "own_goals": stats.get("own_goals", 0),
                "penalties_saved": stats.get("penalties_saved", 0),
                "penalties_missed": stats.get("penalties_missed", 0),
                "yellow_cards": stats.get("yellow_cards", 0),
                "red_cards": stats.get("red_cards", 0),
                "saves": stats.get("saves", 0),
                "bonus": stats.get("bonus", 0),
                "bps": stats.get("bps", 0),
                "influence": stats.get("influence", "0.0"),
                "creativity": stats.get("creativity", "0.0"),
                "threat": stats.get("threat", "0.0"),
                "ict_index": stats.get("ict_index", "0.0"),
                "clearances_blocks_interceptions": stats.get("clearances_blocks_interceptions", 0),
                "recoveries": stats.get("recoveries", 0),
                "tackles": stats.get("tackles", 0),
                "defensive_contribution": stats.get("defensive_contribution", 0),
                "total_points": stats.get("total_points", 0),
                "in_dreamteam": stats.get("in_dreamteam", False),
            })

    count = write_csv(csv_dir / "live.csv", rows, fieldnames)
    print(f"  → live.csv ({count} rows)")


def generate_dream_teams(
    gameweeks_dir: Path, season_dream_team: dict | None,
    element_lookup: dict, csv_dir: Path
) -> None:
    """Generate dream-teams.csv from per-GW and season dream team data."""
    fieldnames = [
        "type", "gw", "position", "fpl_id", "opta_id", "name",
        "player_position", "points", "is_top_player",
    ]

    position_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    rows = []

    def dream_team_rows(data: dict, type_: str, gw: int | str) -> list[dict]:
        top_player_id = data.get("top_player", {}).get("id")
        result = []
        for entry in data.get("team", []):
            fpl_id = entry.get("element")
            el = element_lookup.get(fpl_id, {})
            result.append({
                "type": type_,
                "gw": gw,
                "position": entry.get("position"),
                "fpl_id": fpl_id,
                "opta_id": el.get("code"),
                "name": player_display_name(el),
                "player_position": position_map.get(el.get("element_type", 0), ""),
                "points": entry.get("points"),
                "is_top_player": fpl_id == top_player_id,
            })
        return result

    for gw_dir in sorted(gameweeks_dir.glob("gw*")):
        dt_path = gw_dir / "dream-team.json"
        data = load_json(dt_path)
        if not isinstance(data, dict):
            continue
        try:
            gw = int(gw_dir.name[2:])
        except ValueError:
            continue
        rows.extend(dream_team_rows(data, "gw", gw))

    if season_dream_team:
        rows.extend(dream_team_rows(season_dream_team, "season", ""))

    count = write_csv(csv_dir / "dream-teams.csv", rows, fieldnames)
    print(f"  → dream-teams.csv ({count} rows)")


def generate_regions(regions_data: list, csv_dir: Path) -> None:
    """Generate regions.csv from regions data."""
    fieldnames = ["id", "name", "code", "iso_code_short", "iso_code_long"]

    rows = []
    for region in regions_data:
        rows.append({
            "id": region.get("id"),
            "name": region.get("name"),
            "code": region.get("code"),
            "iso_code_short": region.get("iso_code_short", ""),
            "iso_code_long": region.get("iso_code_long", ""),
        })

    count = write_csv(csv_dir / "regions.csv", rows, fieldnames)
    print(f"  → regions.csv ({count} rows)")


def generate_set_piece_notes(set_piece_data: dict, team_lookup: dict, csv_dir: Path) -> None:
    """Generate set-piece-notes.csv from set piece taker notes."""
    fieldnames = [
        "team_fpl_id", "team_opta_id", "team_name", "team_short_name",
        "note_index", "info_message", "source_link", "external_link", "last_updated",
    ]

    last_updated = set_piece_data.get("last_updated", "")

    rows = []
    for entry in set_piece_data.get("teams", []):
        team_fpl_id = entry.get("id")
        team = team_lookup.get(team_fpl_id, {})
        for i, note in enumerate(entry.get("notes", [])):
            rows.append({
                "team_fpl_id": team_fpl_id,
                "team_opta_id": team.get("code"),
                "team_name": team.get("name"),
                "team_short_name": team.get("short_name"),
                "note_index": i + 1,
                "info_message": note.get("info_message", ""),
                "source_link": note.get("source_link", ""),
                "external_link": note.get("external_link", False),
                "last_updated": last_updated,
            })

    count = write_csv(csv_dir / "set-piece-notes.csv", rows, fieldnames)
    print(f"  → set-piece-notes.csv ({count} rows)")


def generate_player_csvs(players_dir: Path, team_lookup: dict, csv_dir: Path) -> None:
    """Generate per-player history and history_past CSVs from player JSON files.

    Output:
      csv/players/history/{fpl_id}_{first}_{second}_{opta_id}.csv
      csv/players/history_past/{fpl_id}_{first}_{second}_{opta_id}.csv

    Filename is taken directly from the JSON stem so it always reflects the
    player's current name (fetch.py guarantees this). Before writing, stale
    CSVs are removed via {fpl_id}_*.csv glob — the same approach fetch.py
    uses for JSON files, ensuring renames and known_name changes stay in sync.

    history_past is skipped if the array is empty (new players with no prior
    FPL seasons).
    """
    player_files = sorted(players_dir.glob("*.json"))
    if not player_files:
        print("  SKIP: player CSVs (no player JSON files found)")
        return

    history_dir = csv_dir / "players" / "history"
    history_past_dir = csv_dir / "players" / "history_past"

    history_fieldnames = [
        "fpl_id", "fixture_fpl_id", "gameweek", "kickoff_time", "was_home",
        "opponent_team_fpl_id", "opponent_team_name",
        "team_h_score", "team_a_score", "modified",
        "minutes", "starts", "goals_scored", "assists", "clean_sheets",
        "goals_conceded", "own_goals", "penalties_saved", "penalties_missed",
        "yellow_cards", "red_cards", "saves", "bonus", "bps",
        "influence", "creativity", "threat", "ict_index",
        "clearances_blocks_interceptions", "recoveries", "tackles",
        "defensive_contribution", "total_points",
        "value", "selected", "transfers_in", "transfers_out", "transfers_balance",
    ]

    history_past_fieldnames = [
        "fpl_id", "opta_id", "season_name",
        "start_cost", "end_cost", "total_points", "minutes",
        "goals_scored", "assists", "clean_sheets", "goals_conceded",
        "own_goals", "penalties_saved", "penalties_missed",
        "yellow_cards", "red_cards", "saves", "bonus", "bps",
        "influence", "creativity", "threat", "ict_index",
        "clearances_blocks_interceptions", "recoveries", "tackles",
        "defensive_contribution","starts",
    ]

    history_count = 0
    history_past_count = 0

    for json_path in player_files:
        stem = json_path.stem  # e.g. "442_Mohamed_Salah_56322"
        fpl_id = int(stem.split("_")[0])
        csv_name = stem + ".csv"

        data = load_json(json_path)
        if not isinstance(data, dict):
            continue

        # Remove stale CSVs for this player before writing — mirrors the
        # glob-and-unlink pattern in fetch.py's fetch_player(), ensuring that
        # name changes (including known_name updates) don't leave orphaned files.
        for old_file in history_dir.glob(f"{fpl_id}_*.csv"):
            old_file.unlink()
        for old_file in history_past_dir.glob(f"{fpl_id}_*.csv"):
            old_file.unlink()

        # ── history (current season, one row per match) ───────
        history_rows = []
        for match in data.get("history", []):
            opponent_fpl_id = match.get("opponent_team")
            history_rows.append({
                "fpl_id": match.get("element"),
                "fixture_fpl_id": match.get("fixture"),
                "gameweek": match.get("round"),
                "kickoff_time": match.get("kickoff_time"),
                "was_home": match.get("was_home"),
                "opponent_team_fpl_id": opponent_fpl_id,
                "opponent_team_name": team_lookup.get(opponent_fpl_id, {}).get("name"),
                "team_h_score": match.get("team_h_score"),
                "team_a_score": match.get("team_a_score"),
                "modified": match.get("modified"),
                "minutes": match.get("minutes", 0),
                "starts": match.get("starts", 0),
                "goals_scored": match.get("goals_scored", 0),
                "assists": match.get("assists", 0),
                "clean_sheets": match.get("clean_sheets", 0),
                "goals_conceded": match.get("goals_conceded", 0),
                "own_goals": match.get("own_goals", 0),
                "penalties_saved": match.get("penalties_saved", 0),
                "penalties_missed": match.get("penalties_missed", 0),
                "yellow_cards": match.get("yellow_cards", 0),
                "red_cards": match.get("red_cards", 0),
                "saves": match.get("saves", 0),
                "bonus": match.get("bonus", 0),
                "bps": match.get("bps", 0),
                "influence": match.get("influence", "0.0"),
                "creativity": match.get("creativity", "0.0"),
                "threat": match.get("threat", "0.0"),
                "ict_index": match.get("ict_index", "0.0"),
                "clearances_blocks_interceptions": match.get("clearances_blocks_interceptions", 0),
                "recoveries": match.get("recoveries", 0),
                "tackles": match.get("tackles", 0),
                "defensive_contribution": match.get("defensive_contribution", 0),
                "total_points": match.get("total_points", 0),
                "value": match.get("value"),
                "selected": match.get("selected"),
                "transfers_in": match.get("transfers_in", 0),
                "transfers_out": match.get("transfers_out", 0),
                "transfers_balance": match.get("transfers_balance", 0),
            })
        write_csv(history_dir / csv_name, history_rows, history_fieldnames)
        history_count += 1

        # ── history_past (one row per past season) ────────────
        history_past = data.get("history_past", [])
        if history_past:
            past_rows = []
            for season in history_past:
                past_rows.append({
                    "fpl_id": fpl_id,
                    "opta_id": season.get("element_code"),
                    "season_name": season.get("season_name"),
                    "start_cost": season.get("start_cost"),
                    "end_cost": season.get("end_cost"),
                    "total_points": season.get("total_points", 0),
                    "minutes": season.get("minutes", 0),
                    "goals_scored": season.get("goals_scored", 0),
                    "assists": season.get("assists", 0),
                    "clean_sheets": season.get("clean_sheets", 0),
                    "goals_conceded": season.get("goals_conceded", 0),
                    "own_goals": season.get("own_goals", 0),
                    "penalties_saved": season.get("penalties_saved", 0),
                    "penalties_missed": season.get("penalties_missed", 0),
                    "yellow_cards": season.get("yellow_cards", 0),
                    "red_cards": season.get("red_cards", 0),
                    "saves": season.get("saves", 0),
                    "bonus": season.get("bonus", 0),
                    "bps": season.get("bps", 0),
                    "influence": season.get("influence", "0.0"),
                    "creativity": season.get("creativity", "0.0"),
                    "threat": season.get("threat", "0.0"),
                    "ict_index": season.get("ict_index", "0.0"),
                    "clearances_blocks_interceptions": season.get("clearances_blocks_interceptions", 0),
                    "recoveries": season.get("recoveries", 0),
                    "tackles": season.get("tackles", 0),
                    "defensive_contribution": season.get("defensive_contribution", 0),
                    "starts": season.get("starts", 0),
                })
            write_csv(history_past_dir / csv_name, past_rows, history_past_fieldnames)
            history_past_count += 1

    print(f"  → players/history/ ({history_count} files)")
    print(f"  → players/history_past/ ({history_past_count} files)")


# ─── Main ─────────────────────────────────────────────────────

def run(args):
    output = Path(args.output)
    csv_dir = output / "csv"

    print(f"FPL CSV Generator — Season {args.season}/{str(args.season + 1)[-2:]}")
    print(f"Source: {output}")
    print(f"Output: {csv_dir}")
    print("=" * 60)

    bootstrap_path = output / f"fpl-bootstrap_{args.season}.json"
    fixtures_path = output / f"fpl-fixtures_{args.season}.json"

    bootstrap = load_json(bootstrap_path)
    fixtures = load_json(fixtures_path)

    if not isinstance(bootstrap, dict):
        print("FATAL: Bootstrap data not found or invalid. Run fetch.py first.")
        raise SystemExit(1)

    team_lookup = {t["id"]: t for t in bootstrap.get("teams", [])}
    element_lookup = {el["id"]: el for el in bootstrap.get("elements", [])}

    print("\nGenerating CSVs...")
    generate_players(bootstrap, team_lookup, csv_dir)
    generate_teams(bootstrap, csv_dir)
    generate_gameweeks(bootstrap, csv_dir)

    if not isinstance(fixtures, list):
        print("  SKIP: fixtures.csv (fixtures data not found or invalid)")
    else:
        generate_fixtures(fixtures, team_lookup, csv_dir)

    gameweeks_dir = output / "gameweeks"

    if gameweeks_dir.exists():
        generate_live(gameweeks_dir, csv_dir)
        season_dream_team = load_json(output / f"fpl-dream-team_{args.season}.json")
        if not isinstance(season_dream_team, dict):
            season_dream_team = None
        generate_dream_teams(gameweeks_dir, season_dream_team, element_lookup, csv_dir)
    else:
        print("  SKIP: live.csv and dream-teams.csv (no gameweeks directory)")

    players_dir = output / "players"
    if players_dir.exists():
        generate_player_csvs(players_dir, team_lookup, csv_dir)
    else:
        print("  SKIP: player CSVs (no players directory)")

    set_piece_path = output / f"fpl-set-piece-notes_{args.season}.json"
    set_piece_data = load_json(set_piece_path)
    if isinstance(set_piece_data, dict):
        generate_set_piece_notes(set_piece_data, team_lookup, csv_dir)
    else:
        print("  SKIP: set-piece-notes.csv (data not found)")

    regions_path = output / f"fpl-regions_{args.season}.json"
    regions_data = load_json(regions_path)
    if isinstance(regions_data, list):
        generate_regions(regions_data, csv_dir)
    else:
        print("  SKIP: regions.csv (data not found)")

    print("\nDone!")


def main():
    parser = argparse.ArgumentParser(description="Generate CSV files from FPL JSON data")
    parser.add_argument("--season", required=True, type=int,
                        help="Season start year (e.g. 2025 for 2025/26)")
    parser.add_argument("--output", default=None,
                        help="Data directory (default: data/{season})")
    args = parser.parse_args()

    args.output = args.output or f"data/{args.season}"
    run(args)


if __name__ == "__main__":
    main()
