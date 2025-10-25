import json
import os
import logging
from datetime import datetime
import fastf1
import time
from supabase import create_client, Client
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Hardcoded settings (no env vars needed for these)
FASTF1_CACHE_DIR = "/tmp/fastf1_cache"
FASTF1_DELAY_SECONDS = 2.0
TARGET_TABLE_DEFAULT = "f1_data2"

def _truthy_env(name: str) -> bool:
    val = os.environ.get(name, "").strip().lower()
    return val in ("1", "true", "yes", "y", "on")


def _get_target_table(default: str = "f1data2") -> str:
    # Hardcoded table name; ignore env overrides
    return TARGET_TABLE_DEFAULT



def connect_to_supabase():
    """
    Create a Supabase client from Lambda environment variables.
    Requires SUPABASE_URL and SUPABASE_KEY to be set in the Lambda configuration.
    """
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY environment variables.")
    supabase: Client = create_client(url, key)
    return supabase

def check_if_new_data(supabase: Client):
    """Fetch the most recent saved year and round from Supabase."""
    try:
        data = (
            supabase
            .table('f1_entries')
            .select("roundnumber, year")
            .order("year", desc=True)
            .order("roundnumber", desc=True)
            .limit(1)
            .execute()
        )
    except Exception:
        logger.exception("Supabase query failed in check_if_new_data")
        raise
    return data.data

def fetch_new_data(supabase_data):
    """
    Determine which new rounds (if any) need to be inserted, then build full
    standings records for each new round using FastF1.

    Returns: list[dict] with keys:
    Year, EventName, RoundNumber, Driver, Team, QualifyingPosition, RacePoints, RacePosition, SprintPoints
    """
    # Ensure FastF1 cache directory exists (Lambda's writable temp space is /tmp)
    cache_dir = FASTF1_CACHE_DIR
    os.makedirs(cache_dir, exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)
    logger.info(f"FastF1 cache dir: {cache_dir}")

    # Optional delay between remote calls to be gentle to upstream APIs
    delay_sec = float(FASTF1_DELAY_SECONDS)

    current_year = datetime.now().year

    # Decide which rounds to process (current year only, for simplicity)
    schedule = fastf1.get_event_schedule(current_year, include_testing=False)
    completed_rounds = schedule[schedule['EventDate'] < datetime.now()]
    if completed_rounds.empty:
        logger.info("No completed rounds found.")
        return []
    latest_round = int(completed_rounds.iloc[-1]['RoundNumber'])

    start_round = 1
    if supabase_data:
        saved_roundnumber = supabase_data[0]['roundnumber']
        saved_year = supabase_data[0]['year']
        if current_year == saved_year:
            start_round = saved_roundnumber + 1
        elif current_year > saved_year:
            # New season: start at round 1 for current year
            start_round = 1
        else:
            # current year < saved_year; nothing to do
            return []

    rounds_to_fetch = list(range(start_round, latest_round + 1))
    if not rounds_to_fetch:
        logger.info("No new rounds to fetch.")
        return []

    # Build standings for each round
    standings = []
    for _, row in schedule.iterrows():
        round_number = int(row['RoundNumber'])
        if round_number not in rounds_to_fetch:
            continue

        event_name = row['EventName']
        fmt = row.get('EventFormat') if hasattr(row, 'get') else row['EventFormat']

        # Determine sprint session code based on year/format
        sprint_session_name = None
        if current_year >= 2024 and fmt == 'sprint_qualifying':
            sprint_session_name = 'S'
        elif current_year == 2023 and fmt == 'sprint_qualifying':
            sprint_session_name = 'SS'
        elif current_year in [2021, 2022] and fmt == 'sprint_qualifying':
            sprint_session_name = 'SPRINT'

        try:
            time.sleep(delay_sec)
            qualifying = fastf1.get_session(current_year, round_number, 'Q')
            qualifying.load(laps=False, telemetry=False, weather=False, messages=False)

            time.sleep(delay_sec)
            race = fastf1.get_session(current_year, round_number, 'R')
            race.load(laps=False, telemetry=False, weather=False, messages=False)

            sprint = None
            if sprint_session_name:
                time.sleep(delay_sec)
                sprint = fastf1.get_session(current_year, round_number, sprint_session_name)
                sprint.load(laps=False, telemetry=False, weather=False, messages=False)

            for _, driver in race.results.iterrows():
                abbreviation = driver["Abbreviation"]
                race_points = driver["Points"]
                race_position = driver["Position"]

                sprint_points = 0
                if sprint is not None:
                    driver_row = sprint.results[sprint.results["Abbreviation"] == abbreviation]
                    if not driver_row.empty:
                        sprint_points = driver_row["Points"].values[0]

                team = None
                qualifying_position = None
                qualifying_driver_row = qualifying.results[qualifying.results["Abbreviation"] == abbreviation]
                if not qualifying_driver_row.empty:
                    team = qualifying_driver_row["TeamName"].values[0]
                    qualifying_position = qualifying_driver_row["Position"].values[0]

                standings.append({
                    "Year": current_year,
                    "EventName": event_name,
                    "RoundNumber": round_number,
                    "Driver": abbreviation,
                    "Team": team,
                    "QualifyingPosition": qualifying_position,
                    "RacePoints": race_points,
                    "RacePosition": race_position,
                    "SprintPoints": sprint_points
                })
        except Exception as e:
            logger.info(f"Could not load data for year {current_year}, round {round_number}: {e}")
            continue

    return standings



def send_new_data(supabase: Client, new_rounds, dry_run=None, table_name=None):
    """Insert new round records into Supabase.

    Maps keys from fetch_new_data output to match the f1_data2 schema:
    Year, EventName, RoundNumber, Driver, Team → Year, EventName, RoundNumber, Driver, Team
    QualifyingPosition, RacePoints, RacePosition, SprintPoints → qualifyingposition, racepoints, raceposition, sprintpoints
    """
    if not new_rounds:
        logger.info("No new records to insert.")
        return None

    # Allow override via event parameter or environment variable
    if dry_run is None:
        dry_run = _truthy_env("DRY_RUN")
    table_name = table_name or TARGET_TABLE_DEFAULT

    # Simplified contract: expect list[dict] shaped for the target table
    if isinstance(new_rounds, dict):
        new_rounds = [new_rounds]
    if not (isinstance(new_rounds, list) and all(isinstance(r, dict) for r in new_rounds)):
        raise TypeError("send_new_data expects a list[dict] shaped for the target table")

    # Normalize keys to match the f1_data2 schema (all lowercase)
    normalized = []
    for record in new_rounds:
        normalized.append({
            "year": record.get("Year"),
            "eventname": record.get("EventName"),
            "roundnumber": record.get("RoundNumber"),
            "driver": record.get("Driver"),
            "team": record.get("Team"),
            "qualifyingposition": record.get("QualifyingPosition"),
            "racepoints": record.get("RacePoints"),
            "raceposition": record.get("RacePosition"),
            "sprintpoints": record.get("SprintPoints"),
        })
    new_rounds = normalized

    # Dry-run: log and return without inserting
    if dry_run:
        logger.info(f"DRY RUN enabled. Would insert {len(new_rounds)} rows into '{table_name}'. Skipping DB write.")
        if new_rounds:
            logger.info(f"First record: {json.dumps(new_rounds[0], default=str)}")
            logger.info(f"Record keys: {list(new_rounds[0].keys())}")
        return {
            'dry_run': True,
            'target_table': table_name,
            'count': len(new_rounds),
            'first': new_rounds[0] if new_rounds else None,
        }

    try:
        logger.info(f"About to insert {len(new_rounds)} rows into '{table_name}'.")
        if new_rounds:
            logger.info(f"First record: {json.dumps(new_rounds[0], default=str)}")
            logger.info(f"Record keys: {list(new_rounds[0].keys())}")
        res = supabase.table(table_name).insert(new_rounds).execute()
        logger.info(f"Inserted {len(new_rounds)} rows into '{table_name}'.")
        return res
    except Exception:
        logger.exception("Insert into Supabase failed in send_new_data")
        raise





def lambda_handler(event, context):
    try:
        supabase = connect_to_supabase()
        supabase_data = check_if_new_data(supabase)
        new_rounds = fetch_new_data(supabase_data)
        if new_rounds:
            # Allow event-driven overrides for testing
            dry_run = None
            table_override = None
            if isinstance(event, dict):
                dry_run = event.get('dry_run', None)
                table_override = event.get('target_table', None)
            send_new_data(supabase, new_rounds, dry_run=dry_run, table_name=table_override)
        body = 'Success: processed and sent new info' if new_rounds else 'Success: no new rounds'
        return {
            'statusCode': 200,
            'body': json.dumps(body)
        }
    except Exception as e:
        # Log the full exception details in CloudWatch; return a safe error message
        logger.exception("Lambda handler failed")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }



