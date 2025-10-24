import json
import os
from datetime import datetime
import fastf1
from supabase import create_client, Client


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
    data = (
        supabase
        .table('f1_entries')
        .select("roundnumber, year")
        .order("year", ascending=False)
        .order("roundnumber", ascending=False)
        .limit(1)
        .execute()
    )
    return data.data

def fetch_new_data(supabase_data):
    """
    Determine which new rounds (if any) need to be inserted.
    Returns a list of round numbers to insert.
    """
    fastf1.Cache.enable_cache('/tmp/fastf1_cache')
    current_year = datetime.now().year

    # If there's no data saved yet, start from round 1 up to the latest completed round
    if not supabase_data:
        schedule = fastf1.get_event_schedule(current_year, include_testing=False)
        completed_rounds = schedule[schedule['EventDate'] < datetime.now()]
        if completed_rounds.empty:
            print("No completed rounds found.")
            return []
        latest_round = int(completed_rounds.iloc[-1]['RoundNumber'])
        return list(range(1, latest_round + 1))

    saved_roundnumber = supabase_data[0]['roundnumber']
    saved_year = supabase_data[0]['year']

    if current_year > saved_year:
        print(f"New year found: {current_year}")
        schedule = fastf1.get_event_schedule(current_year, include_testing=False)
        completed_rounds = schedule[schedule['EventDate'] < datetime.now()]

        if completed_rounds.empty:
            print("No completed rounds found.")
            return []
        latest_round = int(completed_rounds.iloc[-1]['RoundNumber'])
        return list(range(1, latest_round + 1))

    elif current_year == saved_year:
        schedule = fastf1.get_event_schedule(current_year, include_testing=False)
        completed_rounds = schedule[schedule['EventDate'] < datetime.now()]

        if completed_rounds.empty:
            print("No completed rounds found.")
            return []
        latest_round = int(completed_rounds.iloc[-1]['RoundNumber'])
        if latest_round > saved_roundnumber:
            print(f"New round found: {latest_round}")
            return list(range(saved_roundnumber + 1, latest_round + 1))
        else:
            print("No new rounds found.")
            return []
    else:
        # current_year < saved_year; nothing to do
        return []



def send_new_data(supabase: Client, new_rounds):
    # Note: Ensure the shape of new_rounds matches your table schema.
    supabase.table('f1data2').insert(new_rounds).execute()
    print("New data sent to Supabase. SUCCESS")





def lambda_handler(event, context):
    # TODO implement
    supabase = connect_to_supabase()
    supabase_data = check_if_new_data(supabase)
    new_rounds = fetch_new_data(supabase_data)
    if new_rounds:
        send_new_data(supabase, new_rounds)


    return {
        'statusCode': 200,
        'body': json.dumps('Sheesh it worked and new info sent!!!')
    }



