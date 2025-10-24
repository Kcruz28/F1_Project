import json
import boto3
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import pandas as pd


def connect_to_supabase():
    load_dotenv()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)
    return supabase

def check_if_new_data(supabase):
    ### fetching latest year and round
    data = supabase.table('f1_entries').select("roundnumber, year") \
    .order("year", ascending=False).order("year", ascending=False).limit(1).execute()
    return data.data

def fetch_new_data(supabase_data):
    fastf1.Cache.enable_cache('/tmp/fastf1_cache')
    current_year = datetime.now().year
    
    saved_roundnumber = supabase_data[0]['roundnumber']
    saved_year = supabase_data[0]['year']
    

    if cuurrent_year > saved_year:
        print(f"New year found: {current_year}")
        schedule = fastf1.get_event_schedule(current_year, include_testing=False)
        completed_rounds = schedule[schedule['EventDate'] < datetime.now()]

        if completed_rounds.empty:
            print("No completed rounds found.")
            return []
        
        f1_latest_round = completed_rounds.iloc[-1]['RoundNumber']
        return list(range(1, f1_latest_round + 1))

    elif current_year == saved_year:
        schedule = fastf1.get_event_schedule(current_year, include_testing=False)
        completed_rounds = schedule[schedule['EventDate'] < datetime.now()]

        if completed_rounds.empty:
            print("No completed rounds found.")
            return []
        

        f1_latest_round = completed_rounds.iloc[-1]['RoundNumber']

        if latest_round > saved_roundnumber:
            print(f"New round found: {latest_round}")
            return list(range(saved_roundnumber + 1, latest_round + 1))
        else:
            print("No new rounds found.")
            return []



def send_new_data(new_rounds):
    supabase.table('f1data2').insert(new_rounds).execute()
    print("New data sent to Supabase. SUCCESSS")





def lambda_handler(event, context):
    # TODO implement
    supabase = connect_to_supabase()
    supabase_data = check_if_new_data(supabase)
    new_rounds = fetch_new_data(supabase_data)
    if new_rounds:
        send_new_data(new_rounds)


    return {
        'statusCode': 200,
        'body': json.dumps('Sheesh it worked and new info sent!!!')
    }



