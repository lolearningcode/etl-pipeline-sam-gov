import requests
import pandas as pd
from datetime import datetime, timedelta

API_KEY = "Xs0vOAUZrmo13Tg5kiWeDUdTDka8z8oIsxTynPnQ"
BASE_URL = "https://api.sam.gov/prod/opportunities/v2/search"
KEYWORD = "software"
DAYS_BACK = 14
LIMIT = 100

def get_date_range():
    today = datetime.now()
    past = today - timedelta(days=DAYS_BACK)
    return past.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y')

def fetch_all_results():
    posted_from, posted_to = get_date_range()
    all_results = []
    offset = 0

    while True:
        params = {
            "api_key": API_KEY,
            "q": KEYWORD,
            "postedFrom": posted_from,
            "postedTo": posted_to,
            "limit": LIMIT,
            "offset": offset
        }

        resp = requests.get(BASE_URL, params=params)
        if resp.status_code != 200:
            print("Error:", resp.status_code, resp.text)
            break

        data = resp.json()
        results = data.get("opportunitiesData", [])
        all_results.extend(results)

        if len(results) < LIMIT:
            break  # No more pages

        offset += LIMIT

    return all_results

def filter_veteran_set_asides(data):
    veteran_keywords = ["Veteran-Owned", "Service-Disabled Veteran-Owned"]
    return [
        opp for opp in data
        if any(keyword in opp.get("typeOfSetAsideDescription", "") for keyword in veteran_keywords)
    ]

def flatten_opps(opps):
    # Flatten for DataFrame conversion
    return [
        {
            "noticeId": o.get("noticeId"),
            "title": o.get("title"),
            "solicitationNumber": o.get("solicitationNumber"),
            "agency": o.get("fullParentPathName"),
            "postedDate": o.get("postedDate"),
            "typeOfSetAside": o.get("typeOfSetAsideDescription"),
            "naicsCode": o.get("naicsCode"),
            "city": o.get("officeAddress", {}).get("city"),
            "state": o.get("officeAddress", {}).get("state"),
            "link": o.get("uiLink")
        }
        for o in opps
    ]

def save_to_parquet(data, filename="veteran_contracts.parquet"):
    df = pd.DataFrame(flatten_opps(data))
    df.to_parquet(filename, engine='pyarrow')
    print(f"Saved {len(df)} records to {filename}")

def print_opportunities(opps):
    for i, opp in enumerate(opps, 1):
        print(f"\n🔹 Opportunity #{i}")
        print(f"Title: {opp.get('title')}")
        print(f"Solicitation #: {opp.get('solicitationNumber')}")
        print(f"Agency: {opp.get('fullParentPathName')}")
        print(f"Posted Date: {opp.get('postedDate')}")
        print(f"Set-Aside Type: {opp.get('typeOfSetAsideDescription')}")
        print(f"NAICS Code: {opp.get('naicsCode')}")
        print(f"Location: {opp.get('officeAddress', {}).get('city')}, {opp.get('officeAddress', {}).get('state')}")
        print(f"Link: {opp.get('uiLink')}")
        print("-" * 60)

# 🔁 Run the pipeline
all_opps = fetch_all_results()
veteran_opps = filter_veteran_set_asides(all_opps)

print(f"\n✅ Total veteran-related opportunities: {len(veteran_opps)}")
print_opportunities(veteran_opps)

save_to_parquet(veteran_opps)