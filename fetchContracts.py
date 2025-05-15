import requests
import pandas as pd
from datetime import datetime, timedelta
import boto3

# 🔐 Configuration
API_KEY = "Xs0vOAUZrmo13Tg5kiWeDUdTDka8z8oIsxTynPnQ"
BASE_URL = "https://api.sam.gov/prod/opportunities/v2/search"
KEYWORD = "software"
DAYS_BACK = 5
LIMIT = 100
MAX_RECORDS = 500
S3_BUCKET = "cleo-samgov-etl"
S3_KEY = "contracts/veteran_contracts.parquet"

# 📆 Date Range Helper
def get_date_range():
    today = datetime.now()
    past = today - timedelta(days=DAYS_BACK)
    return past.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y')

# 🌐 Fetch from API
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

        print(f"Fetching records {offset}–{offset + LIMIT}...")
        resp = requests.get(BASE_URL, params=params, timeout=10)
        if resp.status_code != 200:
            print("Error:", resp.status_code, resp.text)
            break

        data = resp.json()
        results = data.get("opportunitiesData", [])
        all_results.extend(results)

        if len(results) < LIMIT or len(all_results) >= MAX_RECORDS:
            break

        offset += LIMIT

    return all_results[:MAX_RECORDS]

# 🎯 Filter for Veteran-Owned Set-Asides
def filter_veteran_set_asides(data):
    veteran_keywords = ["Veteran-Owned", "Service-Disabled Veteran-Owned"]
    return [
        opp for opp in data
        if isinstance(opp.get("typeOfSetAsideDescription"), str)
        and any(keyword in opp["typeOfSetAsideDescription"] for keyword in veteran_keywords)
    ]

# 🔄 Transform & Enrich Data
def transform_opportunities(opps):
    flattened = [
        {
            "noticeId": o.get("noticeId"),
            "title": o.get("title", "").strip(),
            "solicitationNumber": o.get("solicitationNumber", "").strip(),
            "agency": o.get("fullParentPathName", "").strip(),
            "postedDate": o.get("postedDate"),
            "setAside": o.get("typeOfSetAsideDescription", "").strip(),
            "naicsCode": o.get("naicsCode", "").strip(),
            "city": o.get("officeAddress", {}).get("city", "").strip(),
            "state": o.get("officeAddress", {}).get("state", "").strip(),
            "link": o.get("uiLink", "").strip()
        }
        for o in opps
    ]

    df = pd.DataFrame(flattened)

    # 🧼 Clean
    df = df.dropna(subset=["noticeId", "title", "postedDate"])
    df["postedDate"] = pd.to_datetime(df["postedDate"], errors="coerce")
    df["daysSincePosted"] = (pd.Timestamp.now() - df["postedDate"]).dt.days
    df["isRecent"] = df["daysSincePosted"] <= 7
    df["hasNAICS"] = df["naicsCode"].apply(lambda x: bool(x and x.strip()))
    df["state"] = df["state"].str.upper()

    # 🧾 Map NAICS
    naics_map = {
        "541511": "Custom Computer Programming",
        "541512": "Systems Design Services",
        "561730": "Landscaping Services"
    }
    df["naicsDescription"] = df["naicsCode"].map(naics_map).fillna("Other")

    # 🏅 Add Recency Score
    def recency_score(days):
        if days <= 1:
            return 5
        elif days <= 3:
            return 4
        elif days <= 5:
            return 3
        elif days <= 7:
            return 2
        else:
            return 1

    df["recencyScore"] = df["daysSincePosted"].apply(recency_score)
    df = df.sort_values(by=["recencyScore", "postedDate"], ascending=[False, False])

    return df

# 💾 Save to Parquet
def save_to_parquet(opps, filename="veteran_contracts.parquet"):
    df = transform_opportunities(opps)
    df.to_parquet(filename, engine='pyarrow')
    print(f"\n✅ Saved {len(df)} transformed records to {filename}")

# 📤 Upload to S3
def upload_to_s3(local_file, bucket_name, s3_key):
    s3 = boto3.client("s3")
    s3.upload_file(local_file, bucket_name, s3_key)
    print(f"✅ Uploaded {local_file} to s3://{bucket_name}/{s3_key}")

def ensure_bucket_exists(bucket_name, region="us-east-1"):
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists.")
    except boto3.exceptions.botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"🪣 Creating bucket: {bucket_name}")
            if region == "us-east-1":
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": region}
                )
        else:
            raise

# 📋 Print to Console
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

# 🚀 Run ETL Pipeline
if __name__ == "__main__":
    all_opps = fetch_all_results()
    veteran_opps = filter_veteran_set_asides(all_opps)

    print(f"\n✅ Total veteran-related opportunities: {len(veteran_opps)}")
    print_opportunities(veteran_opps)

    # Save locally
    save_to_parquet(veteran_opps)

    # Ensure bucket exists
    ensure_bucket_exists(S3_BUCKET)

    # Upload to S3
    upload_to_s3("veteran_contracts.parquet", S3_BUCKET, S3_KEY)