import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

# ==============================
# ENV VARIABLES
# ==============================
JIRA_BASE_URL = os.environ["JIRA_BASE_URL"]
JIRA_EMAIL = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]

SP_TENANT_ID = os.environ["SP_TENANT_ID"]
SP_CLIENT_ID = os.environ["SP_CLIENT_ID"]
SP_CLIENT_SECRET = os.environ["SP_CLIENT_SECRET"]

SP_SITE_HOSTNAME = os.environ["SP_SITE_HOSTNAME"]
SP_SITE_PATH = os.environ["SP_SITE_PATH"]

# ==============================
# CONFIG
# ==============================
FIELDS = [
    "created",
    "resolutiondate"
]

# ✅ HISTORICAL QUERY (for true backlog)
JIRA_QUERY = """
project = ISD
AND created >= -90d
ORDER BY created ASC
"""

# ==============================
# JIRA FETCH
# ==============================
def fetch_jira_issues(jql):
    print("🔄 Fetching Jira issues...")

    all_issues = []
    start_at = 0

    while True:
        params = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": 100,
            "fields": ",".join(FIELDS),
        }

        res = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Accept": "application/json"},
            params=params,
        )

        if res.status_code != 200:
            print(res.text)
            raise Exception("❌ Jira API failed")

        data = res.json()
        issues = data.get("issues", [])

        all_issues.extend(issues)

        if start_at + 100 >= data.get("total", 0):
            break

        start_at += 100

    print(f"✅ Total issues fetched: {len(all_issues)}")
    return all_issues


# ==============================
# TRANSFORM
# ==============================
def issues_to_dataframe(issues):
    rows = []

    for issue in issues:
        f = issue.get("fields", {}) or {}

        rows.append({
            "CreatedDate": f.get("created"),
            "ResolvedDate": f.get("resolutiondate"),
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["CreatedDate"] = pd.to_datetime(df["CreatedDate"], errors="coerce")
    df["ResolvedDate"] = pd.to_datetime(df["ResolvedDate"], errors="coerce")

    return df


# ==============================
# TRUE BACKLOG METRICS
# ==============================
def compute_weekly_metrics(df):
    print("🔄 Computing TRUE backlog metrics...")

    if df.empty:
        return None

    now = pd.Timestamp.utcnow()

    week_start = now.to_period("W").start_time
    week_end = now.to_period("W").end_time

    # ✅ Submitted this week
    submitted = df[
        (df["CreatedDate"] >= week_start) &
        (df["CreatedDate"] <= week_end)
    ].shape[0]

    # ✅ Resolved this week
    resolved = df[
        (df["ResolvedDate"].notna()) &
        (df["ResolvedDate"] >= week_start) &
        (df["ResolvedDate"] <= week_end)
    ].shape[0]

    # ✅ TRUE backlog
    open_count = df[
        (df["CreatedDate"] <= week_end) &
        (
            df["ResolvedDate"].isna() |
            (df["ResolvedDate"] > week_end)
        )
    ].shape[0]

    metrics = {
        "WeekStart": week_start.strftime("%Y-%m-%d"),
        "Submitted": int(submitted),
        "Resolved": int(resolved),
        "Open": int(open_count)
    }

    print("✅ Metrics:", metrics)
    return metrics


# ==============================
# GRAPH AUTH
# ==============================
def get_graph_token():
    url = f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/v2.0/token"

    res = requests.post(url, data={
        "client_id": SP_CLIENT_ID,
        "client_secret": SP_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    })

    res.raise_for_status()
    return res.json()["access_token"]


def graph_get_site_id(token):
    url = f"https://graph.microsoft.com/v1.0/sites/{SP_SITE_HOSTNAME}:{SP_SITE_PATH}"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()
    return res.json()["id"]


def graph_get_lists(token, site_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()
    return {l["name"]: l["id"] for l in res.json()["value"]}


# ==============================
# UPSERT
# ==============================
def graph_upsert_item(token, site_id, list_id, week_start, payload):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()

    for item in res.json()["value"]:
        if item["fields"].get("WeekStart") == week_start:
            item_id = item["id"]

            update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"

            requests.patch(
                update_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json=payload
            )

            print(f"♻️ Updated week {week_start}")
            return

    # Create new
    create_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

    requests.post(
        create_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={"fields": payload}
    )

    print(f"✅ Created week {week_start}")


# ==============================
# PUSH TO SHAREPOINT
# ==============================
def push_metrics(token, site_id, metrics):
    lists = graph_get_lists(token, site_id)

    resolved_id = lists["Weekly Resolved Tickets"]
    submitted_id = lists["Weekly Submitted Tickets"]
    open_id = lists["Weekly Open Tickets"]

    week = metrics["WeekStart"]

    graph_upsert_item(token, site_id, resolved_id, week, {
        "Title": f"Week {week}",
        "WeekStart": week,
        "ResolvedCount": metrics["Resolved"]
    })

    graph_upsert_item(token, site_id, submitted_id, week, {
        "Title": f"Week {week}",
        "WeekStart": week,
        "SubmittedCount": metrics["Submitted"]
    })

    graph_upsert_item(token, site_id, open_id, week, {
        "Title": f"Week {week}",
        "WeekStart": week,
        "OpenCount": metrics["Open"]
    })


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("🚀 Jira → SharePoint TRUE Weekly Metrics")

    issues = fetch_jira_issues(JIRA_QUERY)
    df = issues_to_dataframe(issues)

    if df.empty:
        print("⚠️ No data found")
        exit()

    metrics = compute_weekly_metrics(df)

    if not metrics:
        print("⚠️ No metrics computed")
        exit()

    token = get_graph_token()
    site_id = graph_get_site_id(token)

    push_metrics(token, site_id, metrics)

    print("🎉 DONE")
