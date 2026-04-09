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

SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

# ==============================
# CONFIG
# ==============================
REQUEST_TYPE_FIELD = "customfield_10010"

FIELDS = [
    "created",
    "resolutiondate",
    REQUEST_TYPE_FIELD
]

JIRA_QUERY_ACTIVITY = """
project = ISD
AND statusCategory = Done
AND "Request Type" IN (
    "Employee offboarding",
    "Onboard new employees",
    "IT Request"
)
AND (
    created >= -30d
    OR resolved >= -30d
)
"""

JIRA_QUERY_BACKLOG = """
project = ISD
AND statusCategory != Done
"""

# ==============================
# JIRA FETCH
# ==============================
def fetch_jira_issues(jql):
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
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Accept": "application/json"},
            params=params,
        )

        res.raise_for_status()
        data = res.json()

        issues = data.get("issues", [])
        all_issues.extend(issues)

        if start_at + 100 >= data.get("total", 0):
            break

        start_at += 100

    print(f"✅ Fetched {len(all_issues)} issues")
    return all_issues


# ==============================
# TRANSFORM
# ==============================
def issues_to_dataframe(issues):
    rows = []

    for issue in issues:
        f = issue.get("fields", {}) or {}

        rt_field = f.get(REQUEST_TYPE_FIELD)
        request_type = None

        if isinstance(rt_field, dict):
            request_type = rt_field.get("requestType", {}).get("name")

        rows.append({
            "CreatedDate": f.get("created"),
            "ResolvedDate": f.get("resolutiondate"),
            "RequestType": request_type
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["CreatedDate"] = pd.to_datetime(df["CreatedDate"], errors="coerce", utc=True)
    df["ResolvedDate"] = pd.to_datetime(df["ResolvedDate"], errors="coerce", utc=True)

    return df


# ==============================
# METRICS
# ==============================
def compute_weekly_metrics(df_activity, df_backlog):
    if df_activity.empty and df_backlog.empty:
        return None

    now = pd.Timestamp.now(tz="UTC")

    week_start = now - pd.Timedelta(days=now.weekday())
    week_start = week_start.normalize()

    week_end = week_start + pd.Timedelta(days=6, hours=23, minutes=59, seconds=59)

    submitted = df_activity[
        (df_activity["CreatedDate"] >= week_start) &
        (df_activity["CreatedDate"] <= week_end)
    ].shape[0]

    resolved = df_activity[
        (df_activity["ResolvedDate"].notna()) &
        (df_activity["ResolvedDate"] >= week_start) &
        (df_activity["ResolvedDate"] <= week_end)
    ].shape[0]

    open_count = df_backlog.shape[0]

    onboarding_completed = df_activity[
        (df_activity["RequestType"] == "Onboard new employees") &
        (df_activity["ResolvedDate"].notna()) &
        (df_activity["ResolvedDate"] >= week_start) &
        (df_activity["ResolvedDate"] <= week_end)
    ].shape[0]

    offboarding_completed = df_activity[
        (df_activity["RequestType"] == "Employee offboarding") &
        (df_activity["ResolvedDate"].notna()) &
        (df_activity["ResolvedDate"] >= week_start) &
        (df_activity["ResolvedDate"] <= week_end)
    ].shape[0]

    return {
        "WeekStart": week_start.strftime("%Y-%m-%d"),
        "Submitted": int(submitted),
        "Resolved": int(resolved),
        "Open": int(open_count),
        "OnboardingCompleted": int(onboarding_completed),
        "OffboardingCompleted": int(offboarding_completed)
    }


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


def graph_get_list_id(token, site_id, list_name):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()

    for l in res.json()["value"]:
        if l["name"] == list_name:
            return l["id"]

    raise Exception(f"List '{list_name}' not found")


# ==============================
# UPSERT
# ==============================
def upsert_metrics(token, site_id, list_id, metrics):
    week = metrics["WeekStart"]

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields"
    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()

    for item in res.json()["value"]:
        if item["fields"].get("WeekStart") == week:
            item_id = item["id"]

            update_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"

            requests.patch(
                update_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json={"Title": f"Week {week}", **metrics}
            )

            print(f"Updated week {week}")
            return

    create_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

    requests.post(
        create_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={"fields": {"Title": f"Week {week}", **metrics}}
    )

    print(f"Created week {week}")


# ==============================
# GET LAST 2 WEEKS (OPTIMIZED)
# ==============================
def get_recent_metrics(token, site_id, list_id):
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
        f"?$expand=fields"
    )

    res = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    res.raise_for_status()

    rows = []
    for item in res.json()["value"]:
        f = item["fields"]

        week = f.get("WeekStart")
        if not week:
            continue

        rows.append({
            "WeekStart": week,
            "Submitted": int(f.get("Submitted", 0)),
            "Resolved": int(f.get("Resolved", 0)),
            "Open": int(f.get("Open", 0)),
            "OnboardingCompleted": int(f.get("OnboardingCompleted", 0)),
            "OffboardingCompleted": int(f.get("OffboardingCompleted", 0)),
        })

    if not rows:
        return None, None

    # 🔥 Use pandas for reliable sorting
    df = pd.DataFrame(rows)

    df["WeekStart"] = pd.to_datetime(df["WeekStart"], errors="coerce")
    df = df.dropna(subset=["WeekStart"])

    df = df.sort_values("WeekStart", ascending=False)

    current = df.iloc[0].to_dict()
    previous = df.iloc[1].to_dict() if len(df) > 1 else None

    return current, previous


# ==============================
# SLACK
# ==============================

def format_date(date_str):
    dt = pd.to_datetime(date_str)
    return dt.strftime("%b %d, %Y")   # Apr 06, 2026

def build_slack_blocks(current, previous=None):
    def diff(curr, prev):
        if prev is None:
            return ""
        delta = curr - prev
        if delta > 0:
            return f" 🟢 +{delta}"
        elif delta < 0:
            return f" 🔴 {delta}"
        return " ⚪ 0"

    def trend_summary():
        if not previous:
            return "No previous data for comparison."

        summary = []

        if current["Resolved"] > previous["Resolved"]:
            summary.append("Resolution improved")
        elif current["Resolved"] < previous["Resolved"]:
            summary.append("Resolution slowed")

        if current["Open"] > previous["Open"]:
            summary.append("Backlog increased")
        elif current["Open"] < previous["Open"]:
            summary.append("Backlog reduced")

        if not summary:
            return "No significant changes vs last week"

        return " • ".join(summary)

    week_label = pd.to_datetime(current["WeekStart"]).strftime("%b %d, %Y")

    return [
        # ==============================
        # HEADER
        # ==============================
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊 Weekly IT Report — {week_label}"
            }
        },

        # ==============================
        # SUMMARY
        # ==============================
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Summary*\n{trend_summary()}"
            }
        },

        {"type": "divider"},

        # ==============================
        # TICKET ACTIVITY
        # ==============================
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*🎫 Ticket Activity*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*New Requests:* {current['Submitted']}"
                    f"{diff(current['Submitted'], previous['Submitted'] if previous else None)}\n\n"
                    f"*Resolved:* {current['Resolved']}"
                    f"{diff(current['Resolved'], previous['Resolved'] if previous else None)}\n\n"
                    f"*Open Backlog:* {current['Open']}"
                    f"{diff(current['Open'], previous['Open'] if previous else None)}"
                )
            }
        },

        {"type": "divider"},

        # ==============================
        # EMPLOYEE LIFECYCLE OPS
        # ==============================
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*👥 Employee Lifecycle Ops*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Onboarding Completed:* {current['OnboardingCompleted']}"
                    f"{diff(current['OnboardingCompleted'], previous['OnboardingCompleted'] if previous else None)}\n\n"
                    f"*Offboarding Completed:* {current['OffboardingCompleted']}"
                    f"{diff(current['OffboardingCompleted'], previous['OffboardingCompleted'] if previous else None)}"
                )
            }
        },

        {"type": "divider"},

        # ==============================
        # FOOTER
        # ==============================
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Data source: Jira • Automated weekly report"
                }
            ]
        }
    ]


def send_to_slack(blocks):
    res = requests.post(
        SLACK_WEBHOOK_URL,
        json={"blocks": blocks},
        headers={"Content-Type": "application/json"}
    )
    res.raise_for_status()
    print("Slack message sent")


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("🚀 Jira → SharePoint → Slack")

    issues_activity = fetch_jira_issues(JIRA_QUERY_ACTIVITY)
    issues_backlog = fetch_jira_issues(JIRA_QUERY_BACKLOG)

    df_activity = issues_to_dataframe(issues_activity)
    df_backlog = issues_to_dataframe(issues_backlog)

    metrics = compute_weekly_metrics(df_activity, df_backlog)

    if not metrics:
        print("No metrics")
        exit()

    token = get_graph_token()
    site_id = graph_get_site_id(token)
    list_id = graph_get_list_id(token, site_id, "Weekly Ticket Metrics")

    # Upsert current week
    upsert_metrics(token, site_id, list_id, metrics)

    # Fetch last 2 weeks using FILTER
    current, previous = get_recent_metrics(token, site_id, list_id)

    # Send Slack message
    blocks = build_slack_blocks(current, previous)
    send_to_slack(blocks)

    print("🎉 DONE")
