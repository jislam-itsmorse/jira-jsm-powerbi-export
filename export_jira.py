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
SP_LIST_URL = os.environ["SP_LIST_URL"]

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
AND (
    created >= -8d
    OR resolved >= -8d
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

    days_since_last_friday = 7
    last_friday = (now - pd.Timedelta(days=days_since_last_friday)).normalize() + pd.Timedelta(hours=12, minutes=1)
    this_friday = now.normalize() + pd.Timedelta(hours=12)

    window_start = last_friday
    window_end = this_friday

    submitted = df_activity[
        (df_activity["CreatedDate"] >= window_start) &
        (df_activity["CreatedDate"] <= window_end)
    ].shape[0]

    resolved_df = df_activity[
        (df_activity["ResolvedDate"].notna()) &
        (df_activity["ResolvedDate"] >= window_start) &
        (df_activity["ResolvedDate"] <= window_end)
    ]
    resolved = resolved_df.shape[0]

    open_count = df_backlog.shape[0]

    onboarding_completed = resolved_df[
        resolved_df["RequestType"] == "Onboard new employees"
    ].shape[0]

    offboarding_completed = resolved_df[
        resolved_df["RequestType"] == "Employee offboarding"
    ].shape[0]

    return {
        # ✅ Date-only strings so SharePoint accepts them cleanly
        "WeekStart": window_start.strftime("%Y-%m-%d"),
        "WeekEnd": window_end.strftime("%Y-%m-%d"),
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
    title = f"IT Report | {metrics['WeekStart']} → {metrics['WeekEnd']}"

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
                json={"Title": title, **metrics}
            )
            print(f"Updated: {title}")
            return

    create_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"

    requests.post(
        create_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={"fields": {"Title": title, **metrics}}
    )

    print(f"Created: {title}")


# ==============================
# GET LAST 2 WEEKS
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
            "WeekEnd": f.get("WeekEnd", ""),
            "Submitted": int(f.get("Submitted", 0)),
            "Resolved": int(f.get("Resolved", 0)),
            "Open": int(f.get("Open", 0)),
            "OnboardingCompleted": int(f.get("OnboardingCompleted", 0)),
            "OffboardingCompleted": int(f.get("OffboardingCompleted", 0)),
        })

    if not rows:
        return None, None

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
def build_slack_blocks(current, list_url):
    week_start_dt = pd.to_datetime(current["WeekStart"])
    week_end_dt = pd.to_datetime(current["WeekEnd"])

    week_label = (
        f"{week_start_dt.strftime('%b %d, %Y %H:%M')} – "
        f"{week_end_dt.strftime('%b %d, %Y %H:%M')} UTC"
    )

    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊 Weekly IT Report — {week_label}"
            }
        },
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
                    f"*New Requests:* {current['Submitted']}\n\n"
                    f"*Resolved:* {current['Resolved']}\n\n"
                    f"*Open Backlog:* {current['Open']}"
                )
            }
        },
        {"type": "divider"},
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
                    f"*Onboarding Completed:* {current['OnboardingCompleted']}\n\n"
                    f"*Offboarding Completed:* {current['OffboardingCompleted']}"
                )
            }
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "📌 *Need more detail?* "
                        f"<{list_url}|View the full report list>\n"
                        "Data source: Jira • Automated weekly report"
                    )
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

    upsert_metrics(token, site_id, list_id, metrics)

    # ✅ Use metrics directly instead of re-fetching from SharePoint
    # get_recent_metrics was returning None because the WeekStart format
    # with datetime string wasn't matching on read-back
    blocks = build_slack_blocks(metrics, SP_LIST_URL)
    send_to_slack(blocks)

    print("🎉 DONE")
