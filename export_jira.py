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

# ==============================
# JQL QUERIES (FIXED)
# ==============================
JIRA_QUERY_ACTIVITY = """
project = ISD
AND (
    created >= -30d
    OR resolved >= -30d
)
ORDER BY created DESC
"""

JIRA_QUERY_BACKLOG = """
project = ISD
AND statusCategory != Done
ORDER BY created DESC
"""

# ==============================
# FETCH JIRA WITH PAGINATION
# ==============================
def fetch_jira_issues(jql):
    print(f"🔄 Fetching Jira issues...\nJQL: {jql}")

    url = f"{JIRA_BASE_URL}/rest/api/3/search/jql"
    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)

    all_issues = []
    start_at = 0
    max_results = 100

    while True:
        response = requests.get(
            url,
            params={
                "jql": jql,
                "startAt": start_at,
                "maxResults": max_results,
                "fields": "created,resolutiondate,status"
            },
            auth=auth
        )

        response.raise_for_status()
        data = response.json()

        issues = data.get("issues", [])
        all_issues.extend(issues)

        print(f"   🔹 Fetched {len(issues)} issues (startAt={start_at})")

        if len(issues) < max_results:
            break

        start_at += max_results

    print(f"✅ Total issues fetched: {len(all_issues)}\n")
    return all_issues


# ==============================
# CONVERT TO DATAFRAME
# ==============================
def issues_to_dataframe(issues):
    rows = []

    for issue in issues:
        fields = issue["fields"]

        rows.append({
            "CreatedDate": pd.to_datetime(fields.get("created"), utc=True),
            "ResolvedDate": pd.to_datetime(fields.get("resolutiondate"), utc=True)
        })

    # ✅ IMPORTANT: ensure columns always exist even if empty
    return pd.DataFrame(rows, columns=["CreatedDate", "ResolvedDate"])


# ==============================
# COMPUTE METRICS (FIXED)
# ==============================
def compute_weekly_metrics(df_activity, df_backlog):
    print("🔄 Computing TRUE metrics...")

    if df_activity.empty and df_backlog.empty:
        return None

    # ✅ Use UTC consistently
    now = pd.Timestamp.now(tz="UTC")

    # ✅ Start of week (Monday)
    week_start = (now - pd.Timedelta(days=now.weekday())).normalize()
    week_end = week_start + pd.Timedelta(days=7)

    print(f"Week range: {week_start} → {week_end}")

    # ======================
    # SUBMITTED
    # ======================
    submitted = df_activity[
        (df_activity["CreatedDate"] >= week_start) &
        (df_activity["CreatedDate"] < week_end)
    ].shape[0]

    # ======================
    # RESOLVED
    # ======================
    resolved = df_activity[
        (df_activity["ResolvedDate"].notna()) &
        (df_activity["ResolvedDate"] >= week_start) &
        (df_activity["ResolvedDate"] < week_end)
    ].shape[0]

    # ======================
    # TRUE BACKLOG
    # ======================
    open_count = df_backlog.shape[0]

    metrics = {
        "WeekStart": week_start.strftime("%Y-%m-%d"),
        "Submitted": int(submitted),
        "Resolved": int(resolved),
        "Open": int(open_count)
    }

    print("✅ Metrics:", metrics)
    return metrics


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("🚀 Jira → SharePoint TRUE Weekly Metrics")

    # 🔹 Fetch BOTH datasets
    issues_activity = fetch_jira_issues(JIRA_QUERY_ACTIVITY)
    issues_backlog = fetch_jira_issues(JIRA_QUERY_BACKLOG)

    # 🔹 Convert to DataFrames
    df_activity = issues_to_dataframe(issues_activity)
    df_backlog = issues_to_dataframe(issues_backlog)

    # 🔹 Compute metrics
    metrics = compute_weekly_metrics(df_activity, df_backlog)

    if metrics:
        print("🎉 DONE")
    else:
        print("⚠️ No data found")
