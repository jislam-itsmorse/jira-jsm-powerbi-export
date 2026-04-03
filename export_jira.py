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
SP_LIBRARY_NAME = os.environ["SP_LIBRARY_NAME"]

# ==============================
# JIRA CONFIG
# ==============================
FIELDS = [
    "summary",
    "status",
    "created",
    "resolutiondate",
    "assignee",
    "issuetype"
]

# 🔥 ONLY CURRENT WEEK DATA
JIRA_QUERY = """
    project = ISD
    AND created >= startOfWeek(-12)
    ORDER BY created ASC
"""


# ==============================
# JIRA FETCH
# ==============================
def fetch_jira_issues(jql):
    print("🔄 Fetching Jira issues...")
    print(jql.strip())

    all_issues = []
    next_page_token = None

    while True:
        params = {
            "jql": jql,
            "maxResults": 100,
            "fields": ",".join(FIELDS),
        }

        if next_page_token:
            params["nextPageToken"] = next_page_token

        response = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Accept": "application/json"},
            params=params,
        )

        print(f"Jira API Status: {response.status_code}")

        if response.status_code != 200:
            print(response.text)
            raise Exception("❌ Jira API call failed")

        data = response.json()
        issues = data.get("issues", [])

        all_issues.extend(issues)
        next_page_token = data.get("nextPageToken")

        if not next_page_token:
            break

    print(f"✅ Total issues fetched: {len(all_issues)}")
    return all_issues


# ==============================
# TRANSFORM
# ==============================
def issues_to_dataframe(issues):
    print("🔄 Transforming data...")

    rows = []
    for issue in issues:
        f = issue.get("fields", {}) or {}

        created = f.get("created")
        resolved = f.get("resolutiondate")

        rows.append({
            "IssueKey": issue.get("key"),
            "Summary": f.get("summary"),
            "Status": (f.get("status") or {}).get("name"),
            "CreatedDate": created,
            "ResolvedDate": resolved,
            "Assignee": (f.get("assignee") or {}).get("displayName"),
            "IssueType": (f.get("issuetype") or {}).get("name"),

            # Useful flags for Power BI
            "IsResolved": 1 if resolved else 0,
            "IsOpen": 1 if not resolved else 0
        })

    df = pd.DataFrame(rows)
    print(f"✅ Dataframe created with {len(df)} rows")
    return df


# ==============================
# GRAPH AUTH
# ==============================
def get_graph_token():
    url = f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/v2.0/token"

    response = requests.post(url, data={
        "client_id": SP_CLIENT_ID,
        "client_secret": SP_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    })

    if response.status_code != 200:
        print("❌ Token error:", response.text)
        raise Exception("Failed to get Graph token")

    return response.json()["access_token"]


def graph_get_site_id(token):
    url = f"https://graph.microsoft.com/v1.0/sites/{SP_SITE_HOSTNAME}:{SP_SITE_PATH}"

    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})

    if response.status_code != 200:
        print("❌ Site lookup error:", response.text)
        raise Exception("Failed to resolve SharePoint site")

    return response.json()["id"]


def graph_get_drive_id(token, site_id):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"

    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})

    if response.status_code != 200:
        print("❌ Drive lookup error:", response.text)
        raise Exception("Failed to list drives")

    for drive in response.json().get("value", []):
        if drive.get("name") == SP_LIBRARY_NAME:
            return drive["id"]

    raise Exception(f"❌ Drive not found: {SP_LIBRARY_NAME}")


def graph_upload_file(token, drive_id, local_path, target_name):
    print(f"⬆️ Uploading {target_name} to SharePoint...")

    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{target_name}:/content"

    with open(local_path, "rb") as f:
        response = requests.put(
            url,
            headers={"Authorization": f"Bearer {token}"},
            data=f
        )

    if response.status_code not in (200, 201):
        print("❌ Upload error:", response.text)
        raise Exception("Graph upload failed")

    print(f"✅ Uploaded: {target_name}")


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("🚀 Starting Jira → SharePoint export")

    # Step 1: Fetch Jira data
    issues = fetch_jira_issues(JIRA_QUERY)

    # Step 2: Convert to DataFrame
    df = issues_to_dataframe(issues)

    if df.empty:
        print("⚠️ WARNING: No data for current week")

    # Step 3: Save CSV
    csv_name = "jira_current_week.csv"
    df.to_csv(csv_name, index=False)
    print(f"💾 Saved: {csv_name}")

    # Step 4: Upload to SharePoint
    token = get_graph_token()
    site_id = graph_get_site_id(token)
    drive_id = graph_get_drive_id(token, site_id)

    graph_upload_file(token, drive_id, csv_name, csv_name)

    print("🎉 DONE")
