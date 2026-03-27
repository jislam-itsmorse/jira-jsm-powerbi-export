import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth


# ==============================
# JIRA ENV VARS
# ==============================
JIRA_BASE_URL = os.environ["JIRA_BASE_URL"]
JIRA_EMAIL = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]
JIRA_JQL = os.environ.get(
    "JIRA_JQL",
    """
    project = ISD
    AND (
        created >= startOfWeek()
        OR
        resolutiondate >= startOfWeek()
    )
    ORDER BY created ASC
    """
)


FIELDS = ["summary", "status", "created", "resolutiondate", "assignee", "issuetype"]


# ==============================
# SHAREPOINT (GRAPH) ENV VARS
# ==============================
SP_TENANT_ID = os.environ["SP_TENANT_ID"]
SP_CLIENT_ID = os.environ["SP_CLIENT_ID"]
SP_CLIENT_SECRET = os.environ["SP_CLIENT_SECRET"]

SP_SITE_HOSTNAME = os.environ["SP_SITE_HOSTNAME"]     # e.g. itsmorse.sharepoint.com
SP_SITE_PATH = os.environ["SP_SITE_PATH"]             # e.g. /sites/Morse-helpdesk
SP_LIBRARY_NAME = os.environ["SP_LIBRARY_NAME"]       # e.g. jira-powerbi-data


# ==============================
# JIRA: Fetch issues (JQL search endpoint)
# ==============================
def fetch_jira_issues():
    print("Starting Jira export using /rest/api/3/search/jql (nextPageToken pagination)...")

    all_issues = []
    next_page_token = None

    while True:
        params = {
            "jql": JIRA_JQL,
            "maxResults": 100,
            "fields": ",".join(FIELDS),
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        r = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Accept": "application/json"},
            params=params,
        )

        print(f"Jira HTTP: {r.status_code}")
        if r.status_code != 200:
            print(r.text)
            raise Exception("Jira API call failed")

        data = r.json()
        issues = data.get("issues", [])
        all_issues.extend(issues)

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    print(f"Fetched {len(all_issues)} issues")
    return all_issues


def issues_to_dataframe(issues):
    rows = []
    for issue in issues:
        f = issue.get("fields", {}) or {}
        rows.append({
            "IssueKey": issue.get("key"),
            "Summary": f.get("summary"),
            "Status": (f.get("status") or {}).get("name"),
            "CreatedDate": f.get("created"),
            "ResolvedDate": f.get("resolutiondate"),
            "Assignee": (f.get("assignee") or {}).get("displayName"),
            "IssueType": (f.get("issuetype") or {}).get("name"),
        })
    return pd.DataFrame(rows)


# ==============================
# GRAPH AUTH
# ==============================
def get_graph_token():
    token_url = f"https://login.microsoftonline.com/{SP_TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": SP_CLIENT_ID,
        "client_secret": SP_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    r = requests.post(token_url, data=data)
    if r.status_code != 200:
        print("Token error:", r.text)
        raise Exception("Failed to get Graph token")
    return r.json()["access_token"]


def graph_get_site_id(token):
    # GET /sites/{hostname}:{server-relative-path}
    url = f"https://graph.microsoft.com/v1.0/sites/{SP_SITE_HOSTNAME}:{SP_SITE_PATH}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if r.status_code != 200:
        print("Site lookup error:", r.text)
        raise Exception("Failed to resolve SharePoint site via Graph")
    return r.json()["id"]


def graph_get_drive_id(token, site_id):
    # List drives (document libraries) and pick the one matching SP_LIBRARY_NAME
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if r.status_code != 200:
        print("Drives lookup error:", r.text)
        raise Exception("Failed to list drives for site")

    drives = r.json().get("value", [])
    for d in drives:
        if d.get("name") == SP_LIBRARY_NAME:
            return d["id"]

    # If not found, print what we saw for debugging
    print("Available drives:", [d.get("name") for d in drives])
    raise Exception(f"Drive/library not found: {SP_LIBRARY_NAME}")


def graph_upload_file(token, drive_id, local_path, target_name):
    # PUT /drives/{drive-id}/root:/{filename}:/content
    upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{target_name}:/content"
    with open(local_path, "rb") as f:
        r = requests.put(
            upload_url,
            headers={"Authorization": f"Bearer {token}"},
            data=f
        )

    if r.status_code not in (200, 201):
        print("Upload error:", r.text)
        raise Exception("Graph upload failed")

    uploaded = r.json()
    print("✅ Uploaded to:", uploaded.get("webUrl"))


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    issues = fetch_jira_issues()
    df = issues_to_dataframe(issues)

    if df.empty:
        print("WARNING: Jira returned 0 issues (CSV will be empty)")
    else:
        print(f"Exporting {len(df)} rows")

    csv_name = "jira_jsm_export.csv"
    df.to_csv(csv_name, index=False)

    token = get_graph_token()
    site_id = graph_get_site_id(token)
    drive_id = graph_get_drive_id(token, site_id)
    graph_upload_file(token, drive_id, csv_name, csv_name)
