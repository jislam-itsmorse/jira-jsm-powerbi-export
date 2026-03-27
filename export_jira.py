import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential


# ==============================
# REQUIRED ENV VARS (from GitHub Actions secrets)
# ==============================
JIRA_BASE_URL = os.environ.get("JIRA_BASE_URL")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")

SP_SITE_URL = os.environ.get("SP_SITE_URL")
SP_CLIENT_ID = os.environ.get("SP_CLIENT_ID")
SP_CLIENT_SECRET = os.environ.get("SP_CLIENT_SECRET")

# IMPORTANT: server-relative URL to your library (internal urlName)
# Example: /sites/Morse-helpdesk/jirapowerbidata
SP_LIBRARY_RELATIVE_URL = os.environ.get("SP_LIBRARY_RELATIVE_URL")

# Optional override so you can change JQL without changing code
# Example: project = ISD ORDER BY created DESC
JIRA_JQL = os.environ.get("JIRA_JQL", "project = ISD ORDER BY created DESC")


# ==============================
# VALIDATE ENV VARS EARLY
# ==============================
required = {
    "JIRA_BASE_URL": JIRA_BASE_URL,
    "JIRA_EMAIL": JIRA_EMAIL,
    "JIRA_API_TOKEN": JIRA_API_TOKEN,
    "SP_SITE_URL": SP_SITE_URL,
    "SP_CLIENT_ID": SP_CLIENT_ID,
    "SP_CLIENT_SECRET": SP_CLIENT_SECRET,
    "SP_LIBRARY_RELATIVE_URL": SP_LIBRARY_RELATIVE_URL,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise Exception(f"Missing required environment variables: {missing}")


# ==============================
# JIRA SETTINGS (JQL SEARCH)
# /rest/api/3/search/jql with nextPageToken pagination
# ==============================
JIRA_HEADERS = {"Accept": "application/json"}

FIELDS = [
    "summary",
    "status",
    "created",
    "resolutiondate",
    "assignee",
    "issuetype",
]


def fetch_jira_issues():
    """
    Jira Cloud JQL search:
    GET /rest/api/3/search/jql
    Pagination uses nextPageToken (not startAt).
    """
    print("Starting Jira export using /rest/api/3/search/jql (nextPageToken pagination)...")

    all_issues = []
    next_page_token = None
    max_results = 100

    while True:
        params = {
            "jql": JIRA_JQL,
            "maxResults": max_results,
            "fields": ",".join(FIELDS),
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        r = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers=JIRA_HEADERS,
            params=params,
        )

        print(f"Jira HTTP: {r.status_code}")
        if r.status_code != 200:
            print("Jira API error response:")
            print(r.text)
            raise Exception("Jira API call failed")

        data = r.json()

        issues = data.get("issues")
        if issues is None:
            print("Unexpected Jira response (no 'issues'):")
            print(data)
            raise Exception("Jira response does not contain 'issues'")

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


def upload_csv_to_sharepoint(local_csv_path, sharepoint_filename="jira_jsm_export.csv"):
    """
    Deterministic upload using server-relative library path, e.g.
    /sites/Morse-helpdesk/jirapowerbidata
    """
    print("Uploading CSV to SharePoint (server-relative path)...")
    creds = ClientCredential(SP_CLIENT_ID, SP_CLIENT_SECRET)
    ctx = ClientContext(SP_SITE_URL).with_credentials(creds)  # supported auth pattern [3](https://www.data-traveling.com/articles/leveraging-power-bi-rest-apis-python-automation-for-dataset-refresh-and-ms-teams-notification)

    folder = ctx.web.get_folder_by_server_relative_url(SP_LIBRARY_RELATIVE_URL)

    with open(local_csv_path, "rb") as f:
        uploaded = folder.files.upload(sharepoint_filename, f.read()).execute_query()  # supported upload style [4](https://pbi-guy.com/2022/01/07/refresh-a-power-bi-dataset-with-python/)

    print("✅ Uploaded to:", uploaded.serverRelativeUrl)


if __name__ == "__main__":
    issues = fetch_jira_issues()
    df = issues_to_dataframe(issues)

    if df.empty:
        print("WARNING: Jira returned 0 issues (CSV will be empty)")
    else:
        print(f"Exporting {len(df)} rows")

    csv_name = "jira_jsm_export.csv"
    df.to_csv(csv_name, index=False)

    upload_csv_to_sharepoint(csv_name, sharepoint_filename=csv_name)
