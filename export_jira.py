import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

# ==============================
# ENVIRONMENT VARIABLES
# ==============================
JIRA_BASE_URL = os.environ.get("JIRA_BASE_URL")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")

SP_SITE_URL = os.environ.get("SP_SITE_URL")
SP_CLIENT_ID = os.environ.get("SP_CLIENT_ID")
SP_CLIENT_SECRET = os.environ.get("SP_CLIENT_SECRET")
SP_LIBRARY = os.environ.get("SP_LIBRARY")

# Optional: override JQL from GitHub Secrets/Env if you want
# Example value: project = ISD ORDER BY created DESC
JIRA_JQL_OVERRIDE = os.environ.get("JIRA_JQL")

# ==============================
# VALIDATE ENV VARS EARLY
# ==============================
required_vars = {
    "JIRA_BASE_URL": JIRA_BASE_URL,
    "JIRA_EMAIL": JIRA_EMAIL,
    "JIRA_API_TOKEN": JIRA_API_TOKEN,
    "SP_SITE_URL": SP_SITE_URL,
    "SP_CLIENT_ID": SP_CLIENT_ID,
    "SP_CLIENT_SECRET": SP_CLIENT_SECRET,
    "SP_LIBRARY": SP_LIBRARY,
}
missing = [k for k, v in required_vars.items() if not v]
if missing:
    raise Exception(f"Missing required environment variables: {missing}")

# ==============================
# JIRA SETTINGS
# ==============================
# Keep this simple first. If Jira returns 0, verify this same JQL in Jira UI.
JQL = JIRA_JQL_OVERRIDE or 'project = ISD ORDER BY created DESC'

# Fields to return
FIELDS = [
    "summary",
    "status",
    "created",
    "resolutiondate",
    "assignee",
    "issuetype",
]

JIRA_HEADERS = {"Accept": "application/json"}


# ==============================
# FETCH JIRA DATA (NEW JQL SEARCH)
# Endpoint: GET /rest/api/3/search/jql
# Pagination: nextPageToken (NOT startAt)
# ==============================
def fetch_jira():
    print("Starting Jira export using /rest/api/3/search/jql (nextPageToken pagination)...")

    issues = []
    next_page_token = None
    max_results = 100

    while True:
        params = {
            "jql": JQL,
            "maxResults": max_results,
            "fields": ",".join(FIELDS),
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        resp = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers=JIRA_HEADERS,
            params=params,
        )

        print(f"HTTP {resp.status_code}")

        if resp.status_code != 200:
            print("Jira API error response:")
            print(resp.text)
            raise Exception("Jira API call failed")

        data = resp.json()

        batch = data.get("issues")
        if batch is None:
            print("Unexpected Jira response (no 'issues' key):")
            print(data)
            raise Exception("Jira response does not contain 'issues'")

        issues.extend(batch)

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    print(f"Fetched {len(issues)} issues")
    return issues


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
# UPLOAD TO SHAREPOINT
# Fixes: ClientCredential url attribute error by using with_credentials()
# Auth pattern: ClientContext(site_url).with_credentials(ClientCredential(...))
# Upload pattern: folder.files.upload(f).execute_query()
# ==============================
def upload_to_sharepoint(csv_path, target_filename="jira_jsm_export.csv"):
    print("Uploading CSV to SharePoint...")

    client_credentials = ClientCredential(SP_CLIENT_ID, SP_CLIENT_SECRET)
