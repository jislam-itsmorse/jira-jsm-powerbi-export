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
# Keep this simple first. Once it works, you can add more filters.
JQL = 'project = ISD ORDER BY created DESC'

# Fields you want back (key is always included in issue objects)
FIELDS = [
    "summary",
    "status",
    "created",
    "resolutiondate",
    "assignee",
    "issuetype",
]

HEADERS = {
    "Accept": "application/json",
}

# ==============================
# FETCH JIRA DATA (NEW JQL SEARCH)
# - Uses GET /rest/api/3/search/jql
# - Pagination uses nextPageToken (NOT startAt)
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
            # For GET /search/jql, fields are passed as a query parameter.
            # Using comma-separated list is the common pattern for Jira search endpoints.
            "fields": ",".join(FIELDS),
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        resp = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers=HEADERS,
            params=params,
        )

        print(f"HTTP {resp.status_code}")
        if resp.status_code != 200:
            print("Jira API error response:")
            print(resp.text)
            raise Exception("Jira API call failed")

        data = resp.json()

        # Defensive checks: new endpoint still returns issues, but never assume.
        batch = data.get("issues")
        if batch is None:
            print("Unexpected Jira response (no 'issues' key):")
            print(data)
            raise Exception("Jira response does not contain 'issues'")

        issues.extend(batch)

        # New pagination mechanism
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    print(f"Fetched {len(issues)} issues")

    rows = []
    for issue in issues:
        f = issue.get("fields", {})
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
# ==============================
def upload_to_sharepoint(df):
    print("Uploading CSV to SharePoint...")

    csv_name = "jira_jsm_export.csv"
    df.to_csv(csv_name, index=False)

    ctx = ClientContext(
        SP_SITE_URL,
        ClientCredential(SP_CLIENT_ID, SP_CLIENT_SECRET)
    )

    with open(csv_name, "rb") as content:
        ctx.web.lists.get_by_title(SP_LIBRARY) \
            .root_folder \
            .upload_file(csv_name, content.read()) \
            .execute_query()

    print("Upload completed successfully")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    df = fetch_jira()
    if df.empty:
        print("WARNING: Jira returned 0 issues (CSV will be empty)")
    else:
        print(f"Exporting {len(df)} rows to CSV")

    upload_to_sharepoint(df)
