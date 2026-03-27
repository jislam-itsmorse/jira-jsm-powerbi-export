import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

# ==============================
# ENVIRONMENT VARIABLES
# ==============================
JIRA_BASE_URL = os.environ["JIRA_BASE_URL"]
JIRA_EMAIL = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]

SP_SITE_URL = os.environ["SP_SITE_URL"]
SP_CLIENT_ID = os.environ["SP_CLIENT_ID"]
SP_CLIENT_SECRET = os.environ["SP_CLIENT_SECRET"]

# ✅ NEW — deterministic upload target
SP_LIBRARY_RELATIVE_URL = os.environ["SP_LIBRARY_RELATIVE_URL"]

# Optional override
JQL = os.environ.get("JIRA_JQL", "project = ISD ORDER BY created DESC")

FIELDS = [
    "summary",
    "status",
    "created",
    "resolutiondate",
    "assignee",
    "issuetype",
]

# ==============================
# FETCH JIRA DATA
# ==============================
def fetch_jira():
    print("Starting Jira export (stable JQL endpoint)...")

    issues = []
    next_page_token = None

    while True:
        params = {
            "jql": JQL,
            "maxResults": 100,
            "fields": ",".join(FIELDS)
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token

        r = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers={"Accept": "application/json"},
            params=params
        )

        print("Jira HTTP:", r.status_code)
        if r.status_code != 200:
            print(r.text)
            raise Exception("Jira API failed")

        data = r.json()
        issues.extend(data.get("issues", []))
        next_page_token = data.get("nextPageToken")

        if not next_page_token:
            break

    print(f"Fetched {len(issues)} issues")
    return issues


def to_dataframe(issues):
    rows = []
    for i in issues:
        f = i.get("fields", {})
        rows.append({
            "IssueKey": i.get("key"),
            "Summary": f.get("summary"),
            "Status": (f.get("status") or {}).get("name"),
            "CreatedDate": f.get("created"),
            "ResolvedDate": f.get("resolutiondate"),
            "Assignee": (f.get("assignee") or {}).get("displayName"),
            "IssueType": (f.get("issuetype") or {}).get("name"),
        })
    return pd.DataFrame(rows)


# ==============================
# UPLOAD TO SHAREPOINT (BULLETPROOF)
# ==============================
def upload_to_sharepoint(csv_path):
    print("Uploading to SharePoint (server-relative path)...")

    creds = ClientCredential(SP_CLIENT_ID, SP_CLIENT_SECRET)
    ctx = ClientContext(SP_SITE_URL).with_credentials(creds)

    # ✅ THIS IS THE KEY FIX
    folder = ctx.web.get_folder_by_server_relative_url(
        SP_LIBRARY_RELATIVE_URL
    )

    with open(csv_path, "rb") as f:
        uploaded = folder.files.upload(
            "jira_jsm_export.csv",
            f.read()
        ).execute_query()

    print("✅ Uploaded to:", uploaded.serverRelativeUrl)


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    issues = fetch_jira()
    df = to_dataframe(issues)

    csv_name = "jira_jsm_export.csv"
    df.to_csv(csv_name, index=False)

    upload_to_sharepoint(csv_name)
