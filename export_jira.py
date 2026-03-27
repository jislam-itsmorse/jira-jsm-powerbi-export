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
# VALIDATE ENV VARS
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
JQL = 'project = ISD ORDER BY created DESC'

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
    "Content-Type": "application/json",
}

# ==============================
# FETCH JIRA DATA (NEW API)
# ==============================
def fetch_jira():
    print("Starting Jira export (new /search/jql API)...")

    issues = []
    start_at = 0
    max_results = 100

    while True:
        payload = {
            "jql": JQL,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": FIELDS,
        }

        response = requests.post(
            f"{JIRA_BASE_URL}/rest/api/3/search/jql",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            headers=HEADERS,
            json=payload,
        )

        print(f"HTTP {response.status_code}")

        if response.status_code != 200:
            print("Jira API error:")
            print(response.text)
            raise Exception("Jira API call failed")

        data = response.json()

        if "issues" not in data:
            print("Unexpected Jira response:")
            print(data)
            raise Exception("Jira response does not contain 'issues'")

        issues.extend(data["issues"])

        if start_at + max_results >= data.get("total", 0):
            break

        start_at += max_results

    print(f"Fetched {len(issues)} issues")

    rows = []
    for i in issues:
        f = i["fields"]
        rows.append({
            "IssueKey": i["key"],
            "Summary": f.get("summary"),
            "Status": f["status"]["name"] if f.get("status") else None,
            "CreatedDate": f.get("created"),
            "ResolvedDate": f.get("resolutiondate"),
            "Assignee": f["assignee"]["displayName"] if f.get("assignee") else None,
            "IssueType": f["issuetype"]["name"] if f.get("issuetype") else None,
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
        print("WARNING: Jira returned 0 issues")
    else:
        print(f"Exporting {len(df)} issues")

    upload_to_sharepoint(df)
