import os
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

# --------------------
# ENV VARIABLES
# --------------------
JIRA_BASE_URL = os.environ["JIRA_BASE_URL"]
JIRA_EMAIL = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]

SP_SITE_URL = os.environ["SP_SITE_URL"]
SP_CLIENT_ID = os.environ["SP_CLIENT_ID"]
SP_CLIENT_SECRET = os.environ["SP_CLIENT_SECRET"]
SP_LIBRARY = os.environ["SP_LIBRARY"]

# --------------------
# JIRA QUERY (JSM)
# --------------------
JQL = """
project = ISD
ORDER BY created DESC
"""

FIELDS = [
    "key",
    "summary",
    "status",
    "created",
    "resolutiondate",
    "assignee",
    "issuetype"
]

# --------------------
# FETCH JIRA DATA
# --------------------
def fetch_jira():
    issues = []
    start_at = 0

    while True:
        response = requests.get(
            f"{JIRA_BASE_URL}/rest/api/3/search",
            auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN),
            params={
                "jql": JQL,
                "startAt": start_at,
                "maxResults": 100,
                "fields": ",".join(FIELDS)
            }
        )
        data = response.json()
        issues.extend(data["issues"])

        if start_at + 100 >= data["total"]:
            break
        start_at += 100

    rows = []
    for i in issues:
        f = i["fields"]
        rows.append({
            "IssueKey": i["key"],
            "Summary": f["summary"],
            "Status": f["status"]["name"],
            "CreatedDate": f["created"],
            "ResolvedDate": f["resolutiondate"],
            "Assignee": f["assignee"]["displayName"] if f["assignee"] else None,
            "IssueType": f["issuetype"]["name"]
        })

    return pd.DataFrame(rows)

# --------------------
# UPLOAD TO SHAREPOINT
# --------------------
def upload_to_sharepoint(df):
    csv_name = "jira_jsm_export.csv"
    df.to_csv(csv_name, index=False)

    ctx = ClientContext(
        SP_SITE_URL,
        ClientCredential(SP_CLIENT_ID, SP_CLIENT_SECRET)
    )

    with open(csv_name, "rb") as content:
        ctx.web.lists.get_by_title(SP_LIBRARY).root_folder.upload_file(
            csv_name, content.read()
        ).execute_query()

# --------------------
# MAIN
# --------------------
if __name__ == "__main__":
    df = fetch_jira()
    upload_to_sharepoint(df)
