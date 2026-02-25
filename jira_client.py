import requests
from requests.auth import HTTPBasicAuth
import time


class JiraClient:
    def __init__(self, base_url, email, api_token):
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(email, api_token)
        self.headers = {"Accept": "application/json"}

    def get_issue(self, issue_key, max_retries=3):
        url = f"{self.base_url}/rest/api/3/issue/{issue_key}"
        params = {
            "fields": "summary,description,labels,components,status,issuetype"
        }

        for attempt in range(max_retries):
            try:
                resp = requests.get(
                    url,
                    headers=self.headers,
                    auth=self.auth,
                    params=params,
                    timeout=15,
                )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    return None
                if resp.status_code == 429:
                    time.sleep(5 * (attempt + 1))
                    continue
                resp.raise_for_status()
            except requests.exceptions.RequestException:
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    raise
        return None

    @staticmethod
    def _extract_text_from_adf(node):
        """Recursively extract plain text from Atlassian Document Format."""
        if node is None:
            return ""
        if isinstance(node, str):
            return node

        parts = []
        if isinstance(node, dict):
            if node.get("type") == "text":
                parts.append(node.get("text", ""))
            for child in node.get("content", []):
                parts.append(JiraClient._extract_text_from_adf(child))
        elif isinstance(node, list):
            for item in node:
                parts.append(JiraClient._extract_text_from_adf(item))

        return " ".join(filter(None, parts))

    def get_issue_details(self, issue_key):
        """Fetch and return structured ticket data."""
        issue = self.get_issue(issue_key)
        if issue is None:
            return None

        fields = issue.get("fields", {})
        desc_text = self._extract_text_from_adf(fields.get("description"))

        return {
            "key": issue_key,
            "title": fields.get("summary", ""),
            "description": desc_text[:5000],
            "labels": fields.get("labels", []),
            "components": [c.get("name", "") for c in fields.get("components", [])],
            "status": fields.get("status", {}).get("name", ""),
            "issue_type": fields.get("issuetype", {}).get("name", ""),
        }
