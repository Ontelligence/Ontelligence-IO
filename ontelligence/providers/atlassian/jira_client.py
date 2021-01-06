import os
from typing import Optional

from ontelligence.providers.atlassian.base import BaseJiraProvider

from pprint import PrettyPrinter
p = PrettyPrinter(indent=4)


class JIRA(BaseJiraProvider):

    project = None
    epic_custom_field = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)

    def get_user_by_email(self, email, include_active=True, include_inactive=False):
        params = {
            'query': email,
            'includeActive': include_active,
            'includeInactive': include_inactive}
        resource = self.get_conn()._get_json('user/search', params=params, base=self.get_conn().JIRA_BASE_URL)
        return resource[0] if resource else None

    def create_issue(
            self,
            summary: str,
            description: str,
            issue_type: str,
            project: Optional[str] = None,
            assignee: Optional[str] = None,
            status: Optional[str] = None,
            epic: Optional[str] = None,
            attachment: Optional[str] = None
    ):
        project = project or self.project
        if not project:
            raise Exception('Please provide a "project"')

        if issue_type not in ['Task', 'Story', 'Bug']:
            raise Exception('The "issue_type" value is not valid')

        fields = {
            'project': project,
            'summary': summary,
            'description': description,
            'issuetype': {'name': issue_type}
        }
        issue = self.get_conn().create_issue(fields=fields)

        if attachment:
            file_name = os.path.split(attachment)[1]
            self.get_conn().add_attachment(issue=issue.key, attachment=attachment, filename=file_name)

        if epic:
            if not self.epic_custom_field:
                raise Exception('The epic "custom_field" value is not defined')
            issue.update(fields={self.epic_custom_field: epic})

        if assignee:
            if '@' in assignee:
                user = self.get_user_by_email(email=assignee)
                if user:
                    assignee = user['accountId']
            issue.update(fields={'assignee': {'accountId': assignee}})

        if status:
            transitions = self.get_conn().transitions(issue.key)
            transition_id = [x['id'] for x in transitions if x['to']['name'] == status]
            if transition_id:
                self.get_conn().transition_issue(issue, transition_id[0])
