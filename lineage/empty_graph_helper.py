

class EmptyGraphHelper(object):
    def __init__(self, platform_type):
        self._platform_type = platform_type

    def get_integration_docs_link(self):
        if self._platform_type == 'snowflake':
            return "https://docs.elementary-data.com/integrations/snowflake"
        elif self._platform_type == 'bigquery':
            return "https://docs.elementary-data.com/integrations/bigquery"
        else:
            return "https://docs.elementary-data.com/guides/usage/query-history-1"

    def get_help_message(self):
        return f"""
        We are deeply sorry but unfortunately the result graph is empty.
        Please try the following steps to fix the problem - 
        \t1. Try running again with a broader date range
        \t2. Check that you have sufficient permissions (follow this short guide here - {self.get_integration_docs_link()})
        \t3. Join our slack channel here - https://bit.ly/slack-elementary, we promise to help and be nice! 
        """
