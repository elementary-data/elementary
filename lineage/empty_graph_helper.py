class EmptyGraphHelper(object):
    @staticmethod
    def get_help_message():
        return f"""
        We are deeply sorry but unfortunately the result graph is empty.
        Please try the following steps to fix the problem - 
        \t1. Try running again with other / no filters
        \t2. Make sure to run 'edr lineage generate' command first
        \t3. Join our slack channel here - https://bit.ly/slack-elementary, we promise to help and be nice! 
        """
