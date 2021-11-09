from collections import defaultdict
from lineage.query_context import QueryContext


class QueryHistoryStats(object):
    def __init__(self):
        self._query_type_stats = defaultdict(lambda: 0)
        self._roles = set()
        self._users = set()

    def update_stats(self, query_context: QueryContext):
        if query_context.query_type is not None:
            self._query_type_stats[query_context.query_type] += 1

        if query_context.user_name is not None:
            self._users.add(query_context.user_name)

        if query_context.role_name is not None:
            self._roles.add(query_context.role_name)

    def to_dict(self):
        query_history_stats_dict = self._query_type_stats.copy()
        query_history_stats_dict['user_count'] = len(self._users)
        query_history_stats_dict['role_count'] = len(self._roles)
        return query_history_stats_dict
