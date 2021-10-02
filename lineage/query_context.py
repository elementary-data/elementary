from datetime import datetime
from typing import Optional
import dateutil.parser


class QueryContext(object):
    def __init__(self, queried_database: str, queried_schema: str, query_time: Optional[datetime] = None,
                 query_volume: Optional[int] = None, query_type: Optional[str] = None, user_name: Optional[str] = None,
                 role_name: Optional[str] = None):
        self.queried_database = queried_database
        self.queried_schema = queried_schema
        self.query_time = query_time
        self.query_volume = query_volume
        self.query_type = query_type
        self.user_name = user_name
        self.role_name = role_name

    def to_dict(self):
        query_time_str = self.query_time.isoformat() if self.query_time is not None else None
        return {'queried_database': self.queried_database,
                'queried_schema': self.queried_schema,
                'query_time': query_time_str,
                'query_volume': self.query_volume,
                'query_type': self.query_type,
                'user_name': self.user_name,
                'role_name': self.role_name}

    @staticmethod
    def _html_param_with_default(param, default='unknown'):
        return default if param is None else param

    def to_html(self):
        query_type = self._html_param_with_default(self.query_type)
        user_name = self._html_param_with_default(self.user_name)
        role_name = self._html_param_with_default(self.role_name)
        query_time = self.query_time.strftime('%Y-%m-%d %H:%M:%S')

        if self.query_volume is not None and self.query_volume > 0:
            volume_color = "DarkSlateGrey"
            query_volume = self.query_volume
        else:
            volume_color = "tomato"
            query_volume = 0

        return f"""
                    <html>
                        <body>
                            <div style="font-family:arial;color:DarkSlateGrey;font-size:110%;">
                                <strong>
                                    Last update</br>
                                </strong>
                                <div style="min-width:62px;display:inline-block">Type:</div> {query_type}</br>
                                <div style="min-width:62px;display:inline-block">User:</div> {user_name}</br>
                                <div style="min-width:62px;display:inline-block">Role:</div> {role_name}</br>
                                <div style="min-width:62px;display:inline-block">Time:</div> {query_time}</br>
                                <div style="min-width:62px;display:inline-block;">Volume:</div> <a style="color:{volume_color}">{query_volume} rows</a>
                            </div>
                        </body>
                    </html>
        """

    @staticmethod
    def from_dict(query_context_dict):
        if 'query_time' in query_context_dict and query_context_dict['query_time'] is not None:
            query_context_dict['query_time'] = dateutil.parser.parse(query_context_dict['query_time'])
        return QueryContext(**query_context_dict)
