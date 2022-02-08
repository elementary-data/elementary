from datetime import datetime
from typing import Optional, Union
import dateutil.parser

from utils.time import format_milliseconds


class QueryContext(object):
    def __init__(self, queried_database: Optional[str] = None, queried_schema: Optional[str] = None,
                 query_time: Optional[datetime] = None, query_volume: Optional[int] = None,
                 query_type: Optional[str] = None, user_name: Optional[str] = None,
                 role_name: Optional[str] = None, referenced_tables: [dict, list] = None,
                 destination_table: [dict, str] = None, duration: int = None,
                 query_id: str = None) -> None:
        self.queried_database = queried_database
        self.queried_schema = queried_schema
        self.query_time = query_time
        self.query_volume = query_volume
        self.query_type = query_type
        self.user_name = user_name
        self.role_name = role_name
        self.referenced_tables = referenced_tables if referenced_tables is not None else []
        self.destination_table = destination_table
        self.duration = duration
        self.query_id = query_id

    def to_dict(self) -> dict:
        return {'queried_database': self.queried_database,
                'queried_schema': self.queried_schema,
                'query_time': self._query_time_to_str(self.query_time),
                'query_volume': self.query_volume,
                'query_type': self.query_type,
                'user_name': self.user_name,
                'role_name': self.role_name,
                'referenced_tables': self.referenced_tables,
                'destination_table': self.destination_table,
                'duration': self.duration,
                'query_id': self.query_id}

    @staticmethod
    def _query_time_to_str(query_time: Optional[datetime], fmt: str = None) -> Optional[str]:
        if query_time is None:
            return None

        if fmt is None:
            return query_time.isoformat()

        return query_time.strftime(fmt)

    @staticmethod
    def _html_param_with_default(param: Union[str, int], default: Union[str, int] = 'Unknown') -> Union[str, int]:
        return default if param is None else param

    def to_html(self) -> str:
        query_type = self._html_param_with_default(self.query_type)
        user_name = self._html_param_with_default(self.user_name)
        role_name = self._html_param_with_default(self.role_name)
        query_time = self._query_time_to_str(self.query_time, fmt='%Y-%m-%d %H:%M:%S')
        query_volume = self._html_param_with_default(self.query_volume, 0)
        query_id = self._html_param_with_default(self.query_id)
        volume_color = "DarkSlateGrey"
        if query_volume == 0:
            volume_color = "tomato"
        is_view = 'view' in query_type.lower()
        query_duration = 'Unknown'
        if self.duration is not None:
            query_duration = format_milliseconds(self.duration)

        if is_view:
            return f"""
                    <html>
                        <body>
                            <div style="font-family:arial;color:DarkSlateGrey;font-size:110%;">
                                <strong>
                                    Last update</br>
                                </strong>
                                <div style="min-width:62px;display:inline-block">Type:</div> {query_type}</br>
                                <div style="min-width:62px;display:inline-block">Role:</div> {role_name}</br>
                                <div style="min-width:62px;display:inline-block">Time:</div> {query_time}</br>
                            </div>
                        </body>
                    </html>
            """

        return f"""
                    <html>
                        <body>
                            <div style="font-family:arial;color:DarkSlateGrey;font-size:110%;">
                                <strong>
                                    Last update</br>
                                </strong>
                                <div style="min-width:66px;display:inline-block">Type:</div> {query_type}</br>
                                <div style="min-width:66px;display:inline-block">User:</div> {user_name}</br>
                                <div style="min-width:66px;display:inline-block">Role:</div> {role_name}</br>
                                <div style="min-width:66px;display:inline-block">Time:</div> {query_time}</br>
                                <div style="min-width:66px;display:inline-block">Duration:</div> {query_duration}</br>
                                <div style="min-width:66px;display:inline-block;">Volume:</div> <a style="color:{volume_color}">{query_volume} rows</a></br>
                                <div style="min-width:66px;display:inline-block">Query ID:</div> {query_id}</br>
                            </div>
                        </body>
                    </html>
        """

    @staticmethod
    def from_dict(query_context_dict: dict) -> 'QueryContext':
        if 'query_time' in query_context_dict and query_context_dict['query_time'] is not None:
            query_context_dict['query_time'] = dateutil.parser.parse(query_context_dict['query_time'])
        return QueryContext(**query_context_dict)
