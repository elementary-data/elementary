from datetime import datetime
from typing import Optional
import dateutil.parser


class QueryContext(object):
    def __init__(self, queried_database: str, queried_schema: str, query_time: Optional[datetime] = None,
                 query_volume: Optional[int] = None, query_type: Optional[str] = None):
        self.queried_database = queried_database
        self.queried_schema = queried_schema
        self.query_time = query_time
        self.query_volume = query_volume
        self.query_type = query_type

    def to_dict(self):
        query_time_str = self.query_time.isoformat() if self.query_time is not None else None
        return {'queried_database': self.queried_database,
                'queried_schema': self.queried_schema,
                'query_time': query_time_str,
                'query_volume': self.query_volume,
                'query_type': self.query_type}

    def to_html(self):
        if self.query_volume is not None and self.query_volume > 0:
            volume_color = "DarkSlateGrey"
            query_volume = self.query_volume
        else:
            volume_color = "tomato"
            query_volume = 0

        query_type = 'unknown' if self.query_type is None else self.query_type
        query_time = self.query_time.strftime('%Y-%m-%d %H:%M:%S')

        return f"""
                    <html>
                        <body>
                            <div style="font-family:arial;color:DarkSlateGrey;font-size:110%;">
                                <strong>
                                    Last update
                                </strong>
                                <ul>    
                                    <li>Type: {query_type}</li>
                                    <li>Time: {query_time}</li>
                                    <li>Volume: <a style="color:{volume_color};">{query_volume} rows</a></li>
                                </ul>
                            </div>
                        </body>
                    </html>
        """

    @staticmethod
    def from_dict(query_context_dict):
        if 'query_time' in query_context_dict and query_context_dict['query_time'] is not None:
            query_context_dict['query_time'] = dateutil.parser.parse(query_context_dict['query_time'])
        return QueryContext(**query_context_dict)
