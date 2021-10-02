from datetime import datetime
from typing import Optional
import dateutil.parser


class QueryContext(object):
    def __init__(self, queried_database: str, queried_schema: str, query_time: Optional[datetime] = None,
                 query_volume: Optional[int] = None):
        self.queried_database = queried_database
        self.queried_schema = queried_schema
        self.query_time = query_time
        self.query_volume = query_volume

    def to_dict(self):
        query_time_str = self.query_time.isoformat() if self.query_time is not None else None
        return {'queried_database': self.queried_database,
                'queried_schema': self.queried_schema,
                'query_time': query_time_str,
                'query_volume': self.query_volume}

    def to_html(self):
        if self.query_volume is not None and self.query_volume > 0:
            volume_color = "RoyalBlue"
            query_volume = self.query_volume
        else:
            volume_color = "tomato"
            query_volume = 0

        return f"""
                    <html>
                        <body>
                            <a style=font-family:arial;color:RoyalBlue;">    
                                Last update time: {self.query_time.strftime('%Y-%m-%d %H:%M:%S')}</br>
                                Last update volume: <vol style="color:{volume_color};">{query_volume} rows</vol>
                            </a>
                        </body>
                    </html>
        """

    @staticmethod
    def from_dict(query_context_dict):
        if 'query_time' in query_context_dict and query_context_dict['query_time'] is not None:
            query_context_dict['query_time'] = dateutil.parser.parse(query_context_dict['query_time'])
        return QueryContext(**query_context_dict)
