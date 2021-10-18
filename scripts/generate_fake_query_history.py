from lineage.query_context import QueryContext
from datetime import datetime, timedelta
from random import seed
from random import randint
import json
from lineage.query_history import QueryHistory
from typing import Optional
import pathlib
import os

from lineage.snowflake_query import SnowflakeQuery

seed(1)


def get_random_date(start_date: datetime, end_date: datetime, specific_day: Optional[int] = None) -> datetime:
    if specific_day is None:
        day_diff = (end_date - start_date).days
        days_to_add = randint(0, day_diff)
    else:
        days_to_add = specific_day

    return datetime(start_date.year, start_date.month, start_date.day + days_to_add, start_date.hour,
                    randint(0, 59), randint(0, 59))


def generate_query_context(start_date: datetime, end_date: datetime, query_type: str, day: Optional[int] = None,
                           introduce_volume_bug: bool = False, user_name: str = 'elementary_dbt',
                           role_name: str = 'elementary_role') -> \
        QueryContext:
    if day is None:
        query_time = get_random_date(start_date, end_date)
    else:
        query_time = get_random_date(start_date, end_date, specific_day=day)

    if introduce_volume_bug:
        query_volume = randint(0, 100)
    else:
        query_volume = randint(500, 1000)

    return QueryContext('analytics', 'elementary', query_time, query_volume, query_type, user_name,
                        role_name)


def generate_query(query_text: str, start_date: datetime, end_date: datetime, day: Optional[int] = None,
                   introduce_bug: bool = False, query_type: str = "INSERT", user_name: str = 'elementary_dbt',
                   role_name: str = 'elementary_role'):
    return SnowflakeQuery(query_text, generate_query_context(start_date, end_date, query_type, day, introduce_bug,
                                                             user_name, role_name),
                          'analytics', 'elementary')


def generate_queries(start_date: datetime, end_date: datetime) -> [tuple]:
    number_of_days = (end_date - start_date).days
    queries = []
    for day in range(number_of_days):
        introduce_bug = False
        # if it's last day, insert a bug
        if (start_date + timedelta(days=day)).date() == (end_date - timedelta(days=1)).date():
            introduce_bug = True

        # Insert queries
        queries.append(generate_query("insert into salesforce_info select * from salesforce_info_stg",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into sales_contracts select * from salesforce_info",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into sales_reps select * from salesforce_info",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into sales_leads select * from salesforce_info",
                                      start_date, end_date, day, introduce_bug).to_dict())
        queries.append(generate_query("insert into sales_demos select * from salesforce_info",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into demo_conversion_rate select * from sales_demos join sales_leads on "
                                      "sales_demos.lead_id = leads.id",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into pipeline_per_rep select * from sales_reps join sales_leads on "
                                      "sales_leads.rep_id = sales_reps.id",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into closed_won_per_rep select * from sales_reps join sales_contracts on "
                                      "sales_contracts.rep_id = sales_reps.id",
                                      start_date, end_date, day).to_dict())

        queries.append(generate_query("insert into facebook_marketing_spend select * from facebook_marketing_spend_stg",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into google_ads_marketing_spend select * from "
                                      "google_ads_marketing_spend_stg",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into marketing_attribution select * from marketing_attribution_stg",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into marketing_unified_view select * from marketing_attribution join "
                                      "facebook_marketing_spend on marketing_attribution.campaign = "
                                      "facebook_marketing_spend.campaign "
                                      "join google_ads_marketing_spend on marketing_attribution.campaign = "
                                      "google_ads_marketing_spend.campaign",
                                      start_date, end_date, day, introduce_bug).to_dict())
        queries.append(generate_query("insert into marketing_qualified_leads select * from marketing_unified_view join "
                                      "sales_leads on marketing_unified_view.anonymous_id = sales_leads.anonymous_id",
                                      start_date, end_date, day).to_dict())
        queries.append(generate_query("insert into pipeline_per_stage select * from marketing_unified_view join "
                                      "sales_leads on marketing_unified_view.anonymous_id = sales_leads.anonymous_id",
                                      start_date, end_date, day).to_dict())

        queries.append(generate_query("select * from marketing_qualified_leads", start_date, end_date, day,
                                      query_type='SELECT', user_name='tableau', role_name='tableau_service').to_dict())
        queries.append(generate_query("select * from demo_conversion_rate", start_date, end_date, day,
                                      query_type='SELECT', user_name='tableau', role_name='tableau_service').to_dict())
        queries.append(generate_query("select * from closed_won_per_rep", start_date, end_date, day,
                                      query_type='SELECT', user_name='tableau', role_name='tableau_service').to_dict())
        queries.append(generate_query("select * from pipeline_per_stage", start_date, end_date, day,
                                      query_type='SELECT', user_name='tableau', role_name='tableau_service').to_dict())

    return queries


def main():
    start_date = datetime(2021, 9, 1, 1, 33, 7)
    end_date = datetime(2021, 9, 30, 2, 33, 7)
    queries = generate_queries(start_date, end_date)
    parent_dir = pathlib.Path(__file__).parent
    with open(os.path.join(parent_dir, '..', 'lineage', QueryHistory.LATEST_QUERY_HISTORY_FILE), 'w') as \
            query_history_file:
        json.dump(queries, query_history_file)


if __name__ == '__main__':
    main()