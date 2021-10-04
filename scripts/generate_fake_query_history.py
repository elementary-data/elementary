from lineage.query_context import QueryContext
from datetime import datetime, timedelta
from random import seed
from random import randint
import json
from lineage.query_history import QueryHistory
from typing import Optional

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
                           introduce_volume_bug: bool = False) -> \
        QueryContext:
    if day is None:
        query_time = get_random_date(start_date, end_date)
    else:
        query_time = get_random_date(start_date, end_date, specific_day=day)

    if introduce_volume_bug:
        query_volume = randint(0, 100)
    else:
        query_volume = randint(500, 1000)

    return QueryContext('elementary_db', 'elementary', query_time, query_volume, query_type, 'elementary_dbt',
                        'elementary_role')


def generate_queries(start_date: datetime, end_date: datetime) -> [tuple]:
    number_of_days = (end_date - start_date).days
    queries = []
    for day in range(number_of_days):
        introduce_bug = False
        # if it's last day, insert a bug
        if (start_date + timedelta(days=day)).date() == (end_date - timedelta(days=1)).date():
            introduce_bug = True

        queries.append(("insert into salesforce_info select * from salesforce_info_stg",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into sales_contracts select * from salesforce_info",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into sales_reps select * from salesforce_info",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into sales_leads select * from salesforce_info",
                        generate_query_context(start_date, end_date, 'INSERT', day, introduce_bug).to_dict()))
        queries.append(("insert into sales_demos select * from salesforce_info",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into demo_conversion_rate select * from sales_demos join sales_leads on "
                        "sales_demos.lead_id = leads.id",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into pipeline_per_rep select * from sales_reps join sales_leads on "
                        "sales_leads.rep_id = sales_reps.id",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into closed_won_per_rep select * from sales_reps join sales_contracts on "
                        "sales_contracts.rep_id = sales_reps.id",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))

        queries.append(("insert into facebook_marketing_spend select * from facebook_marketing_spend_stg",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into google_ads_marketing_spend select * from google_ads_marketing_spend_stg",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into marketing_attribution select * from marketing_attribution_stg",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into marketing_unified_view select * from marketing_attribution join "
                        "facebook_marketing_spend on marketing_attribution.campaign = "
                        "facebook_marketing_spend.campaign "
                        "join google_ads_marketing_spend on marketing_attribution.campaign = "
                        "google_ads_marketing_spend.campaign",
                        generate_query_context(start_date, end_date, 'INSERT', day, introduce_bug).to_dict()))
        queries.append(("insert into marketing_qualified_leads select * from marketing_unified_view join sales_leads "
                        "on marketing_unified_view.anonymous_id = sales_leads.anonymous_id",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))
        queries.append(("insert into pipeline_per_stage select * from marketing_unified_view join sales_leads "
                        "on marketing_unified_view.anonymous_id = sales_leads.anonymous_id",
                        generate_query_context(start_date, end_date, 'INSERT', day).to_dict()))

    return queries


def main():
    start_date = datetime(2021, 9, 1, 1, 33, 7)
    end_date = datetime(2021, 9, 30, 2, 33, 7)
    queries = generate_queries(start_date, end_date)
    with open(f'{QueryHistory.LATEST_QUERY_HISTORY_FILE}', 'w') as query_history_file:
        json.dump(queries, query_history_file)


if __name__ == '__main__':
    main()