import csv
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

FILE_DIR = os.path.dirname(os.path.realpath(__file__))

EPOCH = datetime.utcfromtimestamp(0)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def generate_fake_data():
    generate_string_anomalies_training_and_validation_files()
    generate_numeric_anomalies_training_and_validation_files()
    generate_any_type_anomalies_training_and_validation_files()
    generate_dimension_anomalies_training_and_validation_files()
    generate_backfill_days_training_and_validation_files()
    generate_seasonality_volume_anomalies_files()


def generate_rows_timestamps(base_date, period="days", count=1, days_back=30):
    min_date = base_date - timedelta(days=days_back)
    dates = []
    while base_date > min_date:
        dates.append(base_date)
        base_date = base_date - timedelta(**{period: count})
    return dates


def write_rows_to_csv(csv_path, rows, header):
    # Creates the csv file directories if needed.
    directory_path = Path(csv_path).parent.resolve()
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def generate_rows(rows_count_per_day, dates, get_row_callback):
    rows = []
    for date in dates:
        for i in range(0, rows_count_per_day):
            row = get_row_callback(date, i, rows_count_per_day)
            rows.append(row)
    return rows


def generate_string_anomalies_training_and_validation_files(rows_count_per_day=100):
    def get_training_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=1)).strftime(DATE_FORMAT),
            "min_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 10))
            ),
            "max_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 10))
            ),
            "average_length": "".join(random.choices(string.ascii_lowercase, k=5)),
            "missing_count": ""
            if row_index < (3 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "missing_percent": ""
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
        }

    def get_validation_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=7)).strftime(DATE_FORMAT),
            "min_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(1, 10))
            ),
            "max_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 15))
            ),
            "average_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 8))
            ),
            "missing_count": ""
            if row_index < (20 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "missing_percent": ""
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
        }

    string_columns = [
        "updated_at",
        "occurred_at",
        "min_length",
        "max_length",
        "average_length",
        "missing_count",
        "missing_percent",
    ]
    dates = generate_rows_timestamps(base_date=EPOCH - timedelta(days=2))
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "training", "string_column_anomalies_training.csv"
        ),
        training_rows,
        string_columns,
    )

    validation_date = EPOCH - timedelta(days=1)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "string_column_anomalies_validation.csv"
        ),
        validation_rows,
        string_columns,
    )


def generate_numeric_anomalies_training_and_validation_files(rows_count_per_day=200):
    def get_training_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=1)).strftime(DATE_FORMAT),
            "min": random.randint(100, 200),
            "max": random.randint(100, 200),
            "zero_count": 0
            if row_index < (3 / 100 * rows_count)
            else random.randint(100, 200),
            "zero_percent": 0
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else random.randint(100, 200),
            "average": random.randint(99, 101),
            "standard_deviation": random.randint(99, 101),
            "variance": random.randint(99, 101),
            "sum": random.randint(100, 200),
        }

    def get_validation_row(date, row_index, rows_count):
        row_index += -(rows_count / 2)
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=7)).strftime(DATE_FORMAT),
            "min": random.randint(10, 200),
            "max": random.randint(100, 300),
            "zero_count": 0
            if row_index < (80 / 100 * rows_count)
            else random.randint(100, 200),
            "zero_percent": 0
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else random.randint(100, 200),
            "average": random.randint(101, 110),
            "standard_deviation": random.randint(80, 120),
            "variance": random.randint(80, 120),
            "sum": random.randint(300, 400),
        }

    numeric_columns = [
        "updated_at",
        "occurred_at",
        "min",
        "max",
        "zero_count",
        "zero_percent",
        "average",
        "standard_deviation",
        "variance",
        "sum",
    ]
    dates = generate_rows_timestamps(base_date=EPOCH - timedelta(days=2))
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "training", "numeric_column_anomalies_training.csv"
        ),
        training_rows,
        numeric_columns,
    )

    validation_date = EPOCH - timedelta(days=1)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "numeric_column_anomalies_validation.csv"
        ),
        validation_rows,
        numeric_columns,
    )


def generate_any_type_anomalies_training_and_validation_files(rows_count_per_day=300):
    def get_training_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=1)).strftime(DATE_FORMAT),
            "null_count_str": None
            if row_index < (3 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_percent_str": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_count_float": None
            if row_index < (3 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_percent_float": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_count_int": None
            if row_index < (3 / 100 * rows_count)
            else random.randint(100, 200),
            "null_percent_int": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else random.randint(100, 200),
            "null_count_bool": None
            if row_index < (3 / 100 * rows_count)
            else bool(random.getrandbits(1)),
            "null_percent_bool": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else bool(random.getrandbits(1)),
        }

    def get_validation_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=7)).strftime(DATE_FORMAT),
            "null_count_str": None
            if row_index < (80 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_percent_str": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_count_float": None
            if row_index < (80 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_percent_float": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_count_int": None
            if row_index < (80 / 100 * rows_count)
            else random.randint(100, 200),
            "null_percent_int": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else random.randint(100, 200),
            "null_count_bool": None
            if row_index < (80 / 100 * rows_count)
            else bool(random.getrandbits(1)),
            "null_percent_bool": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else bool(random.getrandbits(1)),
        }

    any_type_columns = [
        "updated_at",
        "occurred_at",
        "null_count_str",
        "null_percent_str",
        "null_count_float",
        "null_percent_float",
        "null_count_int",
        "null_percent_int",
        "null_count_bool",
        "null_percent_bool",
    ]
    dates = generate_rows_timestamps(
        base_date=EPOCH - timedelta(days=2), period="hours", count=4
    )
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "training", "any_type_column_anomalies_training.csv"
        ),
        training_rows,
        any_type_columns,
    )

    validation_date = EPOCH - timedelta(days=1)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "any_type_column_anomalies_validation.csv"
        ),
        validation_rows,
        any_type_columns,
    )


def generate_dimension_anomalies_training_and_validation_files():
    def get_training_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "platform": "android" if row_index < (rows_count - 20) else "ios",
            "version": row_index % 3,
            "user_id": random.randint(1, rows_count),
        }

    def get_validation_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "platform": "android" if row_index < (rows_count - 1) else "ios",
            "version": row_index % 3,
            "user_id": random.randint(1, rows_count),
        }

    dimension_columns = ["updated_at", "platform", "version", "user_id"]
    dates = generate_rows_timestamps(base_date=EPOCH - timedelta(days=2))
    training_rows = generate_rows(1020, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(FILE_DIR, "data", "training", "dimension_anomalies_training.csv"),
        training_rows,
        dimension_columns,
    )

    validation_date = EPOCH - timedelta(days=1)
    validation_rows = generate_rows(1001, [validation_date], get_validation_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "dimension_anomalies_validation.csv"
        ),
        validation_rows,
        dimension_columns,
    )


def generate_seasonal_data_files(
    table_name: str, training_dates: List[datetime], validation_dates: List[datetime]
):
    columns = ["updated_at", "user_id"]
    training_rows = []
    for date in training_dates:
        training_rows.extend(
            [
                {
                    "updated_at": date.strftime(DATE_FORMAT),
                    "user_id": random.randint(1000, 9999),
                }
                for _ in range(700)
            ]
        )
    write_rows_to_csv(
        csv_path=os.path.join(
            FILE_DIR, "data", "training", f"{table_name}_training.csv"
        ),
        rows=training_rows,
        header=columns,
    )

    validation_rows = []
    for date in validation_dates:
        validation_rows.extend(
            [
                {
                    "updated_at": date.strftime(DATE_FORMAT),
                    "user_id": random.randint(1000, 9999),
                }
                for _ in range(100)
            ]
        )
    write_rows_to_csv(
        csv_path=os.path.join(
            FILE_DIR,
            "data",
            "validation",
            f"{table_name}_validation.csv",
        ),
        rows=validation_rows,
        header=columns,
    )


def generate_day_of_week_data():
    training_dates = generate_rows_timestamps(
        base_date=EPOCH - timedelta(days=1), period="weeks", days_back=(7 * 30)
    )
    validation_dates = generate_rows_timestamps(base_date=EPOCH, days_back=1)
    generate_seasonal_data_files(
        "users_per_day_weekly_seasonal", training_dates, validation_dates
    )


def generate_hour_of_day_data():
    training_dates = generate_rows_timestamps(base_date=EPOCH - timedelta(days=1))
    validation_dates = generate_rows_timestamps(base_date=EPOCH, days_back=1)
    generate_seasonal_data_files(
        "users_per_hour_daily_seasonal", training_dates, validation_dates
    )


def generate_seasonality_volume_anomalies_files():
    generate_day_of_week_data()
    generate_hour_of_day_data()


def generate_backfill_days_training_and_validation_files(rows_count_per_day=100):
    def get_training_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=1)).strftime(DATE_FORMAT),
            "min_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 10))
            ),
        }

    def get_validation_row(date, row_index, rows_count):
        return {
            "updated_at": date.strftime(DATE_FORMAT),
            "occurred_at": (date - timedelta(hours=7)).strftime(DATE_FORMAT),
            "min_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(1, 10))
            ),
        }

    string_columns = ["updated_at", "occurred_at", "min_length"]
    dates = generate_rows_timestamps(base_date=EPOCH - timedelta(days=1))
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "training", "backfill_days_column_anomalies_training.csv"
        ),
        training_rows,
        string_columns,
    )

    validation_date = EPOCH - timedelta(days=5)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR,
            "data",
            "validation",
            "backfill_days_column_anomalies_validation.csv",
        ),
        validation_rows,
        string_columns,
    )


def main():
    print("Generating fake data!")
    generate_fake_data()
    print("Done. Please run 'dbt seed -f' to load the data into your database.")


if __name__ == "__main__":
    main()
