import pandas as pd
from api import get_records


def process_records(records: list):
    day_records = []
    variables = [
        "forecast",
        "timestamp",
        "code_commune",
        "commune",
        "position",
        "2_metre_relative_humidity",
        "2_metre_temperature",
        "maximum_temperature_at_2_metres",
        "minimum_temperature_at_2_metres",
        "total_precipitation",
        "10m_wind_speed",
    ]
    for record in records:
        hour_record = {}
        for variable in variables:
            if variable in record["fields"]:
                hour_record[variable] = record["fields"][variable]
        day_records.append(hour_record)
    return day_records


def records_to_dataframe(records):
    return pd.DataFrame.from_records(data=records)


if __name__ == "__main__":
    raw_records = get_records()
    raw_records = process_records(records=raw_records)
    print(records_to_dataframe(records=raw_records))
