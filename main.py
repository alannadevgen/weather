import os

from dotenv import load_dotenv

from weather.weather_records import WeatherRecords

if __name__ == "__main__":
    load_dotenv()
    weather_records = WeatherRecords(
        upload_to_bq=True,
        dataset=os.environ.get("DATASET"),
        table_name=os.environ.get("TABLE_NAME"),
        project_id=os.environ.get("PROJECT_ID"),
    )
    weather_records.run()
