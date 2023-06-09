from weather.weather_records import WeatherRecords

if __name__ == "__main__":
    weather_records = WeatherRecords(
        upload_to_bq=True,
        dataset="weather",
        table_name="weather",
        project_id="weather-prediction-388920",
    )
    weather_records.run()

