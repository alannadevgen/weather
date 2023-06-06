from weather.weather_records import WeatherRecords
from google.oauth2 import service_account


if __name__ == "__main__":
    credentials = service_account.Credentials.from_service_account_file("credentials.json")
    print(credentials)

    # weather_records = WeatherRecords()
    # weather_records.run()
    # records = weather_records.get_records()
    # print(records)
    # records.to_gbq(
    #     destination_table="weather.weather",
    #     project_id="weather-prediction-388920",
    #     if_exists="append",
    #     credentials=credentials
    # )
    records.to_csv("weather_records.csv")
