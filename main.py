from weather.weather_records import WeatherRecords

if __name__ == "__main__":
    weather_records = WeatherRecords()
    weather_records.run()
    records = weather_records.get_records()
