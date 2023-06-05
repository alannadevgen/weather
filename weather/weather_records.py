import requests
import pandas as pd


class WeatherRecords:
    def __init__(self, city: str = "Houilles"):
        """Constructor to get weather records.

        Parameters
        ----------
        city : str
            Name of the city to fetch the info.
        """
        base_url = "https://public.opendatasoft.com/api/records"
        version = "1.0"
        self.url = f"{base_url}/{version}/search/" \
                   f"?dataset=arome-0025-enriched&q={city}" \
                   f"&facet=commune&facet=code_commune" \
                   f"&refine.commune={city}"
        self.records = None

    def get_records(self):
        return self.records

    def _fetch_records(self):
        """Fetches data from Public OpenDataSoft API.

        Returns
        -------
        list
            All weather records for a given city.
        """
        request = requests.get(self.url)
        try:
            if request.status_code == 200:
                raw_request = request.json()
                self.records = raw_request["records"]
        except RuntimeError:
            print(
                f"Error while calling the API : status code {request.status_code}"
            )

    def _process_records(self):
        """Process and clean the raw records.

        Returns
        -------
        day_records : list
            Hour records per day.
        """
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
        for record in self.records:
            hour_record = {}
            for variable in variables:
                if variable in record["fields"]:
                    hour_record[variable] = record["fields"][variable]
            day_records.append(hour_record)
        self.records = day_records

    def _records_to_dataframe(self):
        """Create a pandas dataframe from the records

        Returns
        -------
        records : pa.DataFrame
            Dataframe containing the weather records.
        """
        self.records = pd.DataFrame.from_records(data=self.records)

    def run(self):
        """Execute the "pipeline" of weather records.

        1) Fetch the records from the API.
        2) Process the raw records to clean and neat weather records.
        3) Create a dataframe from the weather records.
        """
        self._fetch_records()
        self._process_records()
        self._records_to_dataframe()
