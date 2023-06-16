import logging
from datetime import date

import pandas as pd
import requests

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class WeatherRecords:
    def __init__(
        self,
        project_id: str,
        dataset: str,
        table_name: str,
        city: str = "Houilles",
        upload_to_bq: bool = True,
    ):
        """Constructor to get weather records.

        Parameters
        ----------
        city : str
            Name of the city to fetch the info.
        upload_to_bq : bool
            Is the data uploaded in BigQuery?
        """
        base_url = "https://public.opendatasoft.com/api/records"
        version = "1.0"
        self.url = (
            f"{base_url}/{version}/search/"
            f"?dataset=arome-0025-enriched&q={city}"
            f"&facet=commune&facet=code_commune"
            f"&refine.commune={city}"
        )
        self.records = None
        self.upload_to_bq = upload_to_bq
        self.project_id = project_id
        self.dataset = dataset
        self.table_name = table_name

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
            logger.error(
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
            "minimum_temperature_at_2_metres",
            "maximum_temperature_at_2_metres",
            "total_precipitation",
            "10m_wind_speed",
        ]
        for record in self.records:
            hour_record = {}
            for variable in variables:
                if variable in record["fields"]:
                    if variable == "position":
                        latitude = record["fields"][variable][0]
                        longitude = record["fields"][variable][1]
                        hour_record["latitude"] = latitude
                        hour_record["longitude"] = longitude
                    else:
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
        self.records = pd.DataFrame.from_records(data=self.records).sort_values(
            by="forecast"
        )

    def upload_records_to_bigquery(self):
        """Upload data to Google BigQuery.

        Create a table with the day values and append its content to the history
        dataset.
        """
        logger.info("Exporting data to Google BigQuery.")
        self.records.to_gbq(
            destination_table=f"{self.dataset}.{self.table_name}_history",
            project_id=self.project_id,
            if_exists="append",
        )
        self.records.to_gbq(
            destination_table=f"{self.dataset}.{self.table_name}_day",
            project_id=self.project_id,
            if_exists="replace",
        )

    def save_records_local(self):
        """Save the results in a CSV file in the /data folder."""
        self.records.to_csv(f"data/weather-records-{date.today()}.csv", index=False)

    def run(self):
        """Execute the "pipeline" of weather records.

        1) Fetch the records from the API.
        2) Process the raw records to clean and neat weather records.
        3) Create a dataframe from the weather records.
        4) Save the results (locally or in BigQuery).
        """
        self._fetch_records()
        self._process_records()
        self._records_to_dataframe()
        if self.upload_to_bq:
            logger.info("Uploading data to BigQuery")
            self.upload_records_to_bigquery()
        else:
            logger.info("Data saved in local")
            self.save_records_local()
