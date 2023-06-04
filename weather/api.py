import requests

url = "https://public.opendatasoft.com/api/records/1.0/search/?dataset=arome-0025-enriched&q=Houilles&facet=commune&facet=code_commune&refine.commune=Houilles"


def get_records():
    """Fetches data from Public OpenDataSoft API.

    Returns
    -------
    list
        All weather records for Houilles.
    """
    request = requests.get(url)
    try:
        if request.status_code == 200:
            raw_request = request.json()
            return raw_request["records"]
    except RuntimeError:
        print(
            f"Error while calling the API : status code {request.status_code}"
        )


