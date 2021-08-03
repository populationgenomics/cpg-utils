from typing import List

import json

from os import getenv

import requests

HOST = 'https://sample-metadata.populationgenomics.org.au/api/v1/'


def get_sample_id_map_with_project(project: str, internal_ids: List[str]):
    path = f'{project}/sample/id-map/internal'
    url = HOST + path
    resp = requests.post(url, json.dumps(internal_ids), headers=get_auth_header(url))
    resp.raise_for_status()
    return resp.json()


def get_sample_id_map_for_all_samples_with_project(project: str):
    path = f'{project}/sample/id-map/internal/all'
    url = HOST + path
    resp = requests.get(url, headers=get_auth_header(url))
    resp.raise_for_status()
    return resp.json()


def get_sample_id_map(internal_ids: List[str]):
    path = 'sample-map'
    url = HOST + path
    resp = requests.post(url, json.dumps(internal_ids), headers=get_auth_header(url))
    return resp.json()


def get_auth_header(url):
    return {'Authorization': f'Bearer {_get_google_auth_token(url)}'}


# def annotation_ht(ht, key):
#
#     return ht.annotate_rows(external_id=)


def _get_google_auth_token(url) -> str:
    # https://stackoverflow.com/a/55804230
    # command = ['gcloud', 'auth', 'print-identity-token']
    import google.oauth2.id_token
    import google.auth.exceptions
    import google.auth.transport.requests

    # ie: use service account identity token by default, then fallback otherwise
    use_service_account = str(getenv('SM_USE_SERVICE_ACCOUNT', 'true')).lower() in (
        'true',
        '1',
    )

    if use_service_account:
        try:
            auth_req = google.auth.transport.requests.Request()
            id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)

            return id_token
        except google.auth.exceptions.DefaultCredentialsError as e:
            raise google.auth.exceptions.DefaultCredentialsError(
                f"Couldn't find a local service account for GCP, if you mean to use your local credentials,"
                f"please 'export SM_USE_SERVICE_ACCOUNT=false'. Original error: {str(e)}"
            ) from e
    else:
        creds, _ = google.auth.default()

        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        return creds.id_token
