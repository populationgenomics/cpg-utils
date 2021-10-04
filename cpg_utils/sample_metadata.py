# pylint: disable=import-outside-toplevel
"""
Sample-metadata related functions
    https://github.com/populationgenomics/sample-metadata
"""
from typing import List, Dict, Optional

from os import getenv
import json
import logging

import requests
import google.oauth2.id_token
import google.auth.exceptions
import google.auth.transport.requests


HOST = 'https://sample-metadata.populationgenomics.org.au/api/v1/'


def get_sample_id_map_with_project(
    project: str, internal_ids: List[str]
) -> Dict[str, str]:
    """
    Get sample-id map for listed internal_ids, specifying a project:
        {internal_id: external_id}.
    Specifying a project is a quicker and easier request
    for the sample-metadata server
    """
    path = f'sample/{project}/id-map/internal'
    url = HOST + path
    resp = requests.post(url, json.dumps(internal_ids), headers=_get_auth_header())
    resp.raise_for_status()
    return resp.json()


def get_sample_id_map_for_all_samples_with_project(project: str) -> Dict[str, str]:
    """
    Get sample-id map for all samples, specifying a project:
        {internal_id: external_id}.
    Specifying a project is a quicker and easier request
    for the sample-metadata server
    """
    path = f'sample/{project}/id-map/internal/all'
    url = HOST + path
    resp = requests.get(url, headers=_get_auth_header())
    resp.raise_for_status()
    return resp.json()


def get_sample_id_map(internal_ids: List[str]) -> Dict[str, str]:
    """
    Get sample-id map for listed samples without knowing a project:
        {internal_id: external_id}.
    If you know the project, you should specify the project.
    """
    path = 'sample/id-map/internal'
    url = HOST + path
    resp = requests.post(url, json.dumps(internal_ids), headers=_get_auth_header())
    return resp.json()


def annotate_mt_with_external_ids(
    mt, project: Optional[str], key='s', annotate_column_name='external_id'
):
    """
    Annotate a matrix table with the external id:
    - By default, find external ids in column {key='s'}
    - Annotate with name {annotate_column_name='external_id'}
    """
    import hail as hl

    keys = mt[key].collect()
    if project:
        id_map = get_sample_id_map_with_project(project, keys)
    else:
        id_map = get_sample_id_map(keys)

    id_map_expr = hl.literal(id_map)

    annotation = {annotate_column_name: id_map_expr.get(mt[key])}
    return mt.annotate_cols(**annotation)


# UTIL


def _get_auth_header() -> Dict[str, str]:
    """Get Authorization header"""
    return {'Authorization': f'Bearer {_get_google_auth_token()}'}


def _get_google_auth_token(
    url='https://sample-metadata-api-mnrpw3mdza-ts.a.run.app/',
) -> str:
    """
    Get google auth token in two ways:
    - if GOOGLE_APPLICATION_CREDENTIALS is set, then you
    """
    # https://stackoverflow.com/a/55804230
    # command = ['gcloud', 'auth', 'print-identity-token']
    # OR

    # ie: use service account identity token by default, then fallback otherwise
    credentials_filename = getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if credentials_filename:
        with open(credentials_filename, 'r', encoding='utf-8') as f:
            from google.oauth2 import service_account

            info = json.load(f)
            credentials_content = (
                info if (info.get('type') == 'service_account') else None
            )
            credentials = service_account.IDTokenCredentials.from_service_account_info(
                credentials_content, target_audience=url
            )
            auth_req = google.auth.transport.requests.Request()
            credentials.refresh(auth_req)
            return credentials.token

    else:
        try:
            creds, _ = google.auth.default()
            auth_req = google.auth.transport.requests.Request()
            creds.refresh(auth_req)
            return creds.id_token
        except google.auth.exceptions.RefreshError as e:
            m = (
                'Failed to refresh credentials, you might need to export '
                f'"GOOGLE_APPLICATION_CREDENTIALS" due to {e}'
            )
            logging.critical(m)
            raise Exception(m) from e
