"""
Sending notifications to Slack. To enable, create a channel, then add a
corresponding Slack app (e.g. Seqr Loader) to the channel:

/invite @Seqr Loader

Make sure `slack/channel`, `slack/token_secret_id`, and `slack/token_project_id`
configuration values are set.
"""

import logging

import slack_sdk

from cpg_utils.cloud import read_secret
from cpg_utils.config import get_config


def send_message(text: str):
    """Sends `text` as a Slack message, reading credentials from the config."""
    slack_channel = get_config().get('slack', {}).get('channel')
    if not slack_channel:
        raise ValueError('`slack.channel` must be set in config')

    token_secret_id = get_config()['slack'].get('token_secret_id')
    token_project_id = get_config()['slack'].get('token_project_id')
    if not token_secret_id or not token_project_id:
        raise ValueError(
            '`slack.token_secret_id` and `slack.token_project_id` '
            'must be set in config to retrieve Slack token'
        )
    slack_token = read_secret(
        project_id=token_project_id,
        secret_name=token_secret_id,
        fail_gracefully=False,
    )

    slack_client = slack_sdk.WebClient(token=slack_token)
    try:
        slack_client.api_call(
            'chat.postMessage',
            json={
                'channel': slack_channel,
                'text': text,
            },
        )
    except slack_sdk.errors.SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')
