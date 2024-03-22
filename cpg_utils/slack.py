"""
Sending notifications to Slack. To enable, create a channel, then add a
corresponding Slack app (e.g. Seqr Loader) to the channel:

/invite @Seqr Loader

Make sure `slack/channel`, `slack/token_secret_id`, and `slack/token_project_id`
configuration values are set.
"""

import logging

try:
    import slack_sdk
except ImportError:
    slack_sdk = None

from cpg_utils.cloud import read_secret
from cpg_utils.config import config_retrieve


def _get_slack_sdk():  # noqa: ANN202
    if slack_sdk:
        return slack_sdk

    raise ImportError("slack_sdk is not installed")


def _get_channel() -> str:
    """Returns the Slack channel from the config."""
    slack_channel = config_retrieve(['slack', 'channel'], default=None)
    if not slack_channel:
        raise ValueError('`slack.channel` must be set in config')
    return slack_channel


def _get_token() -> str:
    """Returns the Slack token from the config."""
    token_secret_id = config_retrieve(['slack', 'token_secret_id'], default=None)
    token_project_id = config_retrieve(['slack', 'token_project_id'], default=None)

    if not token_secret_id or not token_project_id:
        raise ValueError(
            '`slack.token_secret_id` and `slack.token_project_id` '
            'must be set in config to retrieve Slack token',
        )
    slack_token = read_secret(
        project_id=token_project_id,
        secret_name=token_secret_id,
        fail_gracefully=False,
    )
    if slack_token is None:
        raise ValueError('Failed to retrieve Slack token')
    return slack_token


def send_message(text: str) -> None:
    """Sends `text` as a Slack message, reading credentials from the config."""
    slack_client = _get_slack_sdk().WebClient(token=_get_token())
    try:
        slack_client.chat_postMessage(
            channel=_get_channel(),
            text=text,
        )
    except _get_slack_sdk().errors.SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')


def upload_file(content: bytes, comment: str) -> None:
    """Uploads `content` to Slack with the given text `comment`, reading credentials from the config."""
    slack_client = _get_slack_sdk().WebClient(token=_get_token())
    try:
        slack_client.files_upload(
            channels=_get_channel(),
            content=content,
            initial_comment=comment,
        )
    except _get_slack_sdk().errors.SlackApiError as err:
        logging.error(f'Error posting to Slack: {err}')
