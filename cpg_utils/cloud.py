"""Convenience functions related to cloud infrastructure."""

from google.auth import jwt


def email_from_id_token(id_token: str) -> str:
    """Decodes the ID token (JWT) to get the email address of the caller.

    See http://bit.ly/2YAIkzy for details.

    This function assumes that the token has been verified beforehand."""

    return jwt.decode(id_token, verify=False)['email']
