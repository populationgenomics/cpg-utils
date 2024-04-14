import os

GCLOUD_ACTIVATE_AUTH_BASE = [
    'gcloud',
    '-q',
    'auth',
    'activate-service-account',
    '--key-file=/gsa-key/key.json',
]
GCLOUD_ACTIVATE_AUTH = ' '.join(GCLOUD_ACTIVATE_AUTH_BASE)

DEFAULT_GITHUB_ORGANISATION = 'populationgenomics'

DEFAULT_CROMWELL_URL = 'https://cromwell.populationgenomics.org.au'
DEFAULT_CROMWELL_AUDIENCE = (
    '717631777761-ec4u8pffntsekut9kef58hts126v7usl.apps.googleusercontent.com'
)


CROMWELL_AUDIENCE = os.getenv('CROMWELL_AUDIENCE', DEFAULT_CROMWELL_AUDIENCE)
CROMWELL_URL = os.getenv('CROMWELL_URL', DEFAULT_CROMWELL_URL)


class AnsiColors:
    """
    Lookup table: https://en.wikipedia.org/wiki/ANSI_escape_code#3/4_bit
    """

    BRIGHTMAGENTA = '\033[95m'  # Bright magenta
    BRIGHTBLUE = '\033[94m'  # Bright blue
    BRIGHTGREEN = '\033[92m'  # Bright green
    BRIGHTYELLOW = '\033[93m'  # Bright yellow
    BRIGHTRED = '\033[91m'  # Bright red
    RESET = '\033[0m'  # SGR (Reset / Normal)
    BOLD = '\033[1m'  # SGR (Bold or increased intensity
    ITALIC = '\033[3m'  # SGR (Italic)
    UNDERLINE = '\033[4m'  # SGR (Underline)
