import os
import keyring
from keyring.errors import KeyringError
from dotenv import load_dotenv
import marimo as mo

load_dotenv()

def get_wos_api_key():
    api_key = None

    try:
        api_key = keyring.get_password("wos_api", "default")
    except KeyringError:
        api_key = None

    if not api_key:
        api_key = os.getenv("WOS_API_KEY")

    mo.stop(
        not api_key,
        mo.md("Missing API Key. Please read the Secrets Management guide in the README")
    )

    return api_key
