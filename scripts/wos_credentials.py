import os
import keyring
from keyring.errors import KeyringError
from dotenv import load_dotenv

load_dotenv()


def get_wos_api_key() -> str:
    api_key = None

    try:
        api_key = keyring.get_password("wos_api", "default")
    except KeyringError:
        api_key = None

    if not api_key:
        api_key = os.getenv("WOS_API_KEY")

    if not api_key:
        raise RuntimeError(
            "Missing API key. Set WOS_API_KEY in .env or store it in keyring."
        )

    return api_key