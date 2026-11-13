import pytest

from dbimport.config_db import decryption
from dbimport.settings import get_settings


@pytest.fixture
def crypto() -> decryption.Crypto:
    """crypto object with the key pair from the test configuration."""
    obj = decryption.Crypto()
    obj.set_private_key_file(get_settings().credentials.private_key)
    obj.set_public_key_file(get_settings().credentials.public_key)
    return obj


@pytest.mark.parametrize("plaintext", ["user secret", "user pass word", "a"])
def test_decrypt_of_encrypted_text_returns_original(
    crypto: decryption.Crypto, plaintext: str
) -> None:
    assert crypto.decrypt(crypto.encrypt(plaintext)) == plaintext


def test_decrypt_strips_surrounding_whitespace(crypto: decryption.Crypto) -> None:
    # Credentials are stored as "<user> <password>\n".
    assert crypto.decrypt(crypto.encrypt("user secret\n")) == "user secret"


def test_decrypt_none_returns_none(crypto: decryption.Crypto) -> None:
    assert crypto.decrypt(None) is None
