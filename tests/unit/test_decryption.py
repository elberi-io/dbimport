import pytest
from ConfigReader import configuration
from DBImportConfig import decryption


@pytest.fixture
def crypto() -> decryption.crypto:
    """crypto object with the key pair from the test configuration."""
    obj = decryption.crypto()
    obj.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
    obj.setPublicKeyFile(configuration.get("Credentials", "public_key"))
    return obj


@pytest.mark.parametrize("plaintext", ["user secret", "user pass word", "a"])
def test_decrypt_of_encrypted_text_returns_original(
    crypto: decryption.crypto, plaintext: str
) -> None:
    assert crypto.decrypt(crypto.encrypt(plaintext)) == plaintext


def test_decrypt_strips_surrounding_whitespace(crypto: decryption.crypto) -> None:
    # Credentials are stored as "<user> <password>\n".
    assert crypto.decrypt(crypto.encrypt("user secret\n")) == "user secret"


def test_decrypt_none_returns_none(crypto: decryption.crypto) -> None:
    assert crypto.decrypt(None) is None
