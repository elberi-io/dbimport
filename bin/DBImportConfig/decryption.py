# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import base64
import logging
import os

from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
from Crypto.PublicKey.RSA import RsaKey
from Crypto.Random import get_random_bytes

from common.Exceptions import invalidConfiguration


class crypto:
    def __init__(self) -> None:
        logging.debug("Executing crypto.__init__()")
        self.private_key_file: str | None = None
        self.public_key_file: str | None = None
        self.private_key_string: str | None = None
        self.public_key_string: str | None = None
        self.private_key: RsaKey | None = None
        self.public_key: RsaKey | None = None

    def set_private_key_file(self, private_key_file: str) -> None:
        self.private_key_file = private_key_file

        if not self.private_key_file.startswith("/"):
            self.private_key_file = (
                os.environ["DBIMPORT_HOME"] + "/" + self.private_key_file
            )

        if not os.path.isfile(self.private_key_file):
            raise invalidConfiguration(
                "The private key file cant be opened.\n"
                + "Please check the path in the configuration file for settings Credentials/private_key"
            )

        self.private_key_string = open(self.private_key_file).read()
        self.private_key = RSA.importKey(self.private_key_string)

    def set_public_key_file(self, public_key_file: str) -> None:
        self.public_key_file = public_key_file

        if not self.public_key_file.startswith("/"):
            self.public_key_file = (
                os.environ["DBIMPORT_HOME"] + "/" + self.public_key_file
            )

        if not os.path.isfile(self.public_key_file):
            raise invalidConfiguration(
                "The public key file cant be opened.\n"
                + "Please check the path in the configuration file for settings Credentials/public_key"
            )

        self.public_key_string = open(self.public_key_file).read()
        self.public_key = RSA.importKey(self.public_key_string)

    def decrypt(self, str_to_decrypt: str | None) -> str | None:

        if str_to_decrypt == None:
            return None

        sentinel = get_random_bytes(16)

        cipher = PKCS1_v1_5.new(self.private_key)
        str_decrypted = cipher.decrypt(base64.b64decode(str_to_decrypt), sentinel)
        # strDecrypted = self.private_key.decrypt(base64.b64decode(strToDecrypt))

        try:
            if len(str_decrypted) > 0 and bytes(str_decrypted[:1]) == b"\x02":
                pos = str_decrypted.index(b"\x00")
                str_decrypted = str_decrypted[pos + 1 :].decode().strip()
                return str_decrypted
            elif len(str_decrypted) > 0 and type(str_decrypted) is bytes:
                str_decrypted = str_decrypted.decode().strip()
                return str_decrypted
            else:
                return None
        except UnicodeDecodeError:
            raise invalidConfiguration(
                "Unable to decrypt using private/public keys. Are the keys setup correct?"
            )

    def encrypt(self, str_to_encrypt: str) -> str:

        cipher = PKCS1_v1_5.new(self.public_key)

        str_encrypted = cipher.encrypt(str_to_encrypt.encode())
        str_encrypted = base64.b64encode(str_encrypted).decode()

        return str_encrypted
