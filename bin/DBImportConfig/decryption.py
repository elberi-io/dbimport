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
from Crypto.Random import get_random_bytes

from common.Exceptions import invalidConfiguration


class crypto:
    def __init__(self):
        logging.debug("Executing crypto.__init__()")
        self.privateKeyFile = None
        self.publicKeyFile = None
        self.private_key_string = None
        self.public_key_string = None
        self.privateKey = None
        self.publicKey = None

    def set_private_key_file(self, private_key_file):
        self.privateKeyFile = private_key_file

        if self.privateKeyFile.startswith("/") == False:
            self.privateKeyFile = (
                os.environ["DBIMPORT_HOME"] + "/" + self.privateKeyFile
            )

        if os.path.isfile(self.privateKeyFile) == False:
            raise invalidConfiguration(
                "The private key file cant be opened.\n"
                + "Please check the path in the configuration file for settings Credentials/private_key"
            )

        self.private_key_string = open(self.privateKeyFile).read()
        self.privateKey = RSA.importKey(self.private_key_string)

    def set_public_key_file(self, public_key_file):
        self.publicKeyFile = public_key_file

        if self.publicKeyFile.startswith("/") == False:
            self.publicKeyFile = os.environ["DBIMPORT_HOME"] + "/" + self.publicKeyFile

        if os.path.isfile(self.publicKeyFile) == False:
            raise invalidConfiguration(
                "The public key file cant be opened.\n"
                + "Please check the path in the configuration file for settings Credentials/public_key"
            )

        self.public_key_string = open(self.publicKeyFile).read()
        self.publicKey = RSA.importKey(self.public_key_string)

    def decrypt(self, str_to_decrypt):

        if str_to_decrypt == None:
            return None

        sentinel = get_random_bytes(16)

        cipher = PKCS1_v1_5.new(self.privateKey)
        str_decrypted = cipher.decrypt(base64.b64decode(str_to_decrypt), sentinel)
        # strDecrypted = self.privateKey.decrypt(base64.b64decode(strToDecrypt))

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

    def encrypt(self, str_to_encrypt):

        cipher = PKCS1_v1_5.new(self.publicKey)

        str_encrypted = cipher.encrypt(str_to_encrypt.encode())
        str_encrypted = base64.b64encode(str_encrypted).decode()

        return str_encrypted
