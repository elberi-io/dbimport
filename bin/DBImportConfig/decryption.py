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

import os
import io
import sys
import logging
import base64
from Crypto.Cipher import PKCS1_v1_5
from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
from Crypto.Random import get_random_bytes
from Crypto import Random
from Crypto.Hash import SHA
from subprocess import Popen, PIPE
from ConfigReader import configuration
from common.Exceptions import invalidConfiguration
import pandas as pd


class crypto(object):
    def __init__(self):
        logging.debug("Executing crypto.__init__()")
        self.privateKeyFile = None
        self.publicKeyFile = None
        self.privateKeyString = None
        self.publicKeyString = None
        self.privateKey = None
        self.publicKey = None

    def setPrivateKeyFile(self, privateKeyFile):
        self.privateKeyFile = privateKeyFile

        if self.privateKeyFile.startswith("/") == False:
            self.privateKeyFile = (
                os.environ["DBIMPORT_HOME"] + "/" + self.privateKeyFile
            )

        if os.path.isfile(self.privateKeyFile) == False:
            raise invalidConfiguration(
                "The private key file cant be opened.\n"
                + "Please check the path in the configuration file for settings Credentials/private_key"
            )

        self.privateKeyString = open(self.privateKeyFile, "r").read()
        self.privateKey = RSA.importKey(self.privateKeyString)

    def setPublicKeyFile(self, publicKeyFile):
        self.publicKeyFile = publicKeyFile

        if self.publicKeyFile.startswith("/") == False:
            self.publicKeyFile = os.environ["DBIMPORT_HOME"] + "/" + self.publicKeyFile

        if os.path.isfile(self.publicKeyFile) == False:
            raise invalidConfiguration(
                "The public key file cant be opened.\n"
                + "Please check the path in the configuration file for settings Credentials/public_key"
            )

        self.publicKeyString = open(self.publicKeyFile, "r").read()
        self.publicKey = RSA.importKey(self.publicKeyString)

    def decrypt(self, strToDecrypt):

        if strToDecrypt == None:
            return None

        sentinel = get_random_bytes(16)

        cipher = PKCS1_v1_5.new(self.privateKey)
        strDecrypted = cipher.decrypt(base64.b64decode(strToDecrypt), sentinel)
        # strDecrypted = self.privateKey.decrypt(base64.b64decode(strToDecrypt))

        try:
            if len(strDecrypted) > 0 and bytes(strDecrypted[:1]) == b"\x02":
                pos = strDecrypted.index(b"\x00")
                strDecrypted = strDecrypted[pos + 1 :].decode().strip()
                return strDecrypted
            elif len(strDecrypted) > 0 and type(strDecrypted) is bytes:
                strDecrypted = strDecrypted.decode().strip()
                return strDecrypted
            else:
                return None
        except UnicodeDecodeError:
            raise invalidConfiguration(
                "Unable to decrypt using private/public keys. Are the keys setup correct?"
            )

    def encrypt(self, strToEncrypt):

        cipher = PKCS1_v1_5.new(self.publicKey)

        strEncrypted = cipher.encrypt(strToEncrypt.encode())
        strEncrypted = base64.b64encode(strEncrypted).decode()

        return strEncrypted
