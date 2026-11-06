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
import hashlib
import re
from datetime import datetime


class sparkUDFClass:
    def __init__(self):

        self.seedStringEncoded = None

    def _is_bin(self, ints):
        try:
            i = int(str(ints))
            if 0 <= i and i < 256:
                return True
            return False
        except ValueError:
            return False

    def base64EncodeArray(self, s):
        try:
            s_new = []
            for struct in s:
                s_new.append(list(map(self._base64EncodeBinary, struct)))
            return s_new

        except TypeError:
            return self._base64EncodeBinary(s)

    def _checkBinary(self, s):
        for b in s:
            if not self._is_bin(b):
                return False
        return True

    def _base64EncodeBinary(self, s):
        try:
            if self._is_bin(s[0]) and self._checkBinary(s[1]):
                base_64 = base64.b64encode(s[1]).decode("UTF-8")
                return [s[0], base_64]
        except:
            pass

        return s

    def parseStructDate(self, timestampString):
        timestampString = str(timestampString)
        m = re.match(r'\{ *"\$date" *: *(-?\d+)\ *}', timestampString)
        if m:
            num = m.group(1)
            timestamp = datetime.utcfromtimestamp(int(num) / 1000)
            return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        return timestampString

    def setSeedString(self, setValue):
        self.seedStringEncoded = setValue.encode()

    def hashColumn(self, columnValue):
        if columnValue == None:
            return None

        # 		sha_value = hashlib.sha3_512(str(columnValue).encode()).hexdigest()
        h = hashlib.blake2b(digest_size=32, person=self.seedStringEncoded)
        h.update(str(columnValue).encode())
        hashedColumnContent = h.hexdigest()
        return hashedColumnContent

    def replaceCharWithStar(self, columnValue):
        if columnValue == None:
            return None

        returnString = ""
        for character in str(columnValue):
            if character == " ":
                returnString = returnString + " "
            else:
                returnString = returnString + "*"

        return returnString

    def showFirstFourCharacters(self, columnValue):
        if columnValue == None:
            return None

        returnString = str(columnValue)[:4]

        if len(str(columnValue)) > 4:
            for i in range(len(str(columnValue)) - 4):
                returnString = returnString + "*"

        return returnString
