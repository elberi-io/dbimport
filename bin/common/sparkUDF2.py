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
from typing import Any


class sparkUDFClass:
    def __init__(self) -> None:

        self.seed_string_encoded: bytes | None = None

    def _is_bin(self, ints: object) -> bool:
        try:
            i = int(str(ints))
            if 0 <= i and i < 256:
                return True
            return False
        except ValueError:
            return False

    def base64_encode_array(self, s: Any) -> Any:
        try:
            s_new = []
            for struct in s:
                s_new.append(list(map(self._base64_encode_binary, struct)))
            return s_new

        except TypeError:
            return self._base64_encode_binary(s)

    def _check_binary(self, s: Any) -> bool:
        for b in s:
            if not self._is_bin(b):
                return False
        return True

    def _base64_encode_binary(self, s: Any) -> Any:
        try:
            if self._is_bin(s[0]) and self._check_binary(s[1]):
                base_64 = base64.b64encode(s[1]).decode("UTF-8")
                return [s[0], base_64]
        except:
            pass

        return s

    def parse_struct_date(self, timestamp_string: object) -> str:
        timestamp_string = str(timestamp_string)
        m = re.match(r'\{ *"\$date" *: *(-?\d+)\ *}', timestamp_string)
        if m:
            num = m.group(1)
            timestamp = datetime.utcfromtimestamp(int(num) / 1000)
            return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
        return timestamp_string

    def set_seed_string(self, set_value: str) -> None:
        self.seed_string_encoded = set_value.encode()

    def hash_column(self, column_value: object) -> str | None:
        if column_value == None:
            return None

        # 		sha_value = hashlib.sha3_512(str(columnValue).encode()).hexdigest()
        h = hashlib.blake2b(
            digest_size=32,
            # None only before set_seed_string(), which the import calls first.
            person=self.seed_string_encoded,  # type: ignore[arg-type]
        )
        h.update(str(column_value).encode())
        hashed_column_content = h.hexdigest()
        return hashed_column_content

    def replace_char_with_star(self, column_value: object) -> str | None:
        if column_value == None:
            return None

        return_string = ""
        for character in str(column_value):
            if character == " ":
                return_string = return_string + " "
            else:
                return_string = return_string + "*"

        return return_string

    def show_first_four_characters(self, column_value: object) -> str | None:
        if column_value == None:
            return None

        return_string = str(column_value)[:4]

        if len(str(column_value)) > 4:
            for _i in range(len(str(column_value)) - 4):
                return_string = return_string + "*"

        return return_string
