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
import sys
from datetime import datetime

this = sys.modules[__name__]
this.seedString = None
seedString = "testSeed"


def _is_bin(ints):
    try:
        i = int(str(ints))
        if 0 <= i and i < 256:
            return True
        return False
    except ValueError:
        return False


def base64_encode_array(s):
    try:
        s_new = []
        for struct in s:
            s_new.append(list(map(_base64_encode_binary, struct)))
        return s_new

    except TypeError:
        return _base64_encode_binary(s)


def _check_binary(s):
    for b in s:
        if not _is_bin(b):
            return False
    return True


def _base64_encode_binary(s):
    try:
        if _is_bin(s[0]) and _check_binary(s[1]):
            base_64 = base64.b64encode(s[1]).decode("UTF-8")
            return [s[0], base_64]
    except:
        pass

    return s


def parse_struct_date(timestampString):
    timestampString = str(timestampString)
    m = re.match(r'\{ *"\$date" *: *(-?\d+)\ *}', timestampString)
    if m:
        num = m.group(1)
        timestamp = datetime.utcfromtimestamp(int(num) / 1000)
        return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
    return timestampString


def set_seed_string(setValue):
    global seedString
    seedString = setValue
    this.seedString = setValue
    print(seedString)
    print(this.seedString)


def hash_column(columnValue):
    global seedString
    sha_value = hashlib.sha3_512(columnValue.encode()).hexdigest()
    # 	SeedString = None
    # 	SeedString = 'testSeed'
    h = hashlib.blake2b(digest_size=32, person=seedString.encode())
    h.update(columnValue.encode())
    hashed_column_content = h.hexdigest()
    # 	sha_value = hashlib.blake2b(columnValue.encode()).hexdigest()
    # 	return sha_value
    return this.seedString
