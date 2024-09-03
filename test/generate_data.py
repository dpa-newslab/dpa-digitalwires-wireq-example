# -*- coding: utf-8 -*-
#
# Copyright 2024 dpa-IT Services GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import random
import string
import time
import datetime


def generate_random_string(size, alphabet):
    """
    Generates a random string of a certain size, made up of a certain alphabet.
    :param size: The length of the string to return
    :param alphabet: The alphabet the string is generated with
    :return: A randomly generated string of a certain length
    """
    return ''.join(random.choice(alphabet) for _ in range(size))


def generate_random_date(start_date, end_date):
    """
    Generates a random date between two given dates.
    :param start_date: The lower boundary of the randomly generated date
    :param end_date:The upper boundary of the randomly generated date
    :return: A randomly generated date between the start- and end date
    """
    max_difference = int(end_date) - int(start_date)
    difference = random.randint(0, max_difference)
    return start_date + difference


def generate_files(number_of_files):
    """
    Generates random minimal articles in the JSON wireQ-format, containing the fields 'urn', 'entry-id', 'version',
    'version-created' and 'updated'.
    :param number_of_files: The number of JSON-articles to be generated
    :return: A list of wireQ-articles in JSON-format
    """
    id_chars = string.ascii_letters + string.digits
    json_files = []
    i = 0

    while i < number_of_files:
        number_of_versions = random.choice([1, 2, 3, 4, 5, 6])
        current_time = time.time()

        start = datetime.datetime(2020, 1, 1)
        start_date = time.mktime(
            start.timetuple()
        )

        urn = f'urn:newsml:dpa.com:{generate_random_string(8, string.digits)}' \
              f':{generate_random_string(6, string.digits)}-' \
              f'{generate_random_string(2, string.digits)}-{generate_random_string(6, string.digits)}'

        for version in range(number_of_versions):
            if i < number_of_files:
                version_created = generate_random_date(start_date, current_time)

                start_date = version_created

                updated = generate_random_date(start_date, current_time)

                entry_id = generate_random_string(41, id_chars)

                file = json.dumps({
                    'urn': urn,
                    'entry_id': entry_id,
                    'version': version + 1,
                    'version_created': time.strftime('%Y-%m-%dT%X%z', time.gmtime(version_created)),
                    'updated': time.strftime('%Y-%m-%dT%XZ', time.gmtime(updated))
                })
                json_files.append(json.loads(file))
                i += 1

    return json_files
