# -*- coding: utf-8 -*-
#
# Copyright 2021, 2021 dpa-IT Services GmbH
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

import os
import requests

from time import sleep
from helpers import write_article_to_disk

BASE_URL = os.environ["BASE_URL"]


def receive_and_process():
    """ get and delete entries in one step """
    def poll():
        try:
            # Copy base-URL from api-portal - format: base_url/
            url = "{}dequeue-entries.json".format(BASE_URL)
            response = requests.post(url)
        except Exception as e:
            raise e

        return response.json().get("entries", [])

    inner_poll_interval = 15
    outer_poll_interval = 30
    while True:
        while entries := poll():
            for entry in entries:
                try:
                    process(entry)
                except Exception as e:
                    raise e
            sleep(inner_poll_interval)

        sleep(outer_poll_interval)


def process(entry):
    """ Process article """
    # TODO: Transform and import dpa-digitalwires entry
    # Write content to local file system
    return write_article_to_disk(entry)


if __name__ == "__main__":
    receive_and_process()
