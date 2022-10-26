#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2022 dpa-IT Services GmbH
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

BASE_URL = os.environ['BASE_URL'].strip('/')
POLL_INTERVAL = 120
TIMEOUT = 15


def receive_and_process():
    """ get entries and process, delete only if processing is successful """
    def poll():
        try:
            # Copy base-URL from api-portal
            response = requests.get(f'{BASE_URL}/entries.json')
        except Exception as e:
            raise e

        response.raise_for_status()
        entries = response.json().get("entries", [])
        retry_after = response.headers.get("retry-after")
        return entries, retry_after

    while True:
        entries, delay_hint = poll()
        for entry in entries:
            try:
                receipt = entry.get("_wireq_receipt")
                ok = process(entry)
                if ok:
                    # Only remove entry from queue, if processing successful
                    url = f'{BASE_URL}/entry/{receipt}'
                    requests.delete(url)
            except Exception as e:
                raise e
        t = POLL_INTERVAL if delay_hint is None else int(delay_hint)
        print(f'waiting for {t}s ...')
        sleep(t)


def process(entry):
    """ Process article """
    # TODO: Transform and import dpa-digitalwires entry
    # Write content to local file system
    return write_article_to_disk(entry)


if __name__ == "__main__":
    receive_and_process()
