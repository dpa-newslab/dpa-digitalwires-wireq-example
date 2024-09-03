#!/usr/bin/env python
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

import logging
import os
import requests

from requests.exceptions import RequestException
from time import sleep

from .importer import import_entry_with_assets

logger = logging.getLogger()
logging.getLogger('urllib3').setLevel(logging.INFO)

BASE_URL = ''
OUTPUT_DIR = './wireq-output'
POLL_INTERVAL = 120
TIMEOUT = 15


class WireqReceiver(object):
    """
    Store articles locally in OUTPUT_DIR.
    """

    def put(self, entries):
        for entry in entries:
            import_entry_with_assets(OUTPUT_DIR, entry)


def receive_once(store):
    try:
        response = requests.post(
            f'{BASE_URL}/dequeue-entries.json', timeout=TIMEOUT)
    except RequestException as e:
        logger.error(e)

    response.raise_for_status()
    entries = response.json().get('entries', [])
    store.put(entries)

    delay_hint = response.headers.get('retry-after')
    t = int(POLL_INTERVAL if delay_hint is None else delay_hint)
    logger.info(f'waiting for {t}s ...')
    sleep(t)


def receive_forever(store):
    """
    Receive entries from wireq and immediately save them to some store
    component.
    If wireq suggests to retry-after a specific period of time, we follow
    that hint. (this is useful for situations when there are more entries to
    get or after a 429 'too many requests' error)
    """
    delay_hint = None
    while True:
        receive_once(store)


if __name__ == "__main__":
    BASE_URL = os.environ['BASE_URL'].strip('/')
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    store = WireqReceiver()
    receive_forever(store)
