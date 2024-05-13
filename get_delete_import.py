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

import time
from collections import Counter
from os import environ
from requests.exceptions import RequestException

from importer import session_with_exponential_backoff, import_entry_with_assets

BASE_URL = environ["BASE_URL"].strip('/')
OUTPUT_DIR = './wireq-output'
OUTER_POLL_INTERVAL = 30
MAX_INNER_POLLS = 20
DELETE_DELAY = 0.5
TIMEOUT = 25
IMPORT_RETRY_COUNT = 3


class ImportFailed(Exception):
    pass


class EnsureDelay(object):
    """ Ensure delay seconds between calls """
    def __init__(self, delay=0, skip_first=False):
        self.delay = delay
        self.last_call_time = 0 if skip_first else time.time()

    def __call__(self):
        next_start_time = self.last_call_time + self.delay
        sleep_for = next_start_time - time.time()
        if sleep_for > 0:
            print(f'sleeping for {sleep_for}s...')
            time.sleep(sleep_for)
        self.last_call_time = time.time()

    def update(self, delay):
        self.delay = delay


class WireqReceiver(object):
    def __init__(self, base_url, importer):
        self.base_url = base_url
        self.importer = importer
        self.session = session_with_exponential_backoff()
        self.session.headers.update({'Content-Type': 'application/json'})
        self.seen = set()
        self.retries = Counter()

    def receive_entries_with_retry_hint(self):
        """ Receive entries from wireq
            If wireq suggests to retry-after (since there are more entries to
            get), we follow that hint
        """
        url = f'{self.base_url}/entries.json'
        delay = EnsureDelay(delay=0)
        num_received = 0
        try:
            for i in range(MAX_INNER_POLLS):
                delay()

                print(f'receiving entries {url}')
                response = self.session.get(url, timeout=TIMEOUT)
                response.raise_for_status()

                entries = response.json().get('entries', [])
                for entry in entries:
                    num_received += 1
                    yield entry

                retry_after_hint = response.headers.get('retry-after')
                if retry_after_hint is None:
                    break

                delay.update(int(retry_after_hint))

        except RequestException as e:
            print(f'error {e} (entries will remain in queue)')
            return

        print(f'got {num_received} entries')

    def delete_from_queue(self, entry):
        try:
            receipt = entry.get("_wireq_receipt")
            response = self.session.delete(
                f'{self.base_url}/entry/{receipt}', timeout=10)
            response.raise_for_status()
            print(f'entry deleted from wireq {entry["entry_id"]}')
        except RequestException as e:
            print(f'delete error (skipped) {e}')

        time.sleep(DELETE_DELAY)

    def import_with_retry(self, entry):
        entry_id = entry['entry_id']

        should_retry = False
        try:
            if self.retries[entry_id] > 0:
                print(f'retrying import for {entry_id}')

            self.importer(OUTPUT_DIR, entry)
        except ImportFailed as e:
            if self.retries[entry_id] < IMPORT_RETRY_COUNT:
                self.retries[entry_id] += 1
                print(f'import failed, entry will be retried {entry_id}')
                should_retry = True

            else:
                print(f'import failed, giving up for entry after {IMPORT_RETRY_COUNT} retries {entry_id} {entry.get("urn")} {entry.get("version")}')

        return should_retry

    def receive_and_import_forever(self):
        """ Fetch entries from wireq and delete them only if importing into CMS
            was successful
        """
        delay = EnsureDelay(delay=OUTER_POLL_INTERVAL, skip_first=True)

        while True:
            delay()

            for entry in self.receive_entries_with_retry_hint():
                entry_id = entry['entry_id']

                if entry_id not in self.seen:
                    should_retry_import = self.import_with_retry(entry)

                    if should_retry_import:
                        continue

                    self.seen.add(entry_id)
                else:
                    print(f'skipping import for duplicate entry {entry_id}')

                self.delete_from_queue(entry)


if __name__ == "__main__":
    r = WireqReceiver(base_url=BASE_URL, importer=import_entry_with_assets)
    r.receive_and_import_forever()
