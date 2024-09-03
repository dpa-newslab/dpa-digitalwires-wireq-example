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

import unittest
import logging

import requests
import functools
import os

from unittest.mock import patch
from dotenv import load_dotenv

from lib.dequeue_import import receive_once
from lib.get_delete_import import WireqReceiver
from lib.importer import session_with_exponential_backoff

load_dotenv()

BASE_URL = str(os.environ.get('BASE_URL')).strip('/')

logger = logging.getLogger('test_logger')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

handler.setFormatter(formatter)
logger.addHandler(handler)


class DummyStore(object):
    """
    The DummyStore() class is necessary to receive articles from the  wireQ, without saving them locally.
    It has the ability to store and show articles. For testing purposes is the alternative to the wireq-example's usual
    behaviour of writing articles as files onto the disk.
    Any kind of further processing of the articles for additional tests for should be done after they have landed
    in the store.
    """

    stored_entries = []

    def __init__(self):
        self.stored_entries = []

    def put(self, entries):
        self.stored_entries.extend(entries)

    def put_single_entry(self, entry):
        self.stored_entries.append(entry)

    def pull(self):
        return self.stored_entries

    def reset(self):
        self.stored_entries = []


class TestWireq(unittest.TestCase):

    @patch('lib.dequeue_import.BASE_URL', BASE_URL)
    @patch('lib.dequeue_import.sleep')
    def test_dequeue(self, get_sleep):

        logger.info('Starting to test dequeue operation')

        test_store = DummyStore()
        sleep_durations = []
        get_sleep.return_value = True
        entries_per_poll = []

        def get_duration(duration):
            sleep_durations.append(duration)

        get_sleep.side_effect = get_duration

        def add_to_entries_per_poll(all_entries):
            if not entries_per_poll:
                entries_per_poll.append(all_entries)
            else:
                entries_per_poll.append(all_entries - functools.reduce(lambda a, b: a + b, entries_per_poll))

        last_sleep = 0
        max_loop = 3

        while last_sleep < 60 and max_loop > 0:
            receive_once(test_store)
            logger.info('Finished single dequeue operation')
            logger.info(f'Received {len(test_store.pull())} entries')
            logger.info(f'Retry-after header indicates {sleep_durations[0]}s before the next polling attempt')

            add_to_entries_per_poll(len(test_store.pull()))

            if entries_per_poll[-1] == 0:
                assert sleep_durations[0] > 60

            last_sleep = sleep_durations[-1]
            max_loop -= 1

        if sleep_durations[-1] < 60:
            logger.info('Last retry-after header indicates there is more data in the wireQ')
            logger.info('All polls should have received the same amount of elements and the same retry-after header')
            assert all(x == entries_per_poll[0] for x in entries_per_poll)
            assert all(y == sleep_durations[0] for y in sleep_durations)
        else:
            last_entries = entries_per_poll.pop()
            last_sleep_duration = sleep_durations.pop()
            if entries_per_poll and sleep_durations:
                logger.info('More than one poll was made before the wireQ was empty')
                logger.info('All polls made before the last one that emptied the wireQ should have received the same '
                            'amount of elements and the same retry-after header')
                assert all(x == entries_per_poll[0] for x in entries_per_poll)
                assert all(y == sleep_durations[0] and y < 60 for y in sleep_durations)
                assert last_entries <= entries_per_poll[0]
                assert last_sleep_duration >= sleep_durations[0]
            else:
                logger.info('Only one poll was made before the wireQ was empty')
                assert last_sleep_duration > 60
                assert last_entries >= 0

    @patch('lib.get_delete_import.MAX_INNER_POLLS', 1)
    @patch('lib.get_delete_import.EnsureDelay.update')
    @patch('lib.get_delete_import.EnsureDelay.__call__')
    @patch('lib.get_delete_import.WireqReceiver.delete_from_queue')
    def test_get_delete(self, delete_from_queue, call_delay, get_sleep):

        logger.info('Starting to test get_delete operation')

        test_store = DummyStore()
        sleep_durations = []
        entries_per_poll = []
        all_entries = []

        call_delay.return_value = None

        get_sleep.return_value = None
        delete_from_queue.return_value = None

        def get_duration(duration):
            sleep_durations.append(duration)

        get_sleep.side_effect = get_duration

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url=BASE_URL, importer=mock_importer)

        receiver.session = session_with_exponential_backoff()

        last_sleep = 0
        max_loop = 3

        while last_sleep < 60 and max_loop > 0:
            entries_generator = receiver.receive_entries_with_retry_hint()
            entries = [entry for entry in entries_generator]
            logger.info('Finished single Get request')
            logger.info(f'Received {len(entries)} entries')
            entries_per_poll.append(len(entries))
            if len(entries) == 0:
                logger.info(f'No entries received, no sleep attempted')
                break

            logger.info(f'Retry-after header indicates {sleep_durations[0]}s before the next polling attempt')

            all_entries.extend(entries)

            last_sleep = sleep_durations[-1]

            max_loop -= 1

            logger.info('Testing for Deletion')
            receipts = [entry.get('_wireq_receipt') for entry in entries]
            for receipt in receipts:
                logger.info(f'deleting {BASE_URL}/entry/{receipt}')
                response = requests.delete(f'{BASE_URL}/entry/{receipt}')
                self.assertEqual(response.status_code, 204)

        if entries:
            if sleep_durations[-1] < 60:
                logger.info('Last retry-after header indicates there is more data in the wireQ')
                logger.info(
                    'All polls should have received the same amount of elements and the same retry-after header')
                assert all(x == entries_per_poll[0] for x in entries_per_poll)
                assert all(y == sleep_durations[0] for y in sleep_durations)
            else:
                last_entries = entries_per_poll.pop()
                last_sleep_duration = sleep_durations.pop()
                if entries_per_poll and sleep_durations:
                    logger.info('More than one poll was made before the wireQ was empty')
                    logger.info(
                        'All polls made before the last one that emptied the wireQ should have received the same '
                        'amount of elements and the same retry-after header')
                    assert all(x == entries_per_poll[0] for x in entries_per_poll)
                    assert all(y == sleep_durations[0] and y < 60 for y in sleep_durations)
                    assert last_entries <= entries_per_poll[0]
                    assert last_sleep_duration >= sleep_durations[0]
                else:
                    logger.info('Only one poll was made before the wireQ was empty')
                    assert last_sleep_duration > 60
                    assert last_entries >= 0

        else:
            logger.info('Queue was empty')


if __name__ == '__main__':
    unittest.main()
