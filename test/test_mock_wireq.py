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
import io
import sys

import responses
import logging

from time import sleep, time
from unittest.mock import patch
from requests.exceptions import HTTPError

from lib.dequeue_import import receive_once
from lib.get_delete_import import WireqReceiver
from .generate_data import generate_files
from .mock_wireq import MockQueue

logger = logging.getLogger('test_logger')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

handler.setFormatter(formatter)
logger.addHandler(handler)


class DummyStore(object):
    """
    The DummyStore() class is necessary to receive articles from the simulated wireq, it has the ability to store
    and show articles. For testing purposes is the alternative to the wireq-example's usual behaviour that writes
    articles as files onto the disk.
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


def no_sleep(self):
    """
    A function to be patched over the sleep() function that disables the sleep to avoid longer testing times
    """
    return


class MockedTime:
    @staticmethod
    def sleep(time):
        return

    @staticmethod
    def time():
        return time()


def get_files(path, number_of_files):
    return generate_files(number_of_files)


class TestDequeueError(unittest.TestCase):
    """Tests for 404 errors by using the wrong BASE_URL (dumy.co instead of dummy.co)"""

    @patch('lib.dequeue_import.BASE_URL', 'https://dumy.co')
    @patch('lib.dequeue_import.sleep', no_sleep)
    @responses.activate
    def test_dequeue_404(self):
        mocked_queue = MockQueue([])
        with patch('lib.dequeue_import.requests', new=mocked_queue):
            test_store = DummyStore()

            logger.info('Testing, sending dequeue request to wrong base-url should throw 404 error')

            self.assertRaisesRegex(HTTPError, '404 Client Error: Not Found', receive_once, test_store)

    """Tests for 403 errors by using the correct BASE_URL but an unknown route instead of just 'dequeue-entries.json'"""

    @patch('lib.dequeue_import.BASE_URL', 'https://dummy.co/notarealroute')
    @patch('lib.dequeue_import.sleep', no_sleep)
    @responses.activate
    def test_dequeue_403(self):
        mocked_queue = MockQueue([])
        with patch('lib.dequeue_import.requests', new=mocked_queue):
            test_store = DummyStore()

            logger.info('Testing, sending dequeue request to wrong route should throw 403 error')

            self.assertRaisesRegex(HTTPError, '403 Client Error: Forbidden', receive_once, test_store)

    """
    Tests for 429 errors that occur when the wireq receives more requests in a short amount of time than it can handle.
    The max_requests_per_minute parameter of the mock_queue allows 
    changing how many requests the mocked wireq can handle per minute.
    """

    @patch('lib.dequeue_import.BASE_URL', 'https://dummy.co')
    @patch('lib.dequeue_import.sleep', new=no_sleep)
    @responses.activate
    def test_dequeue_429(self):
        files = get_files('data', 10)
        mocked_queue = MockQueue(max_items_returned=1,
                                 max_requests_per_minute=5)
        mocked_queue.add_to_queue(files)
        with patch('lib.dequeue_import.requests', new=mocked_queue):
            test_store = DummyStore()

            logger.info('Testing, sending more requests within a minute than specified in max_requests_per_minute '
                        'should throw 429 error')

            def test_loop():
                while True:
                    receive_once(test_store)
                    if not mocked_queue.get_queue():
                        break

            self.assertRaisesRegex(HTTPError, '429 Client Error: Too Many Requests', test_loop)


class TestDequeue(unittest.TestCase):

    @patch('lib.dequeue_import.BASE_URL', 'https://dummy.co')
    @patch('lib.dequeue_import.sleep', new=no_sleep)
    @responses.activate
    def test_dequeue_single_request_to_store(self):

        """
        A MockQueue object is constructed by passing it a list of articles to put into the queue
        """

        files = get_files('data', 10)

        logger.info('Testing dequeue')
        logger.info(f'Added {len(files)} entries to wireQ')

        mocked_queue = MockQueue()
        mocked_queue.add_to_queue(files)
        with patch('lib.dequeue_import.requests', new=mocked_queue):
            test_store = DummyStore()

            while True:
                receive_once(test_store)
                if not mocked_queue.get_queue():
                    logger.info('Queue is now empty')
                    break

            logger.info(f'Received {len(test_store.pull())} entries')
            self.assertEqual(files, test_store.pull())
            self.assertEqual(10, len(test_store.pull()))
            self.assertEqual([], mocked_queue.get_queue())

    """
    Parameters of the MockQueue can be changed to fit the desired use. Here the retry_after_more_data and
    retry_after parameters were changed to a known value and then compared to the sleeping times that the queue sends
    back in the test, meaning that both 'retry-after' headers were sent correctly.
    """

    @patch('lib.dequeue_import.BASE_URL', 'https://dummy.co')
    @patch('lib.dequeue_import.sleep')
    @responses.activate
    def test_dequeue_several_requests_to_store(self, get_sleep):
        files = get_files('data', 20)
        mocked_queue = MockQueue(retry_after_more_data=5,
                                 retry_after=10)
        mocked_queue.add_to_queue(files)

        logger.info(f'Added {len(files)} entries to wireQ')

        with patch('lib.dequeue_import.requests', new=mocked_queue):
            sleep_durations = []
            test_store = DummyStore()
            get_sleep.returnValue = None

            def get_duration(duration):
                """
                This function is patched as a side effect to the sleep function. It allows the capturing of sleep times
                devised from the retry-after header, storing them in the sleep_durations list.
                :param duration: The duration the software is supposed to sleep in seconds
                :return: None
                """
                sleep_durations.append(duration)

            get_sleep.side_effect = get_duration

            while True:
                receive_once(test_store)
                if not mocked_queue.get_queue():
                    break

            logger.info(f'Received {len(test_store.pull())} entries in {len(sleep_durations)} polls')
            self.assertEqual(files, test_store.pull())
            self.assertEqual(20, len(test_store.pull()))
            self.assertEqual([], mocked_queue.get_queue())
            self.assertEqual([5, 10], sleep_durations)

    """
    A wireq may be filled during polling. This test simulates data being added to a wireq between polls. 
    """

    @patch('lib.dequeue_import.BASE_URL', 'https://dummy.co')
    @patch('lib.dequeue_import.sleep')
    @responses.activate
    def test_dequeue_add_more_files_during_polling(self, get_sleep):
        files = get_files('data', 20)
        mocked_queue = MockQueue(retry_after_more_data=5,
                                 retry_after=10)
        mocked_queue.add_to_queue(files)

        logger.info(f'Added {len(files)} entries to wireQ')

        with patch('lib.dequeue_import.requests', new=mocked_queue):
            sleep_durations = []
            test_store = DummyStore()
            get_sleep.returnValue = None

            def get_duration(duration):
                sleep_durations.append(duration)

            get_sleep.side_effect = get_duration

            while True:
                receive_once(test_store)
                if len(sleep_durations) == 1:
                    more_files = get_files('data', 8)
                    logger.info(f'Added a further {len(more_files)} entries to wireQ')
                    mocked_queue.add_to_queue(more_files)
                if not mocked_queue.get_queue():
                    break

        logger.info(f'Received {len(test_store.pull())} entries in {len(sleep_durations)} polls')
        self.assertEqual(28, len(test_store.pull()))
        self.assertEqual([], mocked_queue.get_queue())
        self.assertEqual([5, 5, 10], sleep_durations)


class TestDequeueLoading(unittest.TestCase):

    @patch('lib.dequeue_import.BASE_URL', 'https://dummy.co')
    @responses.activate
    def test_dequeue_several_requests_loading(self):
        files = get_files('data', 15)
        mocked_queue = MockQueue(
            retry_after_more_data=5,
            retry_after=10)

        logger.info('Testing dequeue with loading times')
        logger.info(f'added {len(files)} entries to WireQ')
        mocked_queue.add_to_queue(files)
        with patch('lib.dequeue_import.requests', new=mocked_queue):
            """
            If the sleep-function is not patched with a different function, real sleep times can be simulated
            """
            test_store = DummyStore()

            while True:
                receive_once(test_store)
                if not mocked_queue.get_queue():
                    break

            logger.info(f'Received {len(test_store.pull())} entries')
            self.assertEqual(files, test_store.pull())
            self.assertEqual(15, len(test_store.pull()))
            self.assertEqual([], mocked_queue.get_queue())


class TestGetDeleteErrors(unittest.TestCase):
    """Tests for 404 errors by using the wrong BASE_URL (dumy.co instead of dummy.co)"""
    mocked_time = MockedTime

    @responses.activate
    def test_get_delete_404(self):
        mocked_queue = MockQueue([])

        test_store = DummyStore

        logger.info('Testing, sending GET request to wrong base-url should throw 404 error')

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url='https://dumy.co', importer=mock_importer)
        receiver.session = mocked_queue
        console = io.StringIO()
        sys.stdout = console
        receiver.receive_and_import_once()
        sys.stdout = sys.__stdout__
        assert '404 Client Error: Not Found' in console.getvalue()

    """Tests for 403 errors by using the correct BASE_URL but an unknown route instead of just 'dequeue-entries.json'"""

    @responses.activate
    def test_get_delete_403(self):
        mocked_queue = MockQueue([])

        test_store = DummyStore

        logger.info('Testing, sending GET request to wrong route should throw 403 error')

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url='https://dummy.co/notarealroute', importer=mock_importer)
        receiver.session = mocked_queue
        console = io.StringIO()
        sys.stdout = console
        receiver.receive_and_import_once()
        sys.stdout = sys.__stdout__
        assert '403 Client Error: Forbidden' in console.getvalue()

    """
    Tests for 410 errors that occur, when trying to delete an article in the wireq, that has already been deleted.
    Articles are deleted as a standard part of the 'receive_once'-function within get_delete_import.py
    """

    @patch('lib.get_delete_import.time.sleep', new=no_sleep)
    @responses.activate
    def test_get_delete_410_double(self):
        files = get_files('data', 1)
        mocked_queue = MockQueue()
        mocked_queue.add_to_queue(files)

        test_store = DummyStore()

        logger.info('Testing, sending DELETE request with an already deleted entry should yield 410 error')

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url='https://dummy.co', importer=mock_importer)
        receiver.session = mocked_queue
        receiver.receive_and_import_once()

        entry = test_store.pull()[0]
        receipt = entry.get('_wireq_receipt')
        url = f'https://dummy.co/entry/{receipt}'

        def delete_article():
            response = mocked_queue.delete(url, 10)
            response.raise_for_status()

        self.assertRaisesRegex(HTTPError, '410 Client Error: Gone', delete_article)

    """
    Tests for 429 errors that occur when the wireq receives more requests in a short amount of time than it can handle.
    The max_requests_per_minute parameter of the mock_queue allows 
    changing how many requests the mocked wireq can handle per minute.
    """

    @patch('lib.get_delete_import.time.sleep', new=no_sleep)
    @responses.activate
    def test_get_delete_429(self):
        files = get_files('data', 10)
        mocked_queue = MockQueue(
            max_items_returned=1,
            max_requests_per_minute=5)
        mocked_queue.add_to_queue(files)

        test_store = DummyStore()

        logger.info('Testing, sending more requests within a minute than specified in max_requests_per_minute '
                    'should throw 429 error')

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url='https://dummy.co', importer=mock_importer)
        receiver.session = mocked_queue

        def test_loop():
            while True:
                receiver.receive_and_import_once()
                if not mocked_queue.get_queue():
                    break

            self.assertRaisesRegex(HTTPError, '429 Client Error: Too Many Requests', test_loop)


class TestGetDelete410Receipt(unittest.TestCase):
    mocked_time = MockedTime

    """
    Tests for 410 errors that occur, when trying to delete an article in the wireq,
    after the receipt to delete the article has already expired.
    It is segmented to a different class than the other tests because it involves sleep times to let the receipt expire.
    """

    @patch('lib.get_delete_import.time.sleep', new=no_sleep)
    @responses.activate
    def test_get_delete_410_receipt_expire(self):
        files = get_files('data', 2)
        mocked_queue = MockQueue(receipt_lifetime_duration=1)
        mocked_queue.add_to_queue(files)

        test_store = DummyStore()

        logger.info('Testing, sending DELETE request with an expired receipt should yield 410 error')

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url='https://dummy.co', importer=mock_importer)
        receiver.session = mocked_queue

        def test_loop():
            for entry in receiver.receive_entries_with_retry_hint():
                entry_id = entry['entry_id']

                if entry_id not in receiver.seen:
                    should_retry_import = receiver.import_with_retry(entry)

                    if should_retry_import:
                        continue

                    receiver.seen.add(entry_id)
                else:
                    logger.info(f'skipping import for duplicate entry {entry_id}')

            sleep(2)
            receiver.delete_from_queue(entry)

        console = io.StringIO()
        sys.stdout = console
        test_loop()
        sys.stdout = sys.__stdout__
        assert '410 Client Error: Gone' in console.getvalue()
        self.assertEqual(len(mocked_queue.get_queue()), 2)


class TestGetDelete(unittest.TestCase):
    mocked_time = MockedTime

    @patch('lib.get_delete_import.time.sleep', new=no_sleep)
    @responses.activate
    def test_get_delete(self):
        files = get_files('data', 15)
        mocked_queue = MockQueue()
        mocked_queue.add_to_queue(files)

        test_store = DummyStore()
        logger.info('Testing get_delete')
        logger.info(f'Added {len(files)} entries to wireQ')

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url='https://dummy.co', importer=mock_importer)
        receiver.session = mocked_queue

        while True:
            receiver.receive_and_import_once()
            if not mocked_queue.get_queue():
                break

        test_files = []
        for entry in files:
            entry.pop('_wireq_receipt', None)
            test_files.append(entry)

        entries_without_receipts = []
        for entry in test_store.pull():
            entry.pop('_wireq_receipt', None)
            entries_without_receipts.append(entry)

        logger.info(f'Received {len(test_store.pull())} entries from wireQ')
        self.assertEqual(len(test_store.pull()), 15)
        self.assertEqual(mocked_queue.get_queue(), [])
        logger.info('Entries were successfully deleted from wireQ')
        self.assertEqual(entries_without_receipts, test_files)

    @patch('lib.get_delete_import.MAX_INNER_POLLS', 10)
    @patch('lib.get_delete_import.time.sleep')
    @responses.activate
    def test_get_delete_sleep_durations(self, get_sleep):
        files = get_files('data', 20)
        mocked_queue = MockQueue(
            retry_after_more_data=5,
            retry_after=10)

        mocked_queue.add_to_queue(files)

        sleep_durations = []
        test_store = DummyStore()

        logger.info('Testing get_delete')
        logger.info(f'Added {len(files)} entries to wireQ')

        get_sleep.returnValue = None

        def get_duration(duration):
            """
            This function is patched as a side effect to the sleep function. It allows the capturing of sleep times
            devised from the retry-after header, storing them in the sleep_durations list.
            :param duration: The duration the software is supposed to sleep in seconds
            :return: None
            """
            sleep_durations.append(duration)

        get_sleep.side_effect = get_duration

        def mock_importer(url, entry):
            test_store.put_single_entry(entry)

        receiver = WireqReceiver(base_url='https://dummy.co', importer=mock_importer)
        receiver.session = mocked_queue

        while True:
            receiver.receive_and_import_once()
            if not mocked_queue.get_queue():
                break

        entries_without_receipts = []
        for entry in test_store.pull():
            entry.pop('_wireq_receipt', None)
            entries_without_receipts.append(entry)

        relevant_sleep_durations = [
                                       round(sleep_time) for sleep_time in sleep_durations if sleep_time != 0.5
                                   ][:2]
        logger.info(f'Received {len(test_store.pull())} entries from wireQ in {len(relevant_sleep_durations)} polls')
        self.assertEqual(len(test_store.pull()), 20)
        self.assertEqual(mocked_queue.get_queue(), [])
        logger.info('Entries were successfully deleted from wireQ')
        self.assertEqual(entries_without_receipts, files)
        self.assertEqual(relevant_sleep_durations, [5, 10])


class TestMixedImports(unittest.TestCase):
    mocked_time = MockedTime

    @patch('lib.dequeue_import.BASE_URL', 'https://dummy.co')
    @patch('lib.dequeue_import.sleep', no_sleep)
    @patch('lib.get_delete_import.time', new=mocked_time)
    @patch('lib.get_delete_import.MAX_INNER_POLLS', 1)
    @responses.activate
    def test_mixed_imports(self):
        files = get_files('data', 22)
        mocked_queue = MockQueue(
            retry_after_more_data=5,
            retry_after=10)
        mocked_queue.add_to_queue(files)

        logger.info("Testing mixed imports")
        logger.info(f'Added {len(files)} entries to wireQ')

        with patch('lib.dequeue_import.requests', mocked_queue):
            test_store = DummyStore()

            def mock_importer(url, entry):
                test_store.put_single_entry(entry)

            receiver = WireqReceiver(base_url='https://dummy.co', importer=mock_importer)
            receiver.session = mocked_queue

            entries = receiver.receive_entries_with_retry_hint()
            entries_without_receipts = []
            receipts = []

            for entry in entries:
                receipts.append(entry.get('_wireq_receipt'))
                entry.pop('_wireq_receipt', None)
                entries_without_receipts.append(entry)

            test_store.put(entries_without_receipts)

            logger.info(f'Received {len(test_store.pull())} entries from GET operation')

            while True:
                receive_once(test_store)
                if not mocked_queue.show_visible_queue():
                    break

            logger.info(f'Received a combined total of {len(test_store.pull())} entries from GET and dequeue operation')
            for receipt in receipts:
                mocked_queue.delete(f'https://dummy.co/entry/{receipt}', 100)
                logger.info(f'Deleted https://dummy.co/entry/{receipt}')

            self.assertEqual(len(test_store.pull()), 22)
            self.assertEqual(mocked_queue.get_queue(), [])
            logger.info('wireQ is empty')
            self.assertEqual(test_store.pull(), files)


if __name__ == '__main__':
    unittest.main()
