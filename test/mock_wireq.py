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

import uuid
import requests
import responses
import time
from copy import deepcopy
from dataclasses import dataclass, field


@dataclass
class MockQueue:
    queue: list = field(default_factory=list)
    hidden_queue: list = field(default_factory=list)
    deleted_urls: list = field(default_factory=list)
    request_timestamps: list = field(default_factory=list)
    timed_out_receipts: list = field(default_factory=list)

    """
    The maximum number of articles returned via a single poll.
    For usual wireq implementations, this value is set to 50.
    The default value is lowered for easier testing purposes.
    """
    max_items_returned: int = 10

    """
    The value of the 'retry-after' response header if no items are in the queue after a poll.
    For usual wireq implementations, this value is set to 50.
    """
    retry_after: int = 100
    """
    The value of the 'retry-after' response header if more items are in the queue after a poll.
    For usual wireq implementations, this value is set to 10.
    """
    retry_after_more_data: int = 10
    """
    The value of the 'retry-after' response header if too many requests have been made to the 
    wireq-api within a short amount of time.
    For usual wireq implementations, this value is set to 3600.
    """
    retry_after_too_many_requests: int = 360
    """
    How many requests the simulated wireq can process a minute before responding with a 429 'too many requests' error
    """
    max_requests_per_minute: int = 30

    receipt_lifetime_duration: int = 200

    def check_hidden_entry_lifetime(self):
        """
        Checks every element of the hidden_queue to see if any of the wireQ-receipts have timed out. If the lifetime
        defined via receipt_lifetime_duration is exceeded, the element is removed from the hidden_queue and its receipt
        is added to the list of timed out receipts.
        :return:
        """
        current_time = time.time()

        for h_entry in self.hidden_queue[:]:
            difference = current_time - h_entry.get('timestamp')

            if difference > self.receipt_lifetime_duration:
                for entry in self.queue:
                    if entry.get('entry') == h_entry.get('entry'):
                        entry['visible'] = True
                        self.hidden_queue.remove(h_entry)
                        self.timed_out_receipts.append(h_entry.get('receipt'))

    def show_visible_queue(self):
        """
        :return: Returns the list of elements that are visible in the wireQ
        """
        return [entry.get('entry') for entry in self.queue if entry.get('visible')]

    def length_visible_queue(self):
        """
        :return: Returns the length of the list of visible items in the wireQ
        """
        visible_list = [
            entry.get('entry') for entry in self.queue if entry.get('visible')
        ]
        return len(visible_list)

    def dequeue_first_x_visible_items(self, number_of_items):
        """
        Finds the first x visible items where x is the number_of_items parameter. These items are removed from the wireQ
        and are returned.
        The timestamp of each item being processed on the wireQ is also added to the requests_timestamp list
        :param number_of_items: The number of items to maximally return
        :return: A list of wireQ-elements
        """
        current_time = time.time()
        first_x_visible_items = [item for item in self.queue if item['visible']][0:number_of_items]

        for item in first_x_visible_items:
            self.request_timestamps.append(current_time)
            self.queue.remove(item)

        entries_to_return = [item.get('entry') for item in first_x_visible_items]

        return entries_to_return

    def get_first_x_visible_items(self, number_of_items):
        """
        Finds the first x visible items where x is the number_of_items parameter. These items are given
        a wireQ-receipt valid for a certain amount of time (set via receipt_lifetime_duration). These items with
        the receipts are returned. The items within the wireQ are set to invisible, and they are also appended to
        the hidden_queue with the timestamp of when they were added.
        The timestamp of each item being processed on the wireQ is also added to the requests_timestamp list
        :param number_of_items: The number of items to maximally return
        :return: A list of wireQ-elements, each with an added wireQ-receipt
        """
        current_time = time.time()
        first_x_visible_items = [item for item in self.queue if item['visible']][0:number_of_items]
        entries_to_return = []

        for item in first_x_visible_items:
            self.request_timestamps.append(current_time)

            item['visible'] = False

            hashed = str(uuid.uuid4())
            copied_entry = deepcopy(item.get('entry'))
            copied_entry['_wireq_receipt'] = hashed

            self.hidden_queue.append(
                {'entry': item.get('entry'), 'receipt': hashed, 'timestamp': current_time})

            entries_to_return.append(copied_entry)

        return entries_to_return

    def get_queue(self):
        return self.queue

    def add_to_queue(self, entries):
        for entry in entries:
            self.queue.append({'entry': entry, 'visible': True})

    def too_many_requests(self):
        """
        Whenever a post or get request is made to the queue, its timestamp is added to the 'request-timestamps' list.
        The too_many_requests function checks if there have been more requests in the last minute than is allowed via
        'max_requests_per_minute', returning "True" if the maximum number of requests has been reached and "False" if
        not

        Requests that are older than one minute are then removed from the list.
        :return: Returns a boolean indicating whether the maximum number of requests has been reached
        """
        current_time = time.time()
        valid_timestamps = [
            timestamp for timestamp in self.request_timestamps if
            current_time - timestamp < 60
        ]
        self.request_timestamps = valid_timestamps
        if len(valid_timestamps) > self.max_requests_per_minute:
            return True
        else:
            return False

    def wireq_dequeue_operation(self):
        """
        Executes the dequeue operation on the wireQ. Returns a number of elements from the wireQ as JSON data. The major
        difference compared to the get operation is that the returned elements from are automatically deleted
        from the wireQ with no further requests or input required.
        :return: Returns a dict made up of the http-status of the request as well as the JSON data and headers to
        be used in the response
        """
        self.check_hidden_entry_lifetime()
        len_visible_queue = self.length_visible_queue()
        if self.too_many_requests():

            """
            Returns a 429 error if the the request per minute limit specified in 'max_requests_per_minute' is reached.
            Includes a 'retry-after' header specifying the time after which the requester may try sending another 
            request.
            """
            responses.post('https://dummy.co/dequeue-entries.json',
                           json={},
                           status=429,
                           headers={'retry-after': str(self.retry_after_too_many_requests)})
        elif len_visible_queue <= self.max_items_returned:

            """
            Returns all elements of the queue if there are less elements in it than specified in 'max_items_returned'.
            Includes a 'retry-after' header specifying the time after which the next polling attempt should start.
            """
            entries = self.dequeue_first_x_visible_items(self.max_items_returned)
            responses.post('https://dummy.co/dequeue-entries.json',
                           json={'entries': entries},
                           status=200,
                           headers={'retry-after': str(self.retry_after)})
        elif len_visible_queue > self.max_items_returned:

            """
            Returns as many elements of the queue as specified in 'max_items_returned'.
            Includes a 'retry-after' header specifying the time after which the next polling attempt should start to get
            more of the queue elements.
            """
            entries = self.dequeue_first_x_visible_items(self.max_items_returned)
            responses.post('https://dummy.co/dequeue-entries.json',
                           json={'entries': entries},
                           status=200,
                           headers={'retry-after': str(self.retry_after_more_data)})

    def wireq_get_operation(self):
        """
        Executes the get operation on the wireQ. Returns a number of elements from the wireQ as JSON data. The major
        difference compared to the dequeue operation is that the returned elements aren't automatically deleted
        from the wireQ but instead have to be removed via a separate DELETE request
        :return: Returns a dict made up of the http-status of the request as well as the JSON data and headers to
        be used in the response
        """
        self.check_hidden_entry_lifetime()
        len_visible_queue = self.length_visible_queue()
        if self.too_many_requests():
            responses.post('https://dummy.co/entries.json',
                           json={},
                           status=429,
                           headers={'retry-after': str(self.retry_after_too_many_requests)})
        elif len_visible_queue <= self.max_items_returned:
            entries = self.get_first_x_visible_items(self.max_items_returned)
            responses.post('https://dummy.co/entries.json',
                           json={'entries': entries},
                           status=200,
                           headers={'retry-after': str(self.retry_after)})
        elif len_visible_queue > self.max_items_returned:
            entries = self.get_first_x_visible_items(self.max_items_returned)
            responses.post('https://dummy.co/entries.json',
                           json={'entries': entries},
                           status=200,
                           headers={'retry-after': str(self.retry_after_more_data)})

    def wireq_delete_operation(self, url):
        """
        Executes the delete operation on the wireQ. Checks if the element that is asked to delete has already
        been deleted, if the receipt to delete it has already run out and whether the element exists.
        If element exists and has a valid receipt, it is deleted and its url is added to the list of deleted urls
        :param url: The url the delete request was sent to
        :return: The http status of the request
        """
        self.check_hidden_entry_lifetime()
        key = url.split('entry/')[1]
        keys = [entry.get('receipt') for entry in self.hidden_queue]
        if key in self.deleted_urls:
            responses.post(url,
                           json={},
                           status=410,
                           headers={})
        elif key in self.timed_out_receipts:
            responses.post(url,
                           json={},
                           status=410,
                           headers={})
        elif key not in keys:
            responses.post(url,
                           json={},
                           status=404,
                           headers={})
        else:
            for entry in self.hidden_queue:
                if entry.get('receipt') == key:
                    formatted_element = {'entry': entry.get('entry'), 'visible': False}
                    self.queue.remove(formatted_element)
                    self.hidden_queue.remove(entry)
                    self.deleted_urls.append(key)
                    responses.post(url,
                                   json={},
                                   status=204,
                                   headers={})

    def post(self, url, timeout):
        """
        Handles the mocking and responding of POST http-calls. Checks for 404 or 403 errors in the response
        and otherwise proceeds with dequeue behaviour.
        :param url: The url of the http-call the method is mocking
        :param timeout: the timeout of the http-call, only necessary to allow method to mock http-calls made
        in dequeue_import
        :return: Returns a response to the call with status code, JSON data and headers
        """
        if 'https://dummy.co/' not in url:

            """Returns a 404 error if the base url of the request is incorrect"""
            responses.post('https://dummy.co/dequeue-entries.json',
                           json={},
                           status=404,
                           headers={})
        elif 'https://dummy.co/dequeue-entries.json' != url:

            """Returns a 403 error if the url of the request is incorrect, but the base url is correct"""
            responses.post('https://dummy.co/dequeue-entries.json',
                           json={},
                           status=403,
                           headers={})
        else:
            self.wireq_dequeue_operation()
        return requests.post('https://dummy.co/dequeue-entries.json')

    def get(self, url, timeout):
        """
        Handles the mocking and responding of GET http-calls. Checks for 404 or 403 errors in the response
        and otherwise proceeds with get behaviour.
        :param url: The url of the http-call the method is mocking
        :param timeout: the timeout of the http-call, only necessary to allow method to mock http-calls made
        in get_delete_import
        :return: Returns a response to the call with status code, JSON data and headers
        """
        if 'https://dummy.co/' not in url:
            responses.post('https://dummy.co/entries.json',
                           json={},
                           status=404,
                           headers={})
        elif 'https://dummy.co/entries.json' not in url:
            responses.post('https://dummy.co/entries.json',
                           json={},
                           status=403,
                           headers={})
        else:
            self.wireq_get_operation()

        return requests.post('https://dummy.co/entries.json')

    def delete(self, url, timeout):
        """
        Handles the mocking and responding of DELETE http-calls. Checks for 404 or 403 errors in the response
        and otherwise proceeds with delete behaviour.
        :param url: The url of the http-call the method is mocking
        :param timeout: the timeout of the http-call, only necessary to allow method to mock http-calls made
        in get_delete_import
        :return: Returns a response to the call with status code, JSON data and headers
        """
        if 'https://dummy.co/entry' not in url:
            responses.post(url,
                           json={},
                           status=404,
                           headers={})
        else:
            self.wireq_delete_operation(url)
        return requests.post(url)
