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

from http.server import BaseHTTPRequestHandler
from copy import deepcopy

import time
import uuid
import json
import logging

logger = logging.getLogger('server_logger')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

handler.setFormatter(formatter)
logger.addHandler(handler)


class RequestHandler(BaseHTTPRequestHandler):

    """
    The list making up the wireQ, filled with elements that are either visible or hidden.
    """
    queue = []

    """
    The list of hidden elements created by get-operations with their wireQ-receipts and timestamps.
    """
    hidden_queue = []

    """
    The list of the urls of elements deleted from the wireQ via DELETE operations.
    """
    deleted_urls = []

    """
    The list of timestamps of the latest operations on the wireQ. Is used to check the maximum amount of requests
    the wireQ can process per minute.
    """
    request_timestamps = []

    """
    The list of wireQ-receipts that have timed out because no delete operation was done within their lifetime.
    """
    timed_out_receipts = []

    """
    The maximum number of articles returned via a single poll.
    For usual wireq implementations, this value is set to 50.
    The default value is lowered for easier testing purposes.
    """
    max_items_returned = 50

    """
    The value of the 'retry-after' response header if no items are in the queue after a poll.
    For usual wireq implementations, this value is set to 50.
    """
    retry_after = 100
    """
    The value of the 'retry-after' response header if more items are in the queue after a poll.
    For usual wireq implementations, this value is set to 10.
    """
    retry_after_more_data = 10
    """
    The value of the 'retry-after' response header if too many requests have been made to the 
    wireq-api within a short amount of time.
    For usual wireq implementations, this value is set to 3600.
    """
    retry_after_too_many_requests = 360
    """
    How many requests the simulated wireq can process a minute before responding with a 429 'too many requests' error
    """
    max_requests_per_minute = 200

    receipt_lifetime_duration = 200

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
        status = 0
        json_data = ''
        headers = ''

        if self.too_many_requests():
            """
            Returns a 429 error if the the request per minute limit specified in 'max_requests_per_minute' is reached.
            Includes a 'retry-after' header specifying the time after which the requester may try sending another 
            request.
            """
            status = 429
            json_data = {}
            headers = {'retry-after': str(self.retry_after_too_many_requests)}
        elif len_visible_queue <= self.max_items_returned:
            """
            Returns all elements of the queue if there are less elements in it than specified in 'max_items_returned'.
            Includes a 'retry-after' header specifying the time after which the next polling attempt should start.
            """
            entries = self.dequeue_first_x_visible_items(self.max_items_returned)
            status = 200
            json_data = {'entries': entries}
            headers = {'retry-after': str(self.retry_after)}

        elif len_visible_queue > self.max_items_returned:
            """
            Returns as many elements of the queue as specified in 'max_items_returned'.
            Includes a 'retry-after' header specifying the time after which the next polling attempt should start to get
            more of the queue elements.
            """
            entries = self.dequeue_first_x_visible_items(self.max_items_returned)
            status = 200
            json_data = {'entries': entries}
            headers = {'retry-after': str(self.retry_after_more_data)}

        return {'status': status, 'json': json_data, 'headers': headers}

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
        status = 0
        json_data = ''
        headers = ''

        if self.too_many_requests():
            """
            Returns a 429 error if the the request per minute limit specified in 'max_requests_per_minute' is reached.
            Includes a 'retry-after' header specifying the time after which the requester may try sending another 
            request.
            """
            status = 429
            json_data = {}
            headers = {'retry-after': str(self.retry_after_too_many_requests)}

        elif len_visible_queue <= self.max_items_returned:
            """
            Returns all elements of the queue if there are less elements in it than specified in 'max_items_returned'.
            Includes a 'retry-after' header specifying the time after which the next polling attempt should start.
            """
            entries = self.get_first_x_visible_items(self.max_items_returned)
            status = 200
            json_data = {'entries': entries}
            headers = {'retry-after': str(self.retry_after)}

        elif len_visible_queue > self.max_items_returned:
            """
            Returns as many elements of the queue as specified in 'max_items_returned'.
            Includes a 'retry-after' header specifying the time after which the next polling attempt should start to get
            more of the queue elements.
            """
            entries = self.get_first_x_visible_items(self.max_items_returned)
            status = 200
            json_data = {'entries': entries}
            headers = {'retry-after': str(self.retry_after_more_data)}

        return {'status': status, 'json': json_data, 'headers': headers}

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
        status = 0
        if key in self.deleted_urls:
            status = 410

        elif key in self.timed_out_receipts:
            status = 410

        elif key not in keys:
            status = 404
        else:
            for entry in self.hidden_queue:
                if entry.get('receipt') == key:
                    formatted_element = {'entry': entry.get('entry'), 'visible': False}
                    self.queue.remove(formatted_element)
                    self.hidden_queue.remove(entry)
                    self.deleted_urls.append(key)
                    logger.info(f'Delete successful for {url}')
                    status = 204

        return status

    def do_POST(self):
        """
        Handle incoming POST-request and send response with correct body, status and header
        """
        logger.info('POST request received')
        if self.path == '/dequeue-entries.json':
            response = self.wireq_dequeue_operation()
            status, json_data, headers = response['status'], response['json'], response['headers']
            self.send_response(status)
            self.send_header('retry-after', headers['retry-after'])
            self.end_headers()
            self.wfile.write(json.dumps(json_data).encode())
        else:
            self.send_error(403)
            self.end_headers()

    def do_GET(self):
        """
        Handle incoming GET-request and send response with correct body, status and header
        """
        logger.info('GET request received')
        if self.path == '/entries.json':
            response = self.wireq_get_operation()
            status, json_data, headers = response['status'], response['json'], response['headers']
            self.send_response(status)
            self.send_header('retry-after', headers['retry-after'])
            self.end_headers()
            self.wfile.write(json.dumps(json_data).encode())
        else:
            self.send_error(403)
            self.end_headers()

    def do_DELETE(self):
        """
        Handle incoming DELETE-request and send response with correct body, status and header
        """
        logger.info('DELETE request received')
        if 'entry/' in self.path:
            status = self.wireq_delete_operation(self.path)
            self.send_response(status)
            self.end_headers()
        else:
            self.send_error(403)
            self.end_headers()
