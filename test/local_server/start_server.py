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

import os
from http.server import HTTPServer
from .wireq_fake_server import RequestHandler
from ..generate_data import generate_files
from dotenv import load_dotenv

load_dotenv()

NUMBER_OF_FILES = int(os.environ.get('NUMBER_OF_FILES'))
MAX_ITEMS_RETURNED = int(os.environ.get('MAX_ITEMS_RETURNED'))
RETRY_AFTER = int(os.environ.get('RETRY_AFTER'))
RETRY_AFTER_MORE_DATA = int(os.environ.get('RETRY_AFTER_MORE_DATA'))
RETRY_AFTER_TOO_MANY_REQUESTS = int(os.environ.get('RETRY_AFTER_TOO_MANY_REQUESTS'))
MAX_REQUESTS_PER_MINUTE = int(os.environ.get('MAX_REQUESTS_PER_MINUTE'))
WIREQ_RECEIPT_LIFETIME_DURATION = int(os.environ.get('WIREQ_RECEIPT_LIFETIME_DURATION'))
PORT = int(os.environ.get('PORT'))

if __name__ == "__main__":
    files = generate_files(NUMBER_OF_FILES)
    RequestHandler.queue = [{'entry': file, 'visible': True} for file in files]

    RequestHandler.max_items_returned = MAX_ITEMS_RETURNED
    RequestHandler.retry_after = RETRY_AFTER
    RequestHandler.retry_after_more_data = RETRY_AFTER_MORE_DATA
    RequestHandler.retry_after_too_many_requests = RETRY_AFTER_TOO_MANY_REQUESTS
    RequestHandler.max_requests_per_minute = MAX_REQUESTS_PER_MINUTE
    RequestHandler.receipt_lifetime_duration = WIREQ_RECEIPT_LIFETIME_DURATION

    httpd = HTTPServer(('localhost', PORT), RequestHandler)
    print(f'Local server serving on: http://{httpd.server_address[0]}:{httpd.server_address[1]}')
    httpd.serve_forever()
