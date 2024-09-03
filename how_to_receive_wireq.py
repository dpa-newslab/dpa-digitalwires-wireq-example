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

"""
How to receive articles in dpa-digitalwires format via wireq-API?

Code example with test -- This file is an executable test!

* Matching with the database:
  * which articles are new
  * which articles are an update
  * and which ones do you have to read over because they are older than the
    articles in the database

Then:
  Optimization: group multiple entries in advance and clearly omit obsolete
  ones without asking the database each time.

Outlook:
  The same with SQL

  Why are there different attributes at all that decide about the version of
  an article?
  Where are the differences between version, version_created and updated?
"""

from itertools import groupby
from operator import itemgetter
from pprint import pprint
import os
import requests
from time import sleep

BASE_URL = os.environ['BASE_URL'].strip('/')

events = []


def log(msg):
    events.append(msg)


test_results = {}


def receive_forever(database, poll_interval=30):
    """
    Normally you receive from the wireq-API as follows:
    """
    while True:
        response = requests.post(f'{BASE_URL}/dequeue-entries.json')
        response.raise_for_status()
        res = response.json()
        retry_after = response.headers.get('retry-after')
        process_wireq_entries(res['entries'], database)
        t = poll_interval if retry_after is None else retry_after
        sleep(t)


def process_wireq_entries(entries, database):
    """
    This method inserts several received entries into the database -- in such a
    way that new articles are added and already existing articles are updated
    only if the received article is newer.
    """
    log('processing entries')
    for entry in entries:
        entry_id = entry['entry_id']
        urn = entry['urn']

        if have_seen(entry_id):
            log(f'received duplicate entry, skipping {entry_id}')
            continue

        if urn not in database:
            log(f'inserting new entry into database {urn}')
            database[urn] = entry
        else:
            existing_entry = database[urn]
            latest_entry = latest(existing_entry, entry)

            if entry is latest_entry:
                log(f'updating entry in database {urn} {entry_id}')
                database[urn] = entry

            else:
                log(f'entry in database for {urn} is more recent, skipping received entry {entry_id}')


seen = set()


def have_seen(entry_id):
    """
    Optional test for trivial multiple transfers:
    If entries differ, then their entry_ids differ.
    This way you can detect duplicate entries and omit them.
    """
    if seen is None:
        return False
    result = (entry_id in seen)
    seen.add(entry_id)
    return result


def latest(e1, e2):
    """
    Which entry is the latest if both entries have the same urn?
    This is determined by the attributes version, version_created and updated.
    First priority has version, then follows version_created if both entries
    have the same urn and then updated.

    You can rank two given entries according to these criteria:
    by sorting by each criterion (in ascending priority of the criterion).
    ascending priority of the criterion).
    """
    assert e1['urn'] == e2['urn']
    entries = [e1, e2]
    entries = sorted(entries, key=itemgetter('updated'))
    entries = sorted(entries, key=itemgetter('version_created'))
    entries = sorted(entries, key=itemgetter('version'))
    return entries[1]


def some_responses_of_wireq_post_dequeue_entries():
    """
    For our test, we define some exemplary content to be received.
    Thereby some interesting constellations shall be played through.
    """
    yield {
        'entries': [
            {
                'urn': 'urn-1',
                'version': 2,  # newer than in the database
                'version_created': '2022-02-01T12:00:00+01',
                'updated': '2022-02-01T11:00:25Z',
                'entry_id': 'e-szex6'
            },
            {
                'urn': 'urn-2',  # not yet in the database
                'version': 4,
                'version_created': '2021-12-12T14:00:00+01',
                'updated': '2022-03-01T13:46:56Z',
                'entry_id': 'e-a8ewb'
            },
            {
                'urn': 'urn-1',
                'version': 3,    # more recent than e-szex6
                'version_created': '2022-02-01T10:00:00+01',
                'updated': '2022-03-01T09:00:27Z',
                'entry_id': 'e-as1eb'
            },
            {
                'urn': 'urn-1',
                'version': 3,    # same version as e-as1eb...
                'version_created': '2022-02-01T13:00:00+01',  # ...but more recent
                'updated': '2022-03-01T12:10:00Z',
                'entry_id': 'e-9qc8n'
            },
            {
                'urn': 'urn-1',
                'version': 3,   # same version
                'version_created': '2022-02-01T13:00:00+01',  # same again
                'updated': '2022-03-01T12:00:27Z',  # but older than e-9qc8n
                'entry_id': 'e-wlaif'
            }
        ]}
    yield {
        'entries': [
            {
                'urn': 'urn-1',
                'version': 3,
                'version_created': '2022-02-01T13:00:00+01',
                'updated': '2022-03-01T12:00:27Z',
                'entry_id': 'e-wlaif'  # has been received before, but is not in the database
            },
            {
                'urn': 'urn-1',
                'version': 3,    # has been received before
                'version_created': '2022-02-01T13:00:00+01',  # ...but more recent
                'updated': '2022-03-01T12:10:00Z',
                'entry_id': 'e-9qc8n'  # This duplicate is in the database
            },
            {
                'urn': 'urn-2',
                'version': 3,   # is older than the already received e-a8ewb
                'version_created': '2021-12-11T11:11:11+01',
                'updated': '2022-04-04T00:00:44Z',
                'entry_id': 'e-lwv2'
            }
        ]}
    yield {
        'entries': []
    }


def initial_database_content():
    """
    Our database contains all articles in their latest version.
    Therefore we use "urn" as the primary key. Even before our test, the
    database contains some content:
    """
    return {
        'urn-0':
        {
            'urn': 'urn-0',  # will not be overwritten
        },
        'urn-1':
        {
            'urn': 'urn-1',
            'version': 1,
            'version_created': '2022-01-01T06:00:00+01',
            'updated': '2022-01-01T05:00:30Z'
        }
    }


my_database = initial_database_content()


# Now we receive the defined entries and process them

for response in some_responses_of_wireq_post_dequeue_entries():
    process_wireq_entries(response['entries'], my_database)

test_results[1] = {'db': my_database, 'events': events}

print('\nFirst test -- This is what happened:')
pprint(events)

assert events == [
    'processing entries',
    'updating entry in database urn-1 e-szex6',
    'inserting new entry into database urn-2',
    'updating entry in database urn-1 e-as1eb',
    'updating entry in database urn-1 e-9qc8n',
    'entry in database for urn-1 is more recent, skipping received entry e-wlaif',
    'processing entries',
    'received duplicate entry, skipping e-wlaif',
    'received duplicate entry, skipping e-9qc8n',
    'entry in database for urn-2 is more recent, skipping received entry e-lwv2',
    'processing entries'
]

# What is in my_database?

print('\nThis is the content of the database:')
pprint(my_database)

assert my_database == {
    'urn-0':
    {
        'urn': 'urn-0'
    },
    'urn-1':
    {
        'urn': 'urn-1',
        'version': 3,
        'version_created': '2022-02-01T13:00:00+01',
        'updated': '2022-03-01T12:10:00Z',
        'entry_id': 'e-9qc8n'
    },
    'urn-2':
    {
        'urn': 'urn-2',
        'version': 4,
        'version_created': '2021-12-12T14:00:00+01',
        'updated': '2022-03-01T13:46:56Z',
        'entry_id': 'e-a8ewb'
    }
}


"""
What do we see?
* reached correct version level in database
* Articles with older version (e-wlaif, e-lwv2) were successfully prevented
  from overwriting the latest version in the DB
* some articles were overwritten several times (urn-1 durch e-szex6, e-as1eb,
  e-9qc8n)
"""

"""
Second Test
Optimize write database accesses
Could we avoid entries that will be overwritten in the database anyway in the
first place?
"""


def keep_only_latest_versions(entries):
    """
    Group entries by urn and in each group take only the latest version
    """
    entries = sorted(entries, key=itemgetter('updated'))
    entries = sorted(entries, key=itemgetter('version_created'))
    entries = sorted(entries, key=itemgetter('version'))
    entries = sorted(entries, key=itemgetter('urn'))
    for urn, group in groupby(entries, itemgetter('urn')):
        # the latest version is at the end of each urn group
        yield list(group)[-1]


"""
What happens when the optimized variant receives data?
"""
# We reset the database, the duplicate identifier and the event log to the
# original state
my_database = initial_database_content()
events = []
seen = set()

# ...and start processing
for response in some_responses_of_wireq_post_dequeue_entries():
    entries = keep_only_latest_versions(response['entries'])
    process_wireq_entries(entries, my_database)

test_results[2] = {'db': my_database, 'events': events}

# the effect of the import, i.e. the state of the database, is the same as in
# the previous test
assert test_results[1]['db'] == test_results[2]['db']

print('\nSecond Test -- This is what happened with the optimized processing:')
pprint(events)

assert events == [
    'processing entries',
    'updating entry in database urn-1 e-9qc8n',
    'inserting new entry into database urn-2',
    'processing entries',
    'received duplicate entry, skipping e-9qc8n',
    'entry in database for urn-2 is more recent, skipping received entry e-lwv2',
    'processing entries'
]

"""
We can see that only the two necessary write database operations has been
performed -- an update (urn-1) and an insert (urn-2).
"""
