#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2025 dpa-IT Services GmbH
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
Use case:

Radio customers want to receive licensed audio content in dpa-digitalwires
format via wireq-API

This basic Python example polls the wireq-API to request audio content
(including metadata). A file pair (an mp3 audio file and a json file including
the dpa-digitalwires entry) is written to the local storage.

Important: the script requires a custom `BASE_URL`. Please copy the `base-URL`
from your setup in the [API-Portal](https://api-portal.dpa-newslab.com).
"""


import os
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from time import sleep
import json
from os import makedirs
from os.path import join as pjoin, exists, basename
from urllib.parse import urlparse
from urllib3.util import Retry
import string
import random


BASE_URL = os.environ["BASE_URL"].strip("/")


def receive_forever(poll_interval=120):
    """
    Normally you receive from the wireq-API as follows:
    """
    while True:
        response = requests.post(f"{BASE_URL}/dequeue-entries.json")
        response.raise_for_status()
        res = response.json()
        entries = res["entries"]
        retry_after = response.headers.get("retry-after")
        t = poll_interval if retry_after is None else int(retry_after)
        if entries:
            save_wireq_entries(res["entries"])
            print(f"Saved {len(entries)} entries, sleep for {t} seconds...\n")
        else:
            print(f"Queue empty, sleep for {t} seconds...\n")
        sleep(t)


def save_wireq_entries(entries, output_dir="./outdir/"):
    """
    Each received dpa-digitalwires entry is saved in a separate file in the
    given output directory (`outdir`). The corresponding audio file(s) are
    downloaded and saved to the same directory.
    """
    for entry in entries:
        if not exists(output_dir):
            makedirs(output_dir)
        entry_name = "dpa-{}.json".format(entry.get("entry_id"))
        entry_path = pjoin(output_dir, entry_name)
        with open(entry_path, "wb") as f:
            f.write(json.dumps(entry, indent=2).encode("utf-8"))
            print(f"wrote {entry_path}")

        audio_session = Session()
        retry = Retry(
            total=3,
            status_forcelist=[404, 429, 500, 502, 503, 504],
            respect_retry_after_header=True,
            backoff_factor=1,
        )
        audio_session.mount("https://", HTTPAdapter(max_retries=retry))

        for assoc in entry.get("associations", []):
            if assoc.get("type") in ["audio"]:
                for rendition in assoc.get("renditions", []):
                    if rendition.get("url") is None:
                        print(f"warning: missing url for rendition {rendition}")
                        continue
                    audio_url = rendition.get("url")
                    audio_name = "dpa-{}-{}-{}".format(
                        entry.get("entry_id"),
                        "".join(c for c in entry.get("headline") if c.isalnum()),
                        basename(urlparse(audio_url).path)
                        or "".join(
                            random.choice(string.ascii_lowercase) for i in range(16)
                        ),
                    )
                    audio_path = pjoin(output_dir, audio_name)
                    with open(audio_path, "wb") as f:
                        r = audio_session.get(audio_url, stream=True)
                        try:
                            r.raise_for_status()
                        except Exception as e:
                            print(
                                f"error getting audio media, will be retried later {e}"
                            )
                            continue
                        written = 0
                        for chunk in r.iter_content(chunk_size=10 * 2**20):
                            print(".", end="", flush=True)
                            f.write(chunk)
                            written += len(chunk)
                        print(f"wrote audio {audio_path} ({written} bytes)")


if __name__ == "__main__":
    # We receive forever using default polling interval
    receive_forever()
