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

import json
from os import makedirs
from os.path import join as pjoin, basename, exists
from requests import Session
from requests.adapters import HTTPAdapter
from urllib.parse import urlparse
from urllib3.util import Retry


def session_with_exponential_backoff():
    s = Session()
    retry = Retry(total=3,
                  status_forcelist=[404, 429, 500, 502, 503, 504],
                  respect_retry_after_header=True,
                  backoff_factor=1)
    s.mount('https://', HTTPAdapter(max_retries=retry))
    return s


def import_entry_with_assets(output_dir, entry):
    print(f'processing {entry.get("urn")} {entry.get("version")} {entry.get("updated")} {entry.get("headline")}')

    entry_name = "{}-{}-{}".format(
        entry.get("version_created"),
        entry.get("urn"),
        entry.get("entry_id"))
    entry_path = pjoin(output_dir, entry_name)

    if not exists(entry_path):
        makedirs(entry_path)

    with open(pjoin(entry_path, 'entry.json'), 'wb') as f:
        f.write(json.dumps(entry, indent=2).encode('utf-8'))
        print(f'wrote {pjoin(entry_path, "entry.json")}')

    asset_session = session_with_exponential_backoff()

    for assoc in entry.get('associations', []):
        if assoc.get('type') in ['image', 'audio', 'video']:
            for rendition in assoc.get('renditions', []):

                if rendition.get('url') is None:
                    print(f'warning: missing url for rendition {rendition}')
                    continue

                outfile = basename(urlparse(rendition['url']).path) or ''.join(random.choice(string.ascii_lowercase) for i in range(16))

                outpath = pjoin(entry_path, outfile)

                with open(outpath, 'wb') as f:
                    r = asset_session.get(rendition['url'], stream=True)
                    try:
                        r.raise_for_status()
                    except Exception as e:
                        print(f'error getting media, will be retried later {e}')
                        continue

                    written = 0
                    for chunk in r.iter_content(chunk_size=10*2**20):
                        print('.', end='', flush=True)
                        f.write(chunk)
                        written += len(chunk)

                print(f'wrote {outpath} ({written} bytes)')
