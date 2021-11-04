# -*- coding: utf-8 -*-
#
# Copyright 2021, 2021 dpa-IT Services GmbH
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

import base64
import hashlib
import json
import os
import pathlib

try:
    THIS_DIR = os.path.dirname(__file__)
except NameError:
    THIS_DIR = "./"


def get_unique_article_name(article):
    """ Return unique key without suffix. """
    content_hsh = hashlib.sha1(json.dumps(article).encode("utf-8")).digest()
    key_hash = base64.b32encode(content_hsh).decode("ascii").lower()
    return "{}-{}-{}".format(article.get("version_created"),
                             article.get("urn"),
                             key_hash)


def write_article_to_disk(article):
    """ Write given article to local disk.
    Return True, if successful else False """
    filename = "{}.json".format(get_unique_article_name(article))
    dpa_dirname = "dpa-content"
    path = os.path.join(THIS_DIR, "..", dpa_dirname, filename)
    # Make sure subdirectory exists
    pathlib.Path(dpa_dirname).mkdir(parents=True, exist_ok=True)
    print("Import article...", article.get("headline", ""), path)
    try:
        with open(path, "w") as file:
            file.write(json.dumps(article))
    except IOError:
        print("Unable to create file on disk.")
        return False

    return True
