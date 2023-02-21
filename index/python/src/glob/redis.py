#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import redis
import json

from oc.index.utils.config import get_config
from oc.index.glob.datasource import DataSource


class RedisDataSource(DataSource):
    def __init__(self, service, unified_index = False):
        super().__init__(service)
        self._rid = None

        _db = get_config().get(service, "db")
        # in case we wanto to use the unified INDEX
        # > the DB storing the data is the one of INDEX
        # > the original id should be mapped to the corrisponding OMID using the "db_index" DB
        if unified_index:
            _db = get_config().get("INDEX", "db")
            self._rid = redis.Redis(
                host=get_config().get("redis", "host"),
                port=get_config().get("redis", "port"),
                db=get_config().get("cnc", "db_index")
            )

        self._rdata = redis.Redis(
            host=get_config().get("redis", "host"),
            port=get_config().get("redis", "port"),
            db=_db
        )

    def get(self, resource_id):
        org_resource_id = resource_id
        if self._rid != None:
            resource_id = self._rid.get(resource_id)
        if resource_id is not None:
            redis_data = self._rdata.get(resource_id)
            if redis_data is not None:
                # include the resource id in the data
                redis_data["omid"] = resource_id.decode("utf-8") if resources_id != org_resource_id else None
                return json.loads(redis_data)
        return None

    def mget(self, resources_id):
        org_resources_id = resources_id

        if self._rid != None:
            # build IDs if we are using unified index
            tmp_org_resources_id = []
            resources_id = []
            for i, v in enumerate(self._rid.mget(org_resources_id)):
                if v != None:
                    resources_id.append(v)
                    tmp_org_resources_id.append(org_resources_id[i])
            org_resources_id = tmp_org_resources_id

        print(org_resources_id,resources_id)
        return {
            org_resources_id[i]: json.loads(v) | {"omid":resources_id[i].decode("utf-8") if resources_id[i] != org_resources_id[i] else None} if v is not None else None
            for i, v in enumerate(self._rdata.mget(resources_id))
        }

    def set(self, resource_id, value):
        return self._rdata.set(resource_id, json.dumps(value))

    def mset(self, resources):
        return self._rdata.mset({k: json.dumps(v) for k, v in resources.items()})
