import redis
import json

from oc.index.utils.config import get_config


class DataSource:
    def __init__(self):
        self._r = redis.Redis(
            host=get_config().get("redis", "host"),
            port=get_config().get("redis", "port"),
            db=get_config().get("redis", "db"),
        )

    def new(self):
        return {"date": None, "valid": False, "issn": [], "orcid": []}

    def get(self, resource_id):
        return json.loads(self._r.get(resource_id))

    def mget(self, resources_id):
        return {
            resources_id[i]: json.loads(v) if not v is None else None
            for i, v in enumerate(self._r.mget(resources_id))
        }

    def set(self, resource_id, value):
        return self._r.set(resource_id, json.dumps(value))

    def mset(self, resources):
        return self._r.mset({k: json.dumps(v) for k, v in resources.items()})
