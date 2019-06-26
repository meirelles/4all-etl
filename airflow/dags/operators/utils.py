import logging
import json

from airflow.contrib.hooks.redis_hook import RedisHook
from itertools import islice, chain
from airflow.models import Variable


log = logging.getLogger(__name__)
config_cached = None


def config():
    """
    Get the cached config

    :return: the config dict
    """

    global config_cached
    if not config_cached:
        config_cached = Variable.get("4all_sample_etl_config", deserialize_json=True)
    return config_cached


def batch_iter(iterable, size=250):
    """
    Split iterator in batches on-the-fly

    :param iterable: the iterator
    :param size: the batch size
    :return: iterator into batches of N size
    """

    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([next(batchiter)], batchiter)


class IntermediateWrite:
    """
    Save the intermediate output to a Redis database.
    """

    def __init__(self, ns):
        """
        Create a new IntermediateWrite instance.

        :param ns: Namespace string
        """

        self.conn = RedisHook(config()['redis_cache_conn']).get_conn()
        self.ns = ns
        self.batch = list()
        self.batch_next_id = 0
        self.batch_sz = config()['intermediate']['batch_sz']
        self._wipe()

    def _wipe(self):
        """
        Wipe any previous data
        """

        it = self.conn.scan_iter(self.ns + ":*")
        for mkeys in batch_iter(it):
            self.conn.delete(*mkeys)

    def add(self, record):
        if len(self.batch) > self.batch_sz:
            self.flush()
        self.batch.append(record)

    def flush(self):
        """
        Write any pending record(s) to the database.
        """

        if not self.batch:
            return
        batch_name = self.ns + ":" + str(self.batch_next_id)
        self.conn.set(batch_name, json.dumps(self.batch), ex=config()['intermediate']['expire_time'])
        self.batch_next_id = self.batch_next_id + 1
        self.batch = list()


class IntermediateRead:
    """
    Read the intermediate input from a Redis database.
    """

    def __init__(self, ns):
        """
        Create a new IntermediateRead instance.

        :param ns: Namespace string
        """

        self.conn = RedisHook(config()['redis_cache_conn']).get_conn()
        self.ns = ns

    def __iter__(self):
        """
        Get the records iterator

        :return: iterator
        """

        for key in self.conn.scan_iter(self.ns + ":*"):
            items = json.loads(self.conn.get(key))
            if not items:
                continue
            yield from items
