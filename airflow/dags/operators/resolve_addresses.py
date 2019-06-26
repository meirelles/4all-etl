import logging
import backoff
import json
import aiohttp
import asyncio
import random
import re

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.utils import (IntermediateRead, IntermediateWrite, config)
from airflow.contrib.hooks.redis_hook import RedisHook
from types import SimpleNamespace

log = logging.getLogger(__name__)


class ResolveAddresses(BaseOperator):
    """
    Resolve lat lng to addresses using Google Maps api
    """

    template_fields = ('input_ns', 'output_ns')

    @apply_defaults
    def __init__(self, input_ns, output_ns, *args, **kwargs):
        super(ResolveAddresses, self).__init__(*args, **kwargs)
        self.input_ns = input_ns
        self.output_ns = output_ns

    def execute(self, context):
        # Get a new asyncio event loop to execute the async tasks.
        loop = asyncio.get_event_loop()

        try:
            r = loop.run_until_complete(self._process())
        finally:
            loop.close()

        # Check if any task got an unhandled exception and report what is
        if r.exception:
            raise r.exception

    async def _process(self):
        async with aiohttp.ClientSession() as client:
            r = SimpleNamespace(
                terminate=False,
                exception=None,
                queue=asyncio.Queue(),
                iwrite=IntermediateWrite(self.output_ns),
                helper=GeocodingHelper(client)
            )

            # Create ONE producer and multiple consumers to resolve the record
            consumers = list()

            for _ in range(config()['google_maps_api']['max_concurrent']):
                consumers.append(self._wrap(self._consumer, r))

            producer = asyncio.ensure_future(self._wrap(self._producer, r))
            await asyncio.gather(*consumers)

        # In case of exception, producer might still be running. Kill it.
        producer.cancel()
        r.iwrite.flush()

        return r

    async def _wrap(self, task, r):
        """
        Wrap a task function with error handling.

        :param task: a task function
        :param r: the shared data
        :return: a wrapped function with the excpetion handling
        """

        try:
            return await task(r)
        except Exception as e:
            # Stop ASAP all tasks on error
            r.terminate = True
            r.exception = e

    async def _producer(self, r):
        """
        Push the input records to the queue.

        :param r: the shared data
        :return: async coroutine
        """

        iread = IntermediateRead(self.input_ns)
        for record in iread:
            await r.queue.put(record)
        r.terminate = True

    async def _consumer(self, r):
        """
        Consume the input records from the queue and then write to the output.

        :param r: the shared data
        :return: async coroutine
        """

        while not r.exception:
            try:
                record = r.queue.get_nowait()
            except asyncio.QueueEmpty:
                if r.terminate:
                    return
                # We should keep pooling
                await asyncio.sleep(0.5)
                continue

            record["address"] = await r.helper.address(record["lat"], record["lng"])
            r.iwrite.add(record)


class OverQueryLimit(IOError):
    """ The Google returned an OVER_QUERY_LIMIT, that trigger the backoff algo """
    pass


class GeocodingHelper:
    def __init__(self, aiohttp_session):
        self.aiohttp_session = aiohttp_session
        self.redis_conn = RedisHook(config()['redis_cache_conn']).get_conn()
        self._rotate_key()

    def _rotate_key(self):
        # API keys in public repository is very insecure. I had scrambled it to at least avoid
        # automated/robots finding/abusing it.

        scrambe_magic = "ZmCgw1"
        new_key = random.choice(config()["google_maps_api"]["keys"])

        if scrambe_magic in new_key:
            # It is a scrambled key
            new_key = new_key.replace(scrambe_magic, "")[::-1]

        self.current_key = new_key

    @backoff.on_exception(backoff.expo, OverQueryLimit, aiohttp.ClientError, max_time=60)
    async def _remote_get(self, lat, lng):
        """
        Documentation in
        https://developers.google.com/maps/documentation/geocoding/intro

        :param lat: Latitude
        :param lng: Longitude
        :return: json response from google maps
        """

        params = {
            'latlng': "{0},{1}".format(lat, lng),
            'key': self.current_key
        }

        url = 'https://maps.googleapis.com/maps/api/geocode/json'

        async with self.aiohttp_session.get(url, params=params) as req:
            res = json.loads(await req.read())
            status = res["status"]

            if status == 'OK':
                return res
            elif status == 'ZERO_RESULTS':
                return None
            elif status == 'OVER_QUERY_LIMIT':
                # Try a new key if multiple was provided
                self._rotate_key()

                # Trigger the backoff algo
                raise OverQueryLimit("Over query limit")
            else:
                raise IOError("Unknown status: " + status)

    async def _get(self, lat, lng):
        cache_key = "google_maps_gc:{0},{1}".format(lat, lng)
        value = self.redis_conn.get(cache_key)

        if value:
            # It's cached, return now
            self.res = json.loads(value)
        else:
            self.res = await self._remote_get(lat, lng)
            self.redis_conn.set(cache_key, json.dumps(self.res), ex=config()['google_maps_api']['expire_time'])

        return self.res

    def _filter_by_type(self, address_components, type_name, name="long_name"):
        for address in address_components:
            if type_name in address["types"]:
                return address[name]
        return None

    async def address(self, lat, lng):
        response = await self._get(lat, lng)

        # Zero results address are also cacheable
        if not response:
            return None

        components = response["results"][0]["address_components"]

        return {
            "street": self._filter_by_type(components, "route"),
            "number": self._filter_by_type(components, "street_number"),
            "neighborhood": self._filter_by_type(components, "street_number"),
            "city": self._filter_by_type(components, "administrative_area_level_2"),
            "zipcode": self._filter_by_type(components, "postal_code"),
            "state": self._filter_by_type(components, "administrative_area_level_1", "short_name"),
            "country": self._filter_by_type(components, "country"),
        }
