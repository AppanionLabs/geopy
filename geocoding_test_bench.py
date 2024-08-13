"""
Install geopy from fork:
 pip install git+https://github.com/AppanionLabs/geopy.git
"""
import asyncio
import functools
import itertools
import json
import logging
import time

import pandas as pd

import geopy
from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import get_geocoder_for_service

geopy.geocoders.options.default_timeout = 7

ADDRESSES = [
    "Schulterblatt 58, 20357 Hamburg, Germany",
    "Wandsbeker Chaussee 164, 22089 Hamburg, Germany",
    "Kazan, Republic of Tatarstan, Russia, 420108",
    "Portovaya Ulitsa, 10А, Novorossiysk, Krasnodar Krai, Russia, 353901",
    "China, Guangdong Province, Guangzhou, Haizhu District",
    "Fengze District, Quanzhou, Fujian, China, 362124",
    "Eastgate Rd, Tema, Ghana",
    "Ad Daerah Al Gomrokeyah, إدارة شرطة ميناء الإسكندرية، Alexandria Governorate 5321001, egypt"
]

COORDINATES = [
    (53.5613, 9.9632),
    (53.5681, 10.0505),
    (55.7685, 49.0880),
    (44.7301, 37.7792),
    (23.1121, 113.2713),
    (24.88279, 118.68452),
    (5.6318, 0.0080),
    (31.1887, 29.8811)
]

SERVICES = [
    "ARCGIS",
    "AZURE",
    "GEOAPIFY",
    "GOOGLE",
    "HEREV7",
    "MAPBOX",
    "MAPTILER",
    "OPENCAGE",
]

BATCH_SERVICES = [
    "GEOAPIFY",
    "AZURE",
    "MAPTILER",
    "MAPBOX",
]

with open(".test_keys") as fp:
    services_api_key = json.load(fp)
    services_api_key = {service: api_key for service, api_key in services_api_key.items()
                        if service in SERVICES}


def timefunc(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = fn(*args, **kwargs)
        except Exception:
            finish = time.time() - start
            logging.info('%s failed in %.2f', fn, finish)
            raise
        else:
            finish = time.time() - start
            logging.info('%s succeeded in %.2f', fn, finish)
            return result

    return wrapper

def timecoro(corofn):
    @functools.wraps(corofn)
    async def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = await corofn(*args, **kwargs)
        except Exception:
            finish = time.time() - start
            logging.info('%s failed in %.2f', corofn, finish)
            raise
        else:
            finish = time.time() - start
            logging.info('%s succeeded in %.2f', corofn, finish)
            return result

    return wrapper


@timecoro
async def async_forward_geocode(geocoder_class, api_key):
    results = []
    async with geocoder_class(
        api_key,
        adapter_factory=AioHTTPAdapter,
    ) as geocoder:
        for ADDRESS in ADDRESSES:
            loc = await geocoder.geocode(ADDRESS, exactly_one=True, language="en")
            if loc is not None:
                results.append((loc.latitude, loc.longitude))
            else:
                results.append(None)

    return results


@timecoro
async def async_reverse_geocode(geocoder_class, api_key):
    results = []
    async with geocoder_class(
        api_key,
        adapter_factory=AioHTTPAdapter,
    ) as geocoder:
        for COORD in COORDINATES:
            loc = await geocoder.reverse(COORD, exactly_one=True, language="en")
            if loc is not None:
                results.append(loc.address)
            else:
                results.append(None)

    return results


@timefunc
def sync_batch_forward_geocode(geocoder_class, api_key):
    with geocoder_class(api_key) as geocoder:
        results = geocoder.geocode(ADDRESSES, exactly_one=True, language="en")

    results = [(loc.latitude, loc.longitude) if loc is not None else None for loc in results]
    return results


@timefunc
def sync_batch_reverse_geocoding(geocoder_class, api_key):
    with geocoder_class(api_key) as geocoder:
        results = geocoder.reverse(COORDINATES, exactly_one=True, language="en")

    results = [loc.address if loc is not None else None for loc in results]
    return results


def _get_geocoder_class(service):
    if service == "here":
        return get_geocoder_for_service("herev7")
    return get_geocoder_for_service(service)


def main():
    processed_services = []

    forward_results = []
    for service, api_key in services_api_key.items():
        logging.info(f"Testing forward geocoding >> {service}")
        geocoder_class = _get_geocoder_class(service)
        processed_services.append(service)

        forward_results.append(
            asyncio.run(async_forward_geocode(geocoder_class, api_key))
        )

        if service in BATCH_SERVICES:
            processed_services.append(f"BATCH >> {service}")
            forward_results.append(
                sync_batch_forward_geocode(geocoder_class, api_key)
            )

    reverse_results = []
    for service, api_key in services_api_key.items():
        logging.info(f"Testing reverse geocoding >> {service}")
        geocoder_class = _get_geocoder_class(service)

        reverse_results.append(
            asyncio.run(async_reverse_geocode(geocoder_class, api_key))
        )
        if service in BATCH_SERVICES:
            reverse_results.append(
                sync_batch_reverse_geocoding(geocoder_class, api_key)
            )


    # load results into a pandas DataFrame
    index = [[], []]
    for service in processed_services:
        for address, coordinate in zip(ADDRESSES, COORDINATES):
            index[0].append(f"{service}")
            index[1].append(f"{address} - {coordinate}")

    df = pd.DataFrame(index=index)

    df["geocode"] = list(itertools.chain.from_iterable(forward_results))
    df["address"] = list(itertools.chain.from_iterable(reverse_results))

    df.to_excel("geocoding_test_results.xlsx")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
