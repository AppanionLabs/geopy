from functools import partial
from urllib.parse import quote_plus, urlencode

from geopy.location import Location
from geopy.exc import GeocoderServiceError
from geopy.geocoders.base import DEFAULT_SENTINEL
from geopy.geocoders.tomtom import TomTom

__all__ = ("AzureMaps",)

from geopy.util import logger


class AzureMaps(TomTom):
    """AzureMaps geocoder based on TomTom.

    Documentation at:
        https://docs.microsoft.com/en-us/azure/azure-maps/index
    """

    geocode_path = '/search/address/json'
    reverse_path = '/search/address/reverse/json'
    batch_geocode_path = "/search/address/batch/sync/json"
    batch_reverse_path = "/search/address/reverse/batch/sync/json"

    def __init__(
        self,
        subscription_key,
        *,
        scheme=None,
        timeout=DEFAULT_SENTINEL,
        proxies=DEFAULT_SENTINEL,
        user_agent=None,
        ssl_context=DEFAULT_SENTINEL,
        adapter_factory=None,
        domain='atlas.microsoft.com'
    ):
        """
        :param str subscription_key: Azure Maps subscription key.

        :param str scheme:
            See :attr:`geopy.geocoders.options.default_scheme`.

        :param int timeout:
            See :attr:`geopy.geocoders.options.default_timeout`.

        :param dict proxies:
            See :attr:`geopy.geocoders.options.default_proxies`.

        :param str user_agent:
            See :attr:`geopy.geocoders.options.default_user_agent`.

        :type ssl_context: :class:`ssl.SSLContext`
        :param ssl_context:
            See :attr:`geopy.geocoders.options.default_ssl_context`.

        :param callable adapter_factory:
            See :attr:`geopy.geocoders.options.default_adapter_factory`.

            .. versionadded:: 2.0

        :param str domain: Domain where the target Azure Maps service
            is hosted.
        """
        super().__init__(
            api_key=subscription_key,
            scheme=scheme,
            timeout=timeout,
            proxies=proxies,
            user_agent=user_agent,
            ssl_context=ssl_context,
            adapter_factory=adapter_factory,
            domain=domain,
        )
        self.api_batch = "%s://%s%s" % (self.scheme, domain, self.batch_geocode_path)
        self.api_reverse_batch = "%s://%s%s" % (
            self.scheme, domain, self.batch_reverse_path
        )
        self.max_batch_size = 1000

    def geocode(
        self,
        query,
        *,
        exactly_one=True,
        timeout=DEFAULT_SENTINEL,
        limit=None,
        typeahead=False,
        language=None
    ):
        if isinstance(query, list):
            return self._apply_batchwise(
                query,
                self._batch_geocode,
                exactly_one=exactly_one,
                timeout=timeout,
                limit=1,
                typeahead=False,
                language=language
            )
        return super().geocode(
            query,
            exactly_one=exactly_one,
            timeout=timeout,
            limit=limit,
            typeahead=typeahead,
            language=language
        )

    def reverse(
        self,
        query,
        *,
        exactly_one=True,
        timeout=DEFAULT_SENTINEL,
        language=None
    ):
        if isinstance(query, list):
            return self._apply_batchwise(
                query,
                self._batch_reverse,
                exactly_one=exactly_one,
                timeout=timeout,
                limit=1,
                typeahead=False,
                language=language
            )
        return super().reverse(
            query,
            exactly_one=exactly_one,
            timeout=timeout,
            language=language
        )

    def _batch_geocode_params(self):
        return {
            'api-version': '1.0',
            'subscription-key': self.api_key,
        }

    def _batch_geocode(self, query, *, exactly_one, timeout, limit, typeahead, language):
        params = self._batch_geocode_params()

        data = {"batchItems": []}
        for location in query:
            if exactly_one:
                query = "?limit=1&"
            else:
                query = "?"
            query += f"query={quote_plus(location)}"
            data["batchItems"].append({"query": query})

        if language:
            params['language'] = language

        url = "?".join((self.api_batch, urlencode(params)))
        logger.debug("%s.geocode: %s", self.__class__.__name__, url)
        callback = partial(self._parse_batch_response, exactly_one=exactly_one,
                           is_reverse=False)
        headers = {'Content-Type': 'application/json'}
        return self._call_geocoder(url, callback, timeout=timeout, headers=headers,
                                   data=data)

    def _batch_reverse(self, query, *, exactly_one, timeout, limit, typeahead, language):
        params = self._batch_geocode_params()

        data = {"batchItems": []}
        for lat, lng in query:
            if exactly_one:
                query = "?limit=1&"
            else:
                query = "?"
            query += f"query={lat},{lng}"
            data["batchItems"].append({"query": query})

        if language:
            params['language'] = language

        url = "?".join((self.api_reverse_batch, urlencode(params)))
        logger.debug("%s.reverse: %s", self.__class__.__name__, url)
        callback = partial(self._parse_batch_response, exactly_one=exactly_one,
                           is_reverse=True)
        headers = {'Content-Type': 'application/json'}
        return self._call_geocoder(url, callback, timeout=timeout, headers=headers,
                                   data=data)

    def _parse_batch_response(self, r_json, exactly_one, is_reverse):
        if is_reverse:
            key = "addresses"
        else:
            key = "results"

        locations = []
        if "error" in r_json:
            raise GeocoderServiceError(str(r_json["error"]))
        for item in r_json.get("batchItems", []):
            if key in item["response"] and item["response"][key]:
                results = []
                for result in item["response"][key]:
                    address = result.get("address", {})

                    position = result.get("position", {})
                    if isinstance(position, dict):
                        lat = float(position.get("lat"))
                        lng = float(position.get("lon"))
                    else:
                        lat = float(position.split(",")[0])
                        lng = float(position.split(",")[1])

                    results.append(
                        Location(
                            address.get("freeformAddress"),
                            (lat, lng),
                            result
                        )
                    )
                if exactly_one:
                    results = results[0]
            elif exactly_one:
                results = None
            else:
                results = []
            locations.append(results)
        return locations

    def _geocode_params(self, formatted_query):
        return {
            'api-version': '1.0',
            'subscription-key': self.api_key,
            'query': formatted_query,
        }

    def _reverse_params(self, position):
        return {
            'api-version': '1.0',
            'subscription-key': self.api_key,
            'query': position,
        }
