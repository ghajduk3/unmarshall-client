import typing
import requests
import logging
import simplejson
from urllib import parse as url_parser

from python_settings import settings

import enums
import utils
import settings as local_settings
from unmarshall import exceptions

settings.configure(local_settings)
logger = logging.getLogger(__name__)


class UnmarshallClient(object):
    API_BASE_URL = settings.UNMARSHALL_API_URL
    API_KEY = settings.UNMARSHALL_API_KEY
    API_VERSION = "v3"
    VALID_STATUS_CODES = [200, 201, 204]

    LOG_PREFIX = "[UNMARSHALL-CLIENT]"


    # WALLET ENDPOINTS
    def get_wallet_balances(self, currency: enums.Currency, address: str) -> typing.List[dict]:
        return simplejson.loads(
            self._request(
                endpoint='/v1/{}/address/{}/assets'.format(currency.to_chain_name(), address),
                method=enums.HttpMethod.GET.value
            ).content
        )

    # TRANSACTION ENDPOINTS
    def get_transactions(
        self, currency: enums.Currency, address: str, depth: int = 1, limit: int = 25
    ) -> typing.List[dict]:
        return self._get_paginated_response(
            endpoint="/v3/{}/address/{}/transactions".format(
                currency.to_chain_name(), address
            ),
            method=enums.HttpMethod.GET.value,
            depth=depth,
            limit=limit,
            data_field="transactions",
        )

    def get_transaction(self, currency: enums.Currency, transaction_hash: str) -> dict:
        return simplejson.loads(
            self._request(
                endpoint="/v1/{}/transactions/{}".format(
                    currency.to_chain_name(), transaction_hash
                ),
                method=enums.HttpMethod.GET.value,
            ).content
        )

    def get_wallet_transactions_count(self, currency: enums.Currency, address: str) -> dict:
        return simplejson.loads(
            self._request(
                endpoint='/v1/{}/address/{}/transactions/count'.format(currency.to_chain_name(), address),
                method=enums.HttpMethod.GET.value,
            ).content
        )
    def _get_paginated_response(
        self,
        endpoint: str,
        method: str,
        depth: int,
        limit: int,
        data_field: str,
        params: typing.Optional[dict] = None,
        payload: typing.Optional[dict] = None,
    ) -> typing.List[dict]:
        data = []

        if not params:
            params = {}
        params["pageSize"] = limit

        for _ in range(depth):
            response = simplejson.loads(
                self._request(
                    endpoint=endpoint,
                    method=method,
                    params=params,
                    payload=payload,
                ).content
            )
            data.extend(response.get(data_field, []))

            if not response.get("has_next", False):
                break

        return data

    def _request(
        self,
        endpoint: str,
        method: str,
        params: typing.Optional[dict] = None,
        payload: typing.Optional[dict] = None,
    ) -> requests.Response:
        url = url_parser.urljoin(base=self.API_BASE_URL, url=endpoint)
        headers = {"Accept": "application/json"}

        if not params:
            params = {}

        params.update(self._get_authentication_params())

        try:
            response = requests.request(
                url=url,
                method=method,
                params=params,
                data=payload,
                headers=headers,
            )

            if response.status_code not in self.VALID_STATUS_CODES:
                msg = "Invalid API client response (status_code={}, data={}).".format(
                    response.status_code,
                    response.content.decode(encoding="utf-8"),
                )
                logger.error("{} {}.".format(self.LOG_PREFIX, msg))
                raise exceptions.BadResponseCodeErrpr(msg)
        except requests.exceptions.ConnectTimeout as e:
            msg = "Connect timeout. Error: {}".format(
                utils.get_exception_message(exception=e)
            )
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.UnmarshallClientError(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(
                utils.get_exception_message(exception=e)
            )
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.UnmarshallClientError(msg)

        return response

    def _get_authentication_params(self) -> dict:
        return {"auth_key": self.API_KEY}
