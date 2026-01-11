"""Client for the newsdata.io API."""

import requests
from .ssm_secrets import get_secret


class NewsdataClient:
    ENDPOINT = "https://newsdata.io/api/1/news"

    def __init__(self, api_key=None):
        self._api_key = api_key or get_secret("NEWS_DATA_API_KEY")

    def fetch_by_category(self, category=None, use_priority=True, page_token=None):
        """
        Fetch stories by category.

        Args:
            category: Category to fetch (business, entertainment, sports, technology, politics)
                      or None for no category filter
            use_priority: If True, only fetch from top-tier sources
            page_token: Pagination token for fetching next page

        Returns:
            API response dict with 'results', 'nextPage', etc.
        """
        params = self._base_params()

        if category is not None:
            params['category'] = category

        if use_priority:
            params['prioritydomain'] = 'top'

        if page_token is not None:
            params['page'] = page_token

        return self._fetch(params)

    def fetch_by_query(self, query, use_priority=True, page_token=None):
        """
        Fetch stories by search query.

        Args:
            query: Search term (e.g., "barack obama", "climate summit")
            use_priority: If True, only fetch from top-tier sources
            page_token: Pagination token for fetching next page

        Returns:
            API response dict with 'results', 'nextPage', etc.
        """
        params = self._base_params()
        params['q'] = query

        if use_priority:
            params['prioritydomain'] = 'top'

        if page_token is not None:
            params['page'] = page_token

        return self._fetch(params)

    def _base_params(self):
        return {
            'apikey': self._api_key,
            'country': 'us',
            'language': 'en',
        }

    def _fetch(self, params):
        safe_params = {k: ('xxx' if k == 'apikey' else v) for k, v in params.items()}
        print(f"Fetching: {self.ENDPOINT}?{self._encode_params(safe_params)}")

        response = requests.get(self.ENDPOINT, params=params).json()

        if response['status'] == 'error':
            raise Exception(f"{response['results']['code']}: {response['results']['message']}")

        if response['status'] != 'success':
            raise Exception(f"Unexpected response status: {response['status']}")

        return response

    def _encode_params(self, params):
        return '&'.join(f"{k}={v}" for k, v in params.items())
