"""Look up a nytimes.com article through the Article Search API.

Article pages serve a paywall stub to anything that isn't a browser with a
subscription; the API hands over the headline, abstract, lead paragraph and
photo for any URL. Needs an API key from developer.nytimes.com stored at
/2000news/NYT_API_KEY. The search index runs hours behind publication, so
articles it can't find yet are looked for in the RSS feeds. Returns stories
in the newsdata.io dict shape that StoriesRepository saves.
"""

from urllib.parse import urlparse, urlunparse

import requests
from botocore.exceptions import ClientError

from .rss_client import RssClient
from .ssm_secrets import get_secret

ENDPOINT = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
IMAGE_ROOT = 'https://www.nytimes.com/'


def is_nyt_url(url):
    host = (urlparse(url).hostname or '').lower()
    return host == 'nytimes.com' or host.endswith('.nytimes.com')


def fetch_story(url):
    """The article behind a nytimes.com URL, or None if neither the search
    nor the feeds have it."""
    canonical = _without_query(url)
    return _search(canonical) or RssClient().find_nyt_story(canonical)


def _search(url):
    api_key = _api_key()
    if not api_key:
        print("No NYT API key configured; skipping the Article Search lookup.")
        return None

    print(f"Looking up {url} via the NYT Article Search API")
    try:
        response = requests.get(ENDPOINT, params={'fq': f'web_url:("{url}")', 'api-key': api_key},
                                timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"NYT Article Search failed: {e}")
        return None
    docs = (response.json().get('response') or {}).get('docs') or []
    if not docs:
        print("NYT Article Search has no match (yet)")
        return None
    return _to_story(docs[0])


def _api_key():
    try:
        return get_secret('NYT_API_KEY')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ParameterNotFound':
            return None
        raise


def _without_query(url):
    parts = urlparse(url)
    return urlunparse(parts._replace(query='', fragment=''))


def _to_story(doc):
    byline = ((doc.get('byline') or {}).get('original') or '').removeprefix('By ').strip()
    return {
        'title': (doc.get('headline') or {}).get('main') or '',
        'link': doc.get('web_url') or '',
        'description': _description(doc),
        'pubDate': doc.get('pub_date') or '',
        'creator': [byline] if byline else None,
        'content': None,
        'image_url': _image_url(doc.get('multimedia')),
        'video_url': None,
        'language': 'english',
        'country': None,
        'keywords': [k['value'] for k in doc.get('keywords') or [] if k.get('value')] or None,
        'category': [doc['section_name']] if doc.get('section_name') else None,
        'source_id': 'nytimes.com',
    }


def _description(doc):
    paragraphs = []
    for field in ('abstract', 'lead_paragraph'):
        text = (doc.get(field) or '').strip()
        if text and text not in paragraphs:
            paragraphs.append(text)
    return '\n\n'.join(paragraphs)


def _image_url(multimedia):
    """The API has shipped multimedia both as a list of image variants with
    site-relative urls and as {default: {url}, thumbnail: {url}}."""
    if isinstance(multimedia, dict):
        url = (multimedia.get('default') or {}).get('url')
        return url or None
    for image in multimedia or []:
        url = image.get('url')
        if url:
            return url if url.startswith('http') else IMAGE_ROOT + url.lstrip('/')
    return None
