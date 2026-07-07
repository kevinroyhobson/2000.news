"""
Client for NYT, ESPN, and bengals.com RSS feeds.

Returns stories in the same dict shape used by NewsdataClient / StoriesRepository:
    title, link, description, pubDate (ISO), creator, content, image_url,
    video_url, language, country, keywords, category, source_id
"""

import email.utils
import json
import re
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET


MEDIA_NS = '{http://search.yahoo.com/mrss/}'
DC_NS = '{http://purl.org/dc/elements/1.1/}'

NYT_FEED_URL = 'https://rss.nytimes.com/services/xml/rss/nyt/{name}.xml'
ESPN_FEED_URL = 'https://www.espn.com/espn/rss/{name}'
BENGALS_FEED_URL = 'https://www.bengals.com/rss/news'

USER_AGENT = 'Mozilla/5.0 (2000.news-fetcher)'

# ESPN feeds include landing pages (/nfl/draft/rounds) and live game URLs
# (/nba/game/_/gameId/...) that don't map to real articles. Only /story/_/id/
# URLs have real editorial content and og:image hero photos.
ESPN_STORY_URL_RE = re.compile(r'/story/_/id/')

# bengals.com article pages embed schema.org JSON-LD with a full articleBody.
JSON_LD_RE = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.S)
LEDE_MAX_PARAGRAPHS = 3
LEDE_MAX_CHARS = 1200

OG_IMAGE_PATTERNS = [
    re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I),
    re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']', re.I),
]


class RssClient:
    def fetch_nyt(self, feed_name):
        """Fetch an NYT feed. feed_name is the path segment, e.g. 'HomePage',
        'MostViewed', 'Business'. Returns list of normalized story dicts."""
        url = NYT_FEED_URL.format(name=feed_name)
        print(f"Fetching NYT {feed_name}: {url}")
        return self._fetch_feed(url, source_id='nytimes.com',
                                og_image_fallback=True,
                                url_filter=None)

    def fetch_espn(self, feed_name):
        """Fetch an ESPN feed. feed_name is the path segment, e.g. 'news',
        'nfl/news', 'nba/news'. Returns list of normalized story dicts."""
        url = ESPN_FEED_URL.format(name=feed_name)
        print(f"Fetching ESPN {feed_name}: {url}")
        return self._fetch_feed(url, source_id='espn.com',
                                og_image_fallback=True,
                                url_filter=ESPN_STORY_URL_RE.search)

    def fetch_bengals_hobson(self, max_lede_fetches=4):
        """Fetch the bengals.com news feed, filtered to Geoff Hobson's stories.
        The feed carries no dc:creator, but his stories are tagged with a
        'hobson' entry in media:keywords.

        For the newest few stories, the feed's one-line description is
        replaced with the article's opening paragraphs — same pattern as
        sources whose feeds carry article text in the description. Only that
        many, because each Fetch run saves at most one new Hobson story and
        the feed is newest-first — anything deeper is already in the table."""
        print(f"Fetching bengals.com news: {BENGALS_FEED_URL}")
        stories = self._fetch_feed(BENGALS_FEED_URL, source_id='bengals.com',
                                   og_image_fallback=False,
                                   url_filter=None,
                                   keyword_filter='hobson')
        for story in stories:
            story['creator'] = ['Geoff Hobson']
        for story in stories[:max_lede_fetches]:
            story['description'] = _fetch_article_lede(story['link']) or story['description']
        return stories

    def _fetch_feed(self, url, source_id, og_image_fallback, url_filter, keyword_filter=None):
        xml_text = _http_get(url)
        root = ET.fromstring(xml_text)
        items = root.findall('.//item')
        stories = []
        for item in items:
            link = (item.findtext('link') or '').strip()
            if url_filter and not url_filter(link):
                continue
            if keyword_filter and keyword_filter not in _item_keywords(item):
                continue
            story = self._item_to_story(item, source_id, og_image_fallback)
            if story:
                stories.append(story)
        print(f"  -> {len(stories)} usable stories from {url}")
        return stories

    def _item_to_story(self, item, source_id, og_image_fallback):
        title = (item.findtext('title') or '').strip()
        link = (item.findtext('link') or '').strip()
        description = (item.findtext('description') or '').strip()
        pub_raw = (item.findtext('pubDate') or '').strip()
        creator = (item.findtext(f'{DC_NS}creator') or '').strip()

        if not title or not link or not pub_raw:
            return None

        try:
            published_at = email.utils.parsedate_to_datetime(pub_raw).isoformat()
        except (TypeError, ValueError):
            return None

        # Image from media:content, otherwise fall back to og:image scrape.
        image_url = None
        media = item.find(f'{MEDIA_NS}content')
        if media is not None:
            image_url = media.get('url')
        if not image_url and og_image_fallback:
            image_url = _fetch_og_image(link)
        if not image_url:
            print(f"  Skipped '{title}': no image available")
            return None

        # NYT includes a photo caption in media:description — fold it into
        # description so the generator gets more concrete nouns to work with.
        media_desc = ''
        if media is not None:
            md = item.find(f'{MEDIA_NS}description')
            if md is not None and md.text:
                media_desc = md.text.strip()
        if media_desc and media_desc not in description:
            description = f"{description}\n\n[Photo: {media_desc}]" if description else media_desc

        categories = [c.text for c in item.findall('category') if c.text]
        keywords = _item_keywords(item, lower=False)

        return {
            'title': title,
            'link': link,
            'description': description,
            'pubDate': published_at,
            'creator': [creator] if creator else None,
            'content': None,
            'image_url': image_url,
            'video_url': None,
            'language': 'english',
            'country': None,
            'keywords': keywords or None,
            'category': categories or None,
            'source_id': source_id,
        }


def _item_keywords(item, lower=True):
    """Parse an item's comma-separated media:keywords into a list of tags."""
    raw = item.findtext(f'{MEDIA_NS}keywords') or ''
    if lower:
        raw = raw.lower()
    return [k.strip() for k in raw.split(',') if k.strip()]


def _http_get(url):
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read()


def _fetch_article_lede(article_url):
    """Extract the opening paragraphs of an article from its schema.org
    JSON-LD articleBody. Returns None if the page can't be fetched or parsed."""
    try:
        html = _http_get(article_url).decode('utf-8', errors='replace')
    except (urllib.error.URLError, TimeoutError):
        return None

    for m in JSON_LD_RE.finditer(html):
        try:
            data = json.loads(m.group(1))
        except ValueError:
            continue
        body = data.get('articleBody') if isinstance(data, dict) else None
        if not body:
            continue
        lede = []
        length = 0
        for para in body.split('\n\n'):
            para = para.strip()
            if not para:
                continue
            lede.append(para)
            length += len(para)
            if len(lede) >= LEDE_MAX_PARAGRAPHS or length >= LEDE_MAX_CHARS:
                break
        return '\n\n'.join(lede) or None
    return None


def _fetch_og_image(article_url):
    """Fetch the first 64KB of an article page and extract <meta property="og:image">."""
    try:
        req = urllib.request.Request(article_url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read(65536).decode('utf-8', errors='replace')
    except (urllib.error.URLError, TimeoutError):
        return None
    for pat in OG_IMAGE_PATTERNS:
        m = pat.search(html)
        if m:
            return m.group(1)
    return None
