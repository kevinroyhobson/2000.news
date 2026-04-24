"""
Client for NYT and ESPN RSS feeds.

Returns stories in the same dict shape used by NewsdataClient / StoriesRepository:
    title, link, description, pubDate (ISO), creator, content, image_url,
    video_url, language, country, keywords, category, source_id
"""

import email.utils
import re
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET


MEDIA_NS = '{http://search.yahoo.com/mrss/}'
DC_NS = '{http://purl.org/dc/elements/1.1/}'

NYT_FEED_URL = 'https://rss.nytimes.com/services/xml/rss/nyt/{name}.xml'
ESPN_FEED_URL = 'https://www.espn.com/espn/rss/{name}'

USER_AGENT = 'Mozilla/5.0 (2000.news-fetcher)'

# ESPN feeds include landing pages (/nfl/draft/rounds) and live game URLs
# (/nba/game/_/gameId/...) that don't map to real articles. Only /story/_/id/
# URLs have real editorial content and og:image hero photos.
ESPN_STORY_URL_RE = re.compile(r'/story/_/id/')

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

    def _fetch_feed(self, url, source_id, og_image_fallback, url_filter):
        xml_text = _http_get(url)
        root = ET.fromstring(xml_text)
        items = root.findall('.//item')
        stories = []
        for item in items:
            link = (item.findtext('link') or '').strip()
            if url_filter and not url_filter(link):
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
            'keywords': None,
            'category': categories or None,
            'source_id': source_id,
        }


def _http_get(url):
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read()


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
