import importlib.util
import pathlib
import urllib.error


def _load_rss_client_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "lib" / "rss_client.py"
    spec = importlib.util.spec_from_file_location("rss_client_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rss_client = _load_rss_client_module()

ARTICLE = "https://www.nytimes.com/2026/09/24/technology/google-suncatcher-ai-data-center-space.html"

FEED = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:media="http://search.yahoo.com/mrss/" xmlns:dc="http://purl.org/dc/elements/1.1/" version="2.0">
<channel>
<item>
  <title>Something Else</title>
  <link>https://www.nytimes.com/2026/09/24/technology/something-else.html</link>
  <pubDate>Thu, 24 Sep 2026 09:00:00 +0000</pubDate>
  <media:content url="https://static01.nyt.com/else.jpg" medium="image"/>
</item>
<item>
  <title>Google Wants to Put Data Centers in Space</title>
  <link>{ARTICLE}?smid=rss</link>
  <description>The company is testing satellites that run A.I. chips.</description>
  <dc:creator>Cade Metz</dc:creator>
  <pubDate>Thu, 24 Sep 2026 10:00:00 +0000</pubDate>
  <media:content url="https://static01.nyt.com/suncatcher.jpg" medium="image"/>
</item>
</channel>
</rss>""".encode()


def _serve(feeds, requested):
    def http_get(url):
        requested.append(url)
        if url not in feeds:
            raise urllib.error.HTTPError(url, 404, "Not Found", None, None)
        return feeds[url]
    return http_get


def _feed_url(name):
    return rss_client.NYT_FEED_URL.format(name=name)


def test_feeds_run_from_most_specific_section_to_front_page():
    link = "https://www.nytimes.com/2026/09/24/us/politics/shutdown.html"
    assert rss_client._nyt_feeds_for(link) == ["Politics", "US", "HomePage", "MostViewed"]


def test_unknown_sections_fall_back_to_front_page_feeds():
    link = "https://www.nytimes.com/2026/09/24/dining/pie.html"
    assert rss_client._nyt_feeds_for(link) == ["HomePage", "MostViewed"]


def test_finds_story_in_section_feed_ignoring_query(monkeypatch):
    requested = []
    monkeypatch.setattr(rss_client, "_http_get", _serve({_feed_url("Technology"): FEED}, requested))

    story = rss_client.RssClient().find_nyt_story(ARTICLE + "?smid=nytcore-ios-share")

    assert requested == [_feed_url("Technology")]
    assert story["title"] == "Google Wants to Put Data Centers in Space"
    assert story["image_url"] == "https://static01.nyt.com/suncatcher.jpg"
    assert story["creator"] == ["Cade Metz"]
    assert story["source_id"] == "nytimes.com"


def test_moves_past_unreadable_feeds(monkeypatch):
    requested = []
    monkeypatch.setattr(rss_client, "_http_get", _serve({_feed_url("MostViewed"): FEED}, requested))

    story = rss_client.RssClient().find_nyt_story(ARTICLE)

    assert requested == [_feed_url(name) for name in ("Technology", "HomePage", "MostViewed")]
    assert story["title"] == "Google Wants to Put Data Centers in Space"


def test_none_when_no_feed_has_it(monkeypatch):
    monkeypatch.setattr(rss_client, "_http_get", _serve({}, []))
    assert rss_client.RssClient().find_nyt_story(ARTICLE) is None
