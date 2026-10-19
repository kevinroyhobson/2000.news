import importlib.util
import pathlib


def _load_page_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "TelegramScoop" / "page.py"
    spec = importlib.util.spec_from_file_location("page_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


page = _load_page_module()

HTML = """<html><head>
<title>Fallback &amp; Title - Site</title>
<meta content="https://cdn.example.com/hero.jpg" property="og:image" />
<meta property='og:title' content='Mayor Declares War on Geese' />
<meta name="author" content="Jane Doe">
<meta property="article:published_time" content="2026-09-21T14:03:00Z">
<meta property="og:description" content="">
</head><body>
<nav>Home | Politics | <a href="/x">Sports</a></nav>
<script>window.__DATA__ = {"junk": "<p>not text</p>"};</script>
<article>
<h1>Mayor Declares War on Geese</h1>
<p>The mayor announced Tuesday that the city&rsquo;s   geese
have had it too good for too long.</p>
<style>.x { color: red }</style>
<p>Residents were split.<br>Some cheered.</p>
</article>
<footer>Copyright</footer>
</body></html>"""


def test_meta_reads_property_or_name_in_either_attribute_order():
    assert page.meta(HTML, "og:image") == "https://cdn.example.com/hero.jpg"
    assert page.meta(HTML, "og:title") == "Mayor Declares War on Geese"
    assert page.meta(HTML, "author") == "Jane Doe"


def test_meta_falls_through_names_in_order_and_skips_empty_content():
    assert page.meta(HTML, "og:description", "article:published_time") == "2026-09-21T14:03:00Z"
    assert page.meta(HTML, "twitter:image") is None


def test_title_is_unescaped():
    assert page.title(HTML) == "Fallback & Title - Site"


def test_visible_text_prefers_the_article_and_drops_scripts_and_styles():
    text = page.visible_text(HTML)
    assert text.splitlines() == [
        "Mayor Declares War on Geese",
        "The mayor announced Tuesday that the city’s geese have had it too good for too long.",
        "Residents were split.",
        "Some cheered.",
    ]


def test_visible_text_uses_the_whole_page_without_an_article_element():
    text = page.visible_text("<body><p>One</p><script>x</script><p>Two</p></body>")
    assert text == "One\nTwo"


PAGE_URL = "https://www.example.com/sports/stephen-a-smith"


def test_image_prefers_the_social_card():
    assert page.image(HTML, PAGE_URL) == "https://cdn.example.com/hero.jpg"


def test_image_reads_itemprop_meta_and_resolves_relative_urls():
    html = '<meta itemprop="image" content="/img/lead.jpg">'
    assert page.image(html, PAGE_URL) == "https://www.example.com/img/lead.jpg"


def test_image_falls_back_to_the_articles_structured_data():
    html = """<script type="application/ld+json">
    {"@context": "https://schema.org", "@graph": [
      {"@type": "Organization", "logo": {"url": "https://cdn.example.com/logo.png"},
       "image": "https://cdn.example.com/org.jpg"},
      {"@type": "NewsArticle", "headline": "x",
       "image": [{"@type": "ImageObject", "url": "https://cdn.example.com/smith.jpg"}]}
    ]}</script>"""
    assert page.image(html, PAGE_URL) == "https://cdn.example.com/smith.jpg"


def test_image_skips_broken_structured_data():
    html = """<script type="application/ld+json">{not json</script>
    <link rel="image_src" href="https://cdn.example.com/linked.jpg">"""
    assert page.image(html, PAGE_URL) == "https://cdn.example.com/linked.jpg"


def test_image_falls_back_to_the_first_photo_in_the_article():
    html = """<header><img src="/site-logo.png"></header>
    <article>
      <img src="https://cdn.example.com/byline-avatar.jpg">
      <img src="https://cdn.example.com/tiny.jpg" width="40" height="40">
      <img src="data:image/gif;base64,R0lGOD" data-src="https://cdn.example.com/smith.jpg?w=1200">
    </article>"""
    assert page.image(html, PAGE_URL) == "https://cdn.example.com/smith.jpg?w=1200"


def test_image_reads_srcset_when_there_is_no_src():
    html = '<img srcset="https://cdn.example.com/a-640.jpg 640w, https://cdn.example.com/a-1280.jpg 1280w">'
    assert page.image(html, PAGE_URL) == "https://cdn.example.com/a-640.jpg"


def test_image_is_none_when_the_page_has_no_photo():
    html = '<body><img src="/logo.svg"><img src="/pixel.gif" width="1" height="1"><p>Text</p></body>'
    assert page.image(html, PAGE_URL) is None


def test_image_follows_a_json_ld_reference_to_an_image_elsewhere_in_the_graph():
    html = """<script type="application/ld+json">
    {"@graph": [
      {"@type": "NewsArticle", "image": {"@id": "https://www.example.com/#primaryimage"}},
      {"@type": "ImageObject", "@id": "https://www.example.com/#primaryimage",
       "url": "https://cdn.example.com/primary.jpg"}
    ]}</script>"""
    assert page.image(html, PAGE_URL) == "https://cdn.example.com/primary.jpg"


def test_image_tries_srcset_when_src_is_a_placeholder():
    html = '<img src="data:image/gif;base64,R0lGOD" srcset="https://cdn.example.com/real-800.jpg 800w">'
    assert page.image(html, PAGE_URL) == "https://cdn.example.com/real-800.jpg"


def test_image_keeps_photos_whose_names_merely_contain_a_decoration_word():
    for name in ("silicon-valley.jpg", "blanket-drive.jpg", "iconic-moment.jpg"):
        html = f'<img src="https://cdn.example.com/{name}">'
        assert page.image(html, PAGE_URL) == f"https://cdn.example.com/{name}"


def test_image_reads_unquoted_attributes():
    html = "<img src=/images/lead.jpg width=1200>"
    assert page.image(html, PAGE_URL) == "https://www.example.com/images/lead.jpg"
