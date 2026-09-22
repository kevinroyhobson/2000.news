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


def test_visible_text_is_capped():
    assert len(page.visible_text("<p>" + "a" * 100 + "</p>", max_chars=10)) == 10
