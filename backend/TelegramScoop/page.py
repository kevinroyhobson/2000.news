"""Pull the useful parts out of an article page's HTML with the standard
library: meta tags, and the visible text for a model to read. Pure functions
(tests/test_scoop_page.py)."""

import html as html_lib
import re

_META_TAG = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)
_ATTRIBUTE = re.compile(r"""([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')""")
_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_ARTICLE = re.compile(r"<article\b.*?</article>", re.IGNORECASE | re.DOTALL)
_INVISIBLE = re.compile(
    r"<(script|style|noscript|svg|template|iframe)\b.*?</\1\s*>", re.IGNORECASE | re.DOTALL
)
_BLOCK_END = re.compile(r"</(p|div|h[1-6]|li|section|article|blockquote|figcaption)\s*>|<br\s*/?>",
                        re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")


def meta(html: str, *names: str):
    """The content of the first <meta> whose property or name is one of names,
    in the order given."""
    found = {}
    for tag in _META_TAG.findall(html):
        attributes = {
            key.lower(): html_lib.unescape(double or single)
            for key, double, single in _ATTRIBUTE.findall(tag)
        }
        key = attributes.get("property") or attributes.get("name")
        content = attributes.get("content")
        if key and content and key.lower() not in found:
            found[key.lower()] = content.strip()
    for name in names:
        if found.get(name.lower()):
            return found[name.lower()]
    return None


def title(html: str):
    match = _TITLE.search(html)
    return html_lib.unescape(match.group(1)).strip() if match else None


def visible_text(html: str, max_chars: int = 15000) -> str:
    """The page's readable text, one block per line. The <article> element
    is preferred when there is one, since that is where the story lives."""
    article = _ARTICLE.search(html)
    source = article.group(0) if article else html
    source = " ".join(_INVISIBLE.sub(" ", source).split())
    source = _BLOCK_END.sub("\n", source)
    text = html_lib.unescape(_TAG.sub(" ", source))
    lines = [" ".join(line.split()) for line in text.splitlines()]
    text = "\n".join(line for line in lines if line)
    return text[:max_chars]
