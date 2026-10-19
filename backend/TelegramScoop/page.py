"""Pull the useful parts out of an article page's HTML with the standard
library: meta tags, the lead image, and the visible text for a model to read.
Pure functions (tests/test_scoop_page.py)."""

import html as html_lib
import json
import re
from urllib.parse import urljoin

_META_TAG = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)
_ATTRIBUTE = re.compile(r"""([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+))""")
_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_ARTICLE = re.compile(r"<article\b.*?</article>", re.IGNORECASE | re.DOTALL)
_INVISIBLE = re.compile(
    r"<(script|style|noscript|svg|template|iframe)\b.*?</\1\s*>", re.IGNORECASE | re.DOTALL
)
_BLOCK_END = re.compile(r"</(p|div|h[1-6]|li|section|article|blockquote|figcaption)\s*>|<br\s*/?>",
                        re.IGNORECASE)
_TAG = re.compile(r"<[^>]+>")
_JSON_LD = re.compile(r"<script\b[^>]*application/ld\+json[^>]*>(.*?)</script\s*>",
                      re.IGNORECASE | re.DOTALL)
_LINK_TAG = re.compile(r"<link\b[^>]*>", re.IGNORECASE)
_IMG_TAG = re.compile(r"<img\b[^>]*>", re.IGNORECASE)
_IMG_SOURCES = ("data-src", "data-lazy-src", "data-original", "src")
_DECORATION = re.compile(r"(?<![a-z])(logo|avatar|icon|sprite|pixel|spacer|placeholder|blank)s?(?![a-z])",
                         re.IGNORECASE)
_MIN_IMG_DIMENSION = 200


def meta(html: str, *names: str):
    """The content of the first <meta> whose property, name or itemprop is one
    of names, in the order given."""
    found = {}
    for tag in _META_TAG.findall(html):
        attributes = _attributes(tag)
        key = (attributes.get("property") or attributes.get("name")
               or attributes.get("itemprop") or "").lower()
        content = (attributes.get("content") or "").strip()
        if key and content:
            found.setdefault(key, content)
    for name in names:
        if content := found.get(name.lower()):
            return content
    return None


def image(html: str, page_url: str):
    """The story's lead image as an absolute URL, or None. Social-card meta
    tags are what publishers pick for sharing, so they come first; then the
    article's structured data; then the first photo-sized <img> in the story."""
    found = (meta(html, "og:image", "og:image:secure_url", "og:image:url", "twitter:image",
                  "twitter:image:src", "image", "thumbnail")
             or _json_ld_image(html)
             or _link_image(html)
             or _first_photo(html))
    return urljoin(page_url, found) if found else None


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


def _attributes(tag: str) -> dict:
    return {key.lower(): html_lib.unescape(double or single or bare)
            for key, double, single, bare in _ATTRIBUTE.findall(tag)}


def _json_ld_image(html: str):
    nodes = [node for block in _JSON_LD.findall(html) for node in _json_ld_nodes(_parse_json(block))]
    nodes_by_id = {node["@id"]: node for node in nodes if isinstance(node.get("@id"), str)}
    for node in nodes:
        if _is_article(node) and (url := _image_url(node.get("image"), nodes_by_id)):
            return url
    return None


def _parse_json(text: str):
    try:
        return json.loads(text)
    except ValueError:
        return None


def _json_ld_nodes(data):
    if isinstance(data, list):
        for item in data:
            yield from _json_ld_nodes(item)
    elif isinstance(data, dict):
        yield data
        yield from _json_ld_nodes(data.get("@graph"))


def _is_article(node: dict) -> bool:
    types = node.get("@type")
    types = types if isinstance(types, list) else [types]
    return any(isinstance(t, str) and (t.endswith("Article") or t == "BlogPosting") for t in types)


def _image_url(value, nodes_by_id: dict):
    """schema.org lets image be a URL, an ImageObject, a reference to one
    elsewhere in the graph by @id, or a list of any of those."""
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, list):
        return next((url for url in (_image_url(item, nodes_by_id) for item in value) if url), None)
    if isinstance(value, dict):
        referenced = nodes_by_id.get(value.get("@id"), {})
        return _image_url(value.get("url") or value.get("contentUrl")
                          or referenced.get("url") or referenced.get("contentUrl"), nodes_by_id)
    return None


def _link_image(html: str):
    for tag in _LINK_TAG.findall(html):
        attributes = _attributes(tag)
        if attributes.get("rel", "").lower() == "image_src" and attributes.get("href"):
            return attributes["href"]
    return None


def _first_photo(html: str):
    article = _ARTICLE.search(html)
    for source in ([article.group(0)] if article else []) + [html]:
        for tag in _IMG_TAG.findall(_INVISIBLE.sub(" ", source)):
            if url := _photo_url(_attributes(tag)):
                return url
    return None


def _photo_url(attributes: dict):
    if _too_small(attributes):
        return None
    return next((url for url in _img_candidates(attributes) if _looks_like_photo(url)), None)


def _img_candidates(attributes: dict):
    yield from (attributes[name] for name in _IMG_SOURCES if attributes.get(name))
    first_srcset_entry = attributes.get("srcset", "").split(",")[0].split()
    if first_srcset_entry:
        yield first_srcset_entry[0]


def _looks_like_photo(url: str) -> bool:
    path = url.lower().split("?")[0]
    return not (path.startswith("data:") or path.endswith((".svg", ".gif"))
                or _DECORATION.search(path))


def _too_small(attributes: dict) -> bool:
    for dimension in ("width", "height"):
        value = attributes.get(dimension, "").removesuffix("px")
        if value.isdigit() and int(value) < _MIN_IMG_DIMENSION:
            return True
    return False
