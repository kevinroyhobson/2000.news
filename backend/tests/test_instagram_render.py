import io
import pathlib
import sys

from PIL import Image

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from Instagram import render  # noqa: E402

HEADLINE_WIDTH = 936


def _paper(headline, photo_url=None):
    story = {"Headline": headline, "OriginalHeadline": "a real one", "ShowOriginal": False, "ImageUrl": photo_url}
    second = {"Headline": "Second Story", "OriginalHeadline": "another real one", "ShowOriginal": True}
    return {"PaperName": "The Test Gazette", "Stories": [story, second]}


def test_short_headline_runs_at_full_size():
    text, lines = render.fit_headline("Great Job, Harvard", HEADLINE_WIDTH)
    assert text.font.size == render.HEADLINE_SIZE
    assert lines == ["Great Job, Harvard"]


def test_long_headline_shrinks_until_it_fits():
    headline = ("Senate Confirms Nominee Who Promised Under Oath That He Would Eventually "
                "Learn What the Department Does, Probably by Spring")
    text, lines = render.fit_headline(headline, HEADLINE_WIDTH)
    assert text.font.size < render.HEADLINE_SIZE
    assert render.BORDER + 10 + text.height(lines) <= render.HEADLINE_MAX_HEIGHT
    assert all(text.font.getlength(line) <= HEADLINE_WIDTH for line in lines)


def test_headline_that_never_fits_stops_at_the_minimum_size():
    text, _ = render.fit_headline("Word " * 200, HEADLINE_WIDTH)
    assert text.font.size == render.HEADLINE_MIN_SIZE


def test_real_headline_is_styled_like_the_fakes():
    assert render.satire_style("Coach: Kucherov ‘committed’ to the Lightning") == (
        "Coach: Kucherov 'Committed' to the Lightning")


def test_post_is_a_feed_sized_jpeg_with_or_without_a_photo(monkeypatch):
    photo = Image.new("RGB", (800, 600), "red")
    monkeypatch.setattr(render, "_fetch_photo", lambda url: photo if url else None)
    for photo_url in ("https://example.com/photo.jpg", None):
        for side in ("left", "right"):
            post = Image.open(io.BytesIO(render.render_post(_paper("Great Job, Harvard", photo_url), side)))
            assert (post.format, post.size) == ("JPEG", render.POST_SIZE)
