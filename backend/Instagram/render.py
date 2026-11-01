"""Render a 2000.news headline as a 4:5 Instagram feed post: a front page with
a paper name, the headline and its photo, and a second story from the same day.

Shared by `news insta` (Scratch/instagram_render.py) and the Telegram /render
worker (TelegramRender/render.py), so both make the same image. Each render
rerolls the paper name (the API picks one per request), the photo column and
the filler copy.

The layout is the site's newspaper (Newspaper.css, Story.js), drawn with
Pillow. Liberation Serif and Mono stand in for Times New Roman and Courier
New, whose metrics they share.
"""

import io
import random
import re
from dataclasses import dataclass
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont, ImageOps, UnidentifiedImageError

HERE = Path(__file__).resolve().parent
BACKGROUND_IMAGE = HERE / 'nyc.jpeg'
SERIF = HERE / 'fonts' / 'LiberationSerif-Regular.ttf'
MONO = HERE / 'fonts' / 'LiberationMono-Regular.ttf'
API_BASE = 'https://api.2000.news'

POST_SIZE = (1080, 1350)
PAPER_GRAY = '#e8e8e8'
FILLER_GRAY = '#a9a9a9'
BLACK = 'black'
BORDER = 2

# CSS "line-height: normal" for each font, as a multiple of its size.
SERIF_LINE_HEIGHT = 1.149
MONO_LINE_HEIGHT = 1.133

NEWSPAPER_BOX = (44, 72, 1036, 1240)
CLOSE_BOX_OFFSET = 8
CLOSE_BOX_SIZE = 38 + 2 * BORDER
PAPER_NAME_SIZE = 58
PAPER_NAME_PADDING = 64
HEADLINE_SIZE = 96
HEADLINE_MIN_SIZE = 48
HEADLINE_MAX_HEIGHT = 420
HEADLINE_LINE_HEIGHT = 1.1
HEADLINE_MARGIN_TOP = 12
HEADLINE_MARGIN_BOTTOM = 24
HEADLINE_PADDING_TOP = 10
HEADLINE_PADDING = 28
STORY_SIZE = 42
STORY_TITLE_PADDING_TOP = 12
STORY_TITLE_PADDING_BOTTOM = 14
STORIES_MARGIN = 16
STORIES_GAP = 24
SIDE_COLUMN_SHARE = 0.36
PHOTO_MAX_HEIGHT = 440
PHOTO_GRAYSCALE = 0.75
FILLER_HEIGHT = 13
FILLER_SPACING = 9
FILLER_INDENT = 14
PARAGRAPH_INDENT = 72
CREDIT = '2000.news'
CREDIT_SIZE = 30

MINOR_WORDS = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'in', 'nor', 'of', 'on', 'or', 'the', 'to', 'vs'}
STRAIGHT_QUOTES = str.maketrans({'‘': "'", '’': "'", '“': '"', '”': '"'})


class RenderError(Exception):
    """What was asked for can't be made into a post."""


def permalink(day, headline_id):
    return f'https://www.2000.news/{day}/{headline_id}'


def fetch_paper(day, headline_id):
    """The day's paper, led by the headline. The API leads with the day's best
    headline instead when it has no such headline."""
    response = requests.get(f'{API_BASE}/{day}/{headline_id}', timeout=30)
    response.raise_for_status()
    paper = response.json()
    stories = paper.get('Stories') or []
    if not stories or stories[0]['HeadlineId'] != headline_id:
        raise RenderError(f"No headline at {permalink(day, headline_id)}")
    return paper


def fetch_post_paper(headline_key, second_key=None):
    """The paper a post shows: the headline's day, led by the headline, with
    second_key's headline as the second story, or the day's next one."""
    paper = fetch_paper(*headline_key)
    if second_key:
        paper['Stories'][1:] = [fetch_paper(*second_key)['Stories'][0]]
    if len(paper['Stories']) < 2:
        raise RenderError(f"{headline_key[0]} has no other headline to run as the second story")
    return paper


def satire_style(real_headline):
    """Title Case and straight quotes, so a real headline is indistinguishable from the fakes."""
    words = real_headline.translate(STRAIGHT_QUOTES).split()
    styled = []
    for index, word in enumerate(words):
        starts_clause = index == 0 or styled[-1].endswith(':')
        is_last = index == len(words) - 1
        is_minor = word.lower() in MINOR_WORDS and not starts_clause and not is_last
        styled.append(word if is_minor else _capitalize_first_letter(word))
    return ' '.join(styled)


def _capitalize_first_letter(word):
    return re.sub(r'(^\W*|-)([a-z])', lambda match: match.group(1) + match.group(2).upper(), word)


def story_title(story):
    return satire_style(story['OriginalHeadline']) if story['ShowOriginal'] else story['Headline']


def render_post(paper, image_side=None):
    """The post as JPEG bytes. image_side is 'left' or 'right' for the photo's
    column, random when not given."""
    post = _cover(Image.open(BACKGROUND_IMAGE).convert('RGB'), POST_SIZE, centering=(0.5, 0))
    post.paste(_newspaper(paper, image_side or random.choice(['left', 'right'])), NEWSPAPER_BOX[:2])
    _draw_credit(ImageDraw.Draw(post))
    out = io.BytesIO()
    post.save(out, 'JPEG', quality=95)
    return out.getvalue()


@dataclass
class Text:
    font: ImageFont.FreeTypeFont
    line_height: float

    @classmethod
    def serif(cls, size, line_height=SERIF_LINE_HEIGHT):
        return cls(ImageFont.truetype(str(SERIF), size), size * line_height)

    def wrap(self, text, width):
        lines = []
        for word in text.split():
            candidate = f'{lines[-1]} {word}' if lines else word
            if lines and self.font.getlength(candidate) <= width:
                lines[-1] = candidate
            else:
                lines.append(word)
        return lines

    def height(self, lines):
        return len(lines) * self.line_height

    def draw_centered(self, draw, lines, left, top, width):
        """Each line centered in [left, left + width], half-leading above and
        below as in CSS. Returns the bottom of the last line."""
        ascent, descent = self.font.getmetrics()
        half_leading = (self.line_height - ascent - descent) / 2
        for index, line in enumerate(lines):
            baseline = top + index * self.line_height + half_leading + ascent
            x = left + (width - self.font.getlength(line)) / 2
            draw.text((x, baseline), line, font=self.font, fill=BLACK, anchor='ls')
        return top + self.height(lines)


def _newspaper(paper, image_side):
    width, height = NEWSPAPER_BOX[2] - NEWSPAPER_BOX[0], NEWSPAPER_BOX[3] - NEWSPAPER_BOX[1]
    newspaper = Image.new('RGB', (width, height), PAPER_GRAY)
    draw = ImageDraw.Draw(newspaper)

    _draw_close_box(draw)
    y = _draw_paper_name(draw, paper['PaperName'], width)
    y = _draw_headline(draw, story_title(paper['Stories'][0]), y, width)
    _draw_stories(newspaper, draw, paper['Stories'], image_side, y, width)
    return newspaper


def _draw_close_box(draw):
    far_edge = CLOSE_BOX_OFFSET + CLOSE_BOX_SIZE - 1
    draw.rectangle((CLOSE_BOX_OFFSET, CLOSE_BOX_OFFSET, far_edge, far_edge),
                   fill='white', outline=BLACK, width=BORDER)


def _draw_paper_name(draw, name, width):
    text = Text.serif(PAPER_NAME_SIZE)
    draw.rectangle((0, 0, width - 1, BORDER - 1), fill=BLACK)
    inner_width = width - 2 * PAPER_NAME_PADDING
    return text.draw_centered(draw, text.wrap(name, inner_width), PAPER_NAME_PADDING, BORDER, inner_width)


def _draw_headline(draw, headline, top, width):
    """Bordered and padded like the site's headline, at the largest size that
    keeps it within HEADLINE_MAX_HEIGHT. Returns where the stories start."""
    top += HEADLINE_MARGIN_TOP
    inner_width = width - 2 * HEADLINE_PADDING
    text, lines = fit_headline(headline, inner_width)
    draw.rectangle((0, top, width - 1, top + BORDER - 1), fill=BLACK)
    bottom = text.draw_centered(draw, lines, HEADLINE_PADDING, top + BORDER + HEADLINE_PADDING_TOP, inner_width)
    return bottom + HEADLINE_MARGIN_BOTTOM


def fit_headline(headline, width):
    """The headline's Text and wrapped lines: 96px, shrunk 2px at a time down
    to 48px until it fits HEADLINE_MAX_HEIGHT with its border and padding."""
    size = HEADLINE_SIZE
    while True:
        text = Text.serif(size, HEADLINE_LINE_HEIGHT)
        lines = text.wrap(headline, width)
        if BORDER + HEADLINE_PADDING_TOP + text.height(lines) <= HEADLINE_MAX_HEIGHT or size <= HEADLINE_MIN_SIZE:
            return text, lines
        size -= 2


def _draw_stories(newspaper, draw, stories, image_side, top, width):
    columns_width = width - 2 * STORIES_MARGIN
    side_width = round(columns_width * SIDE_COLUMN_SHARE)
    main_width = columns_width - side_width - STORIES_GAP
    left, right = STORIES_MARGIN, STORIES_MARGIN + main_width + STORIES_GAP
    if image_side == 'right':
        left, right = STORIES_MARGIN + side_width + STORIES_GAP, STORIES_MARGIN

    _draw_headline_column(newspaper, draw, stories[0], left, top, main_width)
    _draw_second_story_column(draw, stories[1], right, top, side_width)


def _draw_headline_column(newspaper, draw, story, left, top, width):
    photo = _fetch_photo(story.get('ImageUrl'))
    if photo:
        height = min(round(width * photo.height / photo.width), PHOTO_MAX_HEIGHT)
        newspaper.paste(_faded(_cover(photo, (width, height))), (left, round(top)))
        top += height
    _draw_filler(draw, left, top, width, 40)


def _draw_second_story_column(draw, story, left, top, width):
    """The tail of an earlier story sits above the title so it reads as its own article."""
    y = _draw_filler(draw, left, top, width, random.randint(2, 7))
    text = Text.serif(STORY_SIZE)
    title_top = y + FILLER_SPACING + STORY_TITLE_PADDING_TOP
    y = text.draw_centered(draw, text.wrap(story_title(story), width), left, title_top, width)
    _draw_filler(draw, left, y + STORY_TITLE_PADDING_BOTTOM, width, 40)


def _draw_filler(draw, left, top, width, count):
    """Gray bars standing in for body copy, with indented paragraph starts and
    ragged paragraph ends, as in Story.js. Returns the bottom of the last bar."""
    y = top
    for starts_paragraph, ragged_share in _filler_lines(count):
        y += FILLER_SPACING
        x0 = left + (PARAGRAPH_INDENT if starts_paragraph else FILLER_INDENT)
        x1 = left + width - ragged_share * width
        if x1 > x0:
            draw.rectangle((x0, y, x1 - 1, y + FILLER_HEIGHT - 1), fill=FILLER_GRAY)
        y += FILLER_HEIGHT
    return y


def _filler_lines(count):
    """(starts a paragraph, share of the width left blank) for each bar."""
    is_paragraph_start, is_paragraph_end = True, False
    for _ in range(count):
        yield is_paragraph_start, random.random() * 0.75 if is_paragraph_end else 0
        is_paragraph_start = is_paragraph_end
        is_paragraph_end = random.random() < 0.2


def _draw_credit(draw):
    text = Text(ImageFont.truetype(str(MONO), CREDIT_SIZE), CREDIT_SIZE * MONO_LINE_HEIGHT)
    padding_x, padding_y = 14, 2
    width = padding_x * 2 + text.font.getlength(CREDIT)
    height = BORDER + padding_y * 2 + text.line_height
    bottom = POST_SIZE[1]
    top = bottom - height
    draw.rectangle((0, top, width + BORDER - 1, bottom - 1), fill=PAPER_GRAY)
    draw.rectangle((0, top, width + BORDER - 1, top + BORDER - 1), fill=BLACK)
    draw.rectangle((width, top, width + BORDER - 1, bottom - 1), fill=BLACK)
    text.draw_centered(draw, [CREDIT], 0, top + BORDER + padding_y, width)


def _fetch_photo(url):
    """The story's photo, or None when it has none or it won't load, which is
    how the site treats a broken image too."""
    if not url:
        return None
    try:
        response = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        return Image.open(io.BytesIO(response.content)).convert('RGB')
    except (requests.RequestException, UnidentifiedImageError, OSError) as e:
        print(f"Running without the photo at {url} ({type(e).__name__}: {e}).")
        return None


def _cover(image, size, centering=(0.5, 0.5)):
    """CSS object-fit/background-size: cover."""
    return ImageOps.fit(image, size, Image.Resampling.LANCZOS, centering=centering)


def _faded(photo):
    """CSS grayscale(75%), as the site shows photos."""
    return Image.blend(photo, ImageOps.grayscale(photo).convert('RGB'), PHOTO_GRAYSCALE)
