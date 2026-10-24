#!/usr/bin/env python3
"""
Render 2000.news headlines as Instagram images.

Usage (from backend/ with venv active):
    python3 Scratch/instagram_render.py post https://www.2000.news/20261021/5c31ed2b
    python3 Scratch/instagram_render.py why

`post` makes a 4:5 feed image of one headline, dressed up as a front page with
a random paper name and a second story from the same day. Re-run it to reroll
the paper name and the second story, or pin the second story with --second.
--real runs the story's real headline, styled like the fakes.

`why` makes a 9:16 story image of the README, dressed up as GitHub's README
view, for the "why?" highlight.

Images land in <repo>/instagram/. Needs Playwright (pip install playwright);
it drives the installed Google Chrome, so no browser download is needed.
"""

import argparse
import base64
import html
import random
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from string import Template

import requests
from playwright.sync_api import sync_playwright


REPO = Path(__file__).resolve().parents[2]
TEMPLATES = Path(__file__).resolve().parent / 'instagram'
BACKGROUND_IMAGE = REPO / 'frontend' / 'src' / 'images' / 'nyc.jpeg'
README = REPO / 'README.md'
DEFAULT_OUT_DIR = REPO / 'instagram'
API_BASE = 'https://api.2000.news'

POST_SIZE = {'width': 1080, 'height': 1350}
STORY_SIZE = {'width': 1080, 'height': 1920}

MINOR_WORDS = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'in', 'nor', 'of', 'on', 'or', 'the', 'to', 'vs'}
STRAIGHT_QUOTES = str.maketrans({'‘': "'", '’': "'", '“': '"', '”': '"'})


def parse_headline_url(url):
    match = re.search(r'(\d{8})/([0-9a-z]+)', url)
    if not match:
        sys.exit(f"Not a headline permalink: {url}")
    return match.group(1), match.group(2)


def fetch_paper(day, headline_id):
    response = requests.get(f'{API_BASE}/{day}/{headline_id}', timeout=30)
    response.raise_for_status()
    paper = response.json()
    if paper['Stories'][0]['HeadlineId'] != headline_id:
        sys.exit(f"API didn't return headline {headline_id} for {day}")
    return paper


def fetch_headline_story(url):
    return fetch_paper(*parse_headline_url(url))['Stories'][0]


def capitalize_first_letter(word):
    return re.sub(r'(^\W*|-)([a-z])', lambda match: match.group(1) + match.group(2).upper(), word)


def satire_style(real_headline):
    """Title Case and straight quotes, so a real headline is indistinguishable from the fakes."""
    words = real_headline.translate(STRAIGHT_QUOTES).split()
    styled = []
    for index, word in enumerate(words):
        starts_clause = index == 0 or styled[-1].endswith(':')
        is_last = index == len(words) - 1
        is_minor = word.lower() in MINOR_WORDS and not starts_clause and not is_last
        styled.append(word if is_minor else capitalize_first_letter(word))
    return ' '.join(styled)


def story_title(story):
    return satire_style(story['OriginalHeadline']) if story['ShowOriginal'] else story['Headline']


def filler_lines(count):
    """Port of Story.js: gray bars standing in for body copy, with ragged paragraph ends."""
    lines = []
    is_paragraph_start, is_paragraph_end = True, False
    for _ in range(count):
        classes = 'line paragraph-start' if is_paragraph_start else 'line'
        margin_right = f'{random.random() * 75:.0f}%' if is_paragraph_end else '0'
        lines.append(f'<div class="{classes}" style="margin-right: {margin_right}"></div>')
        is_paragraph_start = is_paragraph_end
        is_paragraph_end = random.random() < 0.2
    return '\n'.join(lines)


def image_tag(story):
    if not story.get('ImageUrl'):
        return ''
    return f'<img src="{html.escape(story["ImageUrl"])}" onerror="this.remove()">'


def fill_template(name, **values):
    background = base64.b64encode(BACKGROUND_IMAGE.read_bytes()).decode()
    base_css = Template((TEMPLATES / 'base.css').read_text()).substitute(
        background=f'data:image/jpeg;base64,{background}')
    return Template((TEMPLATES / name).read_text()).substitute(base_css=base_css, **values)


def headline_column(story):
    return f'<div class="story main">{image_tag(story)}{filler_lines(40)}</div>'


def second_story_column(story):
    """The tail of an earlier story sits above the title so it reads as its own article."""
    return (f'<div class="story side">{filler_lines(random.randint(2, 7))}'
            f'<div class="title">{html.escape(story_title(story))}</div>'
            f'{filler_lines(40)}</div>')


def render_post_html(paper):
    headline, second = paper['Stories'][0], paper['Stories'][1]
    columns = [headline_column(headline), second_story_column(second)]
    random.shuffle(columns)
    return fill_template(
        'post.html',
        paper_name=html.escape(paper['PaperName']),
        headline=html.escape(story_title(headline)),
        columns='\n'.join(columns),
    )


def readme_prose_and_url():
    *prose, url = [p.strip() for p in README.read_text().split('\n\n') if p.strip()]
    return prose, url


def readme_committed_at():
    iso_date = subprocess.run(['git', 'log', '-1', '--format=%cI', '--', README.name],
                              cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()
    committed_at = datetime.fromisoformat(iso_date)
    time = f"{committed_at:%-I:%M%p}".lower()
    return f"{committed_at:%b} {committed_at.day}, {committed_at.year}, {time}"


def render_why_html():
    prose, url = readme_prose_and_url()
    paragraph_tags = [f'<p>{html.escape(p)}</p>' for p in prose]
    paragraph_tags.append(f'<p><a>{html.escape(url)}</a></p>')
    return fill_template('why.html', owner='kevinroyhobson', repo='2000.news',
                         committed_at=readme_committed_at(), paragraphs='\n'.join(paragraph_tags))


def screenshot(page_html, viewport, out_path):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='chrome')
        page = browser.new_page(viewport=viewport)
        page.set_content(page_html, wait_until='networkidle')
        page.screenshot(path=str(out_path), type='jpeg', quality=95)
        browser.close()


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--out-dir', type=Path, default=DEFAULT_OUT_DIR)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subcommands = parser.add_subparsers(dest='command', required=True)
    post_parser = subcommands.add_parser('post', parents=[common], help='4:5 feed image of one headline')
    post_parser.add_argument('url', help='headline permalink, e.g. https://www.2000.news/20261021/5c31ed2b')
    post_parser.add_argument('--real', action='store_true',
                             help="run the story's real headline instead of the satirical one")
    post_parser.add_argument('--second', metavar='URL',
                             help='headline permalink to run as the second story (default: random from that day)')
    subcommands.add_parser('why', parents=[common], help='9:16 story image of the README')
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.command == 'post':
        day, headline_id = parse_headline_url(args.url)
        paper = fetch_paper(day, headline_id)
        paper['Stories'][0]['ShowOriginal'] = args.real
        if args.second:
            paper['Stories'][1] = fetch_headline_story(args.second)
        suffix = '-real' if args.real else ''
        out_path = args.out_dir / f'{day}-{headline_id}{suffix}.jpg'
        screenshot(render_post_html(paper), POST_SIZE, out_path)
        print(story_title(paper['Stories'][0]))
    else:
        out_path = args.out_dir / 'why.jpg'
        screenshot(render_why_html(), STORY_SIZE, out_path)

    print(out_path)


if __name__ == '__main__':
    main()
