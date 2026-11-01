#!/usr/bin/env python3
"""
Render 2000.news headlines as Instagram images.

Usage (from backend/ with venv active):
    python3 Scratch/instagram_render.py post https://www.2000.news/20261021/5c31ed2b
    python3 Scratch/instagram_render.py why

`post` makes a 4:5 feed image of one headline, dressed up as a front page with
a random paper name and a second story from the same day. Re-run it to reroll
the paper name, layout and second story, or pin them with --paper,
--image-side and --second.
--real runs the story's real headline, styled like the fakes.
Instagram/render.py draws it, the same renderer as Telegram's /render.

`why` makes a 9:16 story image of the README, dressed up as GitHub's README
view, for the "why?" highlight. It needs Playwright (pip install playwright),
which screenshots the page in the installed Google Chrome.

Images land in <repo>/instagram/.
"""

import argparse
import html
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from string import Template

from playwright.sync_api import sync_playwright

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from Instagram import render as instagram  # noqa: E402


REPO = Path(__file__).resolve().parents[2]
WHY_TEMPLATE = Path(__file__).resolve().parent / 'instagram' / 'why.html'
README = REPO / 'README.md'
DEFAULT_OUT_DIR = REPO / 'instagram'

STORY_SIZE = {'width': 1080, 'height': 1920}


def parse_headline_url(url):
    match = re.search(r'(\d{8})/([0-9a-z]+)', url)
    if not match:
        sys.exit(f"Not a headline permalink: {url}")
    return match.group(1), match.group(2)


def render_post(args):
    day, headline_id = parse_headline_url(args.url)
    second_key = parse_headline_url(args.second) if args.second else None
    try:
        paper = instagram.fetch_post_paper((day, headline_id), second_key)
    except instagram.RenderError as e:
        sys.exit(str(e))
    paper['Stories'][0]['ShowOriginal'] = args.real
    if args.paper:
        paper['PaperName'] = args.paper
    suffix = '-real' if args.real else ''
    out_path = args.out_dir / f'{day}-{headline_id}{suffix}.jpg'
    out_path.write_bytes(instagram.render_post(paper, args.image_side))
    print(instagram.story_title(paper['Stories'][0]))
    return out_path


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
    return Template(WHY_TEMPLATE.read_text()).substitute(
        owner='kevinroyhobson', repo='2000.news',
        committed_at=readme_committed_at(), paragraphs='\n'.join(paragraph_tags))


def render_why(args):
    out_path = args.out_dir / 'why.jpg'
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='chrome')
        page = browser.new_page(viewport=STORY_SIZE)
        page.set_content(render_why_html(), wait_until='networkidle')
        page.screenshot(path=str(out_path), type='jpeg', quality=95)
        browser.close()
    return out_path


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
    post_parser.add_argument('--paper', metavar='NAME', help='paper name (default: random)')
    post_parser.add_argument('--image-side', choices=['left', 'right'], help='column for the photo (default: random)')
    subcommands.add_parser('why', parents=[common], help='9:16 story image of the README')
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path = render_post(args) if args.command == 'post' else render_why(args)
    print(out_path)


if __name__ == '__main__':
    main()
