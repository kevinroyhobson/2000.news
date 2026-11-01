# TelegramScoop

Add a story to the paper from the channel. Post a bare link, or `/scoop` and
what you're after, and the bot replies with how it went and the best headline
it wrote.

```
you post https://example.com/story     (or: /scoop haley stevens endorsing mike rogers)
        |
Telegram --POST /telegram/webhook--> TelegramWebhook/webhook.py
        |                                (verifies the secret token, returns 200 immediately)
        | async invoke
        v
     scoop.py  -> article.py / editor.py          fetch the story (or the editor's picks, up to 3)
               -> Stories table                   filed under today, FetchCategory telegram:*
               -> Subvert/on_demand.py            brainstorm + generate, synchronously
               -> mini_tournament.py              rank this request's headlines among themselves
               -> reply on the post               "Wrote 19 headlines for “…”. Best: …"
```

Nobody waits on the scheduled pipeline, so it runs through the Batch API and
takes anywhere up to an hour. Someone is waiting on this, so its handful of
calls run synchronously at standard pricing: about a minute for a link, a few
for a search. The prompts, parsing, writes and judge are the pipeline's own.

## What the headlines do next

They are written with `Deferred=True`, and the tournament stream trigger
starts nothing for a batch made up entirely of those. The next scheduled fetch
sets off a run as usual, and that run picks them up (they have no
`TournamentBatch`) into the same-day and cross-day rankings. Until then they
are reachable by permalink but not on the front page.

The ranking in the reply is only for the reply. Nothing is written back.

## Links

A 2000.news permalink isn't a story to fetch; it posts that headline for
grading instead (see `../TelegramReaction/README.md`). Anything else with an
`og:image` works: the image, title and publish time come
from the page's meta tags, and Claude picks the opening paragraphs out of the
page text. A page with no image, no text, or a hostile status code gets a
reply saying so instead of a story.

`/scoop <url>` is the same as posting the link bare.

nytimes.com links go through the Article Search API instead, since the pages
serve a paywall stub. That needs a key from developer.nytimes.com at
`/2000news/NYT_API_KEY` in SSM; without one, NYT links get the page treatment
like everything else.

## Searches

`/scoop <request>` goes to the scoop editor (`editor.py`). The request can be a
name, a topic or a sentence: `/scoop joe burrow`, `/scoop find something about
haley stevens endorsing mike rogers`. Claude reads it, searches newsdata.io
(recent articles, top-tier US sources) up to 4 times, headlines first and then
article text, and picks up to 3 stories that are about what you asked. Each
pick carries a note for the writers, saved as its `EditorNote`.

Stories filed in the last two days that never got headlines are on the table
too. Picking one moves it into today as a scoop story, since the tournament
only ranks today's headlines and the Stories stream leaves scoop stories alone.
Ones the paper already wrote up, and scheduled ones filed in the last two hours
(their batch may still be running), can't be picked.

When nothing fits, the reply says so and links the closest misses. Post one of
them bare to run it anyway.

## Who can post

Broadcast channels only let administrators post, so give anyone who should
be able to do this the **Post Messages** admin right and nothing else. Posts
from any other chat, including DMs to the bot, are ignored.

## Setup

Same webhook as TelegramReaction: `news telegram-webhook --register` after
deploying subscribes to `channel_post`.

## Files

- `commands.py` — which posts are requests, pure functions (`tests/test_telegram_commands.py`).
- `page.py` — meta tags and visible text out of HTML, pure functions (`tests/test_scoop_page.py`).
- `article.py` — a URL into a story dict.
- `mini_tournament.py` — the reply's ranking.
- `editor.py` — the scoop editor's search loop and picks (`tests/test_scoop_editor.py`).
- `scoop.py` — the worker.
