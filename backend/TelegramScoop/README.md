# TelegramScoop

Add a story to the paper from the channel. Post a bare link, or `/scoop <topic>`
to search the news, and the bot replies with how it went and the best headline
it wrote.

```
you post https://example.com/story     (or: /scoop taylor swift)
        |
Telegram --POST /telegram/webhook--> TelegramWebhook/webhook.py
        |                                (verifies the secret token, returns 200 immediately)
        | async invoke
        v
     scoop.py  -> article.py / lib.topic_search   fetch the story (or up to 3 for a search)
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

Anything with an `og:image` works: the image, title and publish time come
from the page's meta tags, and Claude picks the opening paragraphs out of the
page text. A page with no image, no text, or a hostile status code gets a
reply saying so instead of a story.

nytimes.com links go through the Article Search API instead, since the pages
serve a paywall stub. That needs a key from developer.nytimes.com at
`/2000news/NYT_API_KEY` in SSM; without one, NYT links get the page treatment
like everything else.

## Searches

`/scoop <topic>` is `news fetch-topic` from the keyboard: a newsdata.io query,
top-tier sources, up to 3 new stories. A topic with nothing new gets a reply
saying so.

A plain topic is sent with every word required (`cincinnati AND reds`), since
newsdata otherwise matches the words independently anywhere in an article.
Quotes and operators you type are passed through, so `/scoop "joe burrow"` asks
for the phrase and `/scoop bengals OR browns` asks for either.

## Who can post

Broadcast channels only let administrators post, so give anyone who should
be able to do this the **Post Messages** admin right and nothing else. Posts
from any other chat, including DMs to the bot, are ignored.

## Setup

Same webhook as TelegramReaction: `news telegram-webhook --register` after
deploying subscribes to `channel_post` alongside the reaction updates.

## Files

- `commands.py` — which posts are requests, pure functions (`tests/test_telegram_commands.py`).
- `page.py` — meta tags and visible text out of HTML, pure functions (`tests/test_scoop_page.py`).
- `article.py` — a URL into a story dict.
- `mini_tournament.py` — the reply's ranking.
- `scoop.py` — the worker.
