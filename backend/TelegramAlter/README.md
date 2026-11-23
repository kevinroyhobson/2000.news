# TelegramAlter

Fix a headline that's already on the site. Post `/alter` with its permalink
and what to change, in plain English, and Claude rewrites it.

```
/alter https://www.2000.news/20261122/5c31ed2b say graphics card the first time, so people get it
```

Or reply `/alter` and the instruction to any post with a permalink in it, like
an hourly headline. The bot's reply carries the permalink too, so replying
`/alter` to it alters the headline again:

```
/alter make "maybe best card" the last thing in it
```

```
you post /alter <permalink> <instruction>
        |
Telegram --POST /telegram/webhook--> TelegramWebhook/webhook.py
        |                                (verifies the secret token, returns 200 immediately)
        | async invoke
        v
      alter.py  -> SubvertedHeadlines        the headline and the real one it riffs on
                -> Claude                    edits it the way the instruction asks
                -> SubvertedHeadlines        new Headline, old one appended to Alterations
                -> reply on the post         the new headline, the old one and the permalink
                -> rebuild exemplar cache    outstanding headlines only
```

## What changes

The headline's text, in place. Its permalink, grade, rank and rationale stay
put, so the link serves the new wording within a minute (the API rereads a
day at most once a minute). An outstanding headline is one of the judge's
exemplars, so the cache is rebuilt to show the judge the new wording. The
rationale isn't touched; if the change makes it wrong, reply to it to fix it.

Every alteration is appended to the headline's `Alterations`, oldest first:
the wording before, the wording after, the instruction and when. The first
entry's `Headline` is what the pipeline wrote.

A write is refused if the headline changed while Claude was working on it, so
two alters sent close together can't silently overwrite each other; the bot
says so and you send it again. A failed alter isn't retried, since a retry
would apply the instruction twice.

## Files

- `alter.py` — the worker (`tests/test_telegram_alter.py`).
- `../lib/curation.py` — the prompt (`alter_headline`) and the write (`set_headline`).
- `../TelegramScoop/commands.py` — what counts as an alter request (`tests/test_telegram_commands.py`).
