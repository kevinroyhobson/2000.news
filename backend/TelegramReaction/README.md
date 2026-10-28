# TelegramReaction

Grade headlines by replying to the Telegram post with an emoji. A grade does
what `news curate` does at the keyboard — same grades, same Claude-written
rationale, same exemplar cache — and the bot replies with the grade, plus the
rationale for an outstanding pick.

```
you reply 🔥 to a channel post
        |
Telegram --POST /telegram/webhook--> TelegramWebhook/webhook.py
        |                                (verifies the secret token, returns 200 immediately)
        | async invoke
        v
     grade.py  -> resolve message -> headline   (TelegramSentHeadlines.MessageIdIndex)
               -> write Grade + GradedAt        (SubvertedHeadlines)
               -> Claude writes the rationale   (outstanding only)
               -> rebuild the exemplar cache    (META/outstanding_exemplars)
               -> reply with the grade (and rationale)
```

The split exists because Telegram retries any webhook it doesn't get a prompt
200 from, and grading takes a model call plus a table scan. Because grading is
slow, invocations for the same post can overlap; every write carries the
Telegram update id and is refused if a later one already landed, so the newest
grade always wins. A failed cache rebuild raises, so the async retry rebuilds
it again.

## Emoji

Reply to the headline post, or to the bot's rationale under it, with just one
of these:

| Grade | Emoji | Effect |
| --- | --- | --- |
| outstanding | 🔥 | Kept, plus a rationale, plus becomes an exemplar in the Tournament judge's system prompt |
| solid | 👍 🤣 | Kept and served, no exemplar |
| meh | 😐 | Pulled from the site |
| bad | 👎 | Pulled from the site (Get filters meh/bad) |

Replying a different emoji regrades. Any other reply is a note on the rationale
(see `refine.py`).

## Who can grade

Only the channel's admins: only they can post in a channel, and grades are
posts. Emoji reactions grade nothing, because anyone following the channel can
react and Telegram reports reactions anonymously.

## Grading a headline that never got posted

The hourly poster only posts the best headline it hasn't posted yet, so most
headlines (tournament losers, siblings you find in debug mode) never reach the
channel. To grade one of those, post its permalink to the channel on its own:

```
https://www.2000.news/20261008/ccc10cac
```

The bot replies with that headline (and its grade, if it has one), and
replying to that grades it like any other post. `permalink.py` does this by
recording the reply in TelegramSentHeadlines exactly as the hourly poster
records its posts, which also means the hourly poster won't post it again.

A headline already in the channel isn't posted twice; the bot replies to the
original post instead, so you can grade it there.

## Setup

1. Deploy: `news deploy`.
2. Make the bot an **administrator** of the channel. Telegram delivers channel
   posts only to admins.
3. Register the webhook: `news telegram-webhook --register`. This generates
   `/2000news/telegram-webhook-secret` in SSM on first run and subscribes to
   `channel_post`, which is opt-in: nothing arrives without it in
   `allowed_updates`.
4. Check it any time with `news telegram-webhook --info`; `--delete` stops
   delivery.

Only posts made after this shipped are gradable: the mapping from a message
back to a headline is the `MessageId` the poster now records. Records are kept
for 14 days, so you can scroll back about that far.

## Files

- `../TelegramWebhook/webhook.py` — authenticates and dispatches; nothing else.
  Shared with TelegramScoop, since a bot gets one webhook.
- `grade.py` — the grading worker.
- `permalink.py` — posts a headline on request so it can be graded (`tests/test_telegram_permalink.py`).
- `refine.py` — edits a rationale the way a reply asks.
- `../TelegramScoop/commands.py` — what a channel post asks for, grades included
  (`tests/test_telegram_commands.py`).
- `../lib/sent_headlines.py` — the post records grading resolves, shared with TelegramAlert.
- `../lib/curation.py` — grade writes, rationale, exemplar cache. Shared with
  `Scratch/curate_headlines.py`, which is what keeps the two paths identical.
