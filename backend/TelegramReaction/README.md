# TelegramReaction

Grade headlines by tapping an emoji on the Telegram post. A reaction does what
`news curate` does at the keyboard — same grades, same Claude-written rationale,
same exemplar cache — and the bot replies to the post with the grade, plus the
rationale for an outstanding pick.

```
you tap 🔥 on a channel post
        |
Telegram --POST /telegram/webhook--> TelegramWebhook/webhook.py
        |                                (verifies the secret token, returns 200 immediately)
        | async invoke
        v
     grade.py  -> resolve message -> headline   (TelegramSentHeadlines.MessageIdIndex)
               -> write Grade + GradedAt        (SubvertedHeadlines)
               -> Claude writes the rationale   (outstanding only)
               -> rebuild the exemplar cache    (META/outstanding_exemplars)
               -> reply to the post with the grade (and rationale)
```

The split exists because Telegram retries any webhook it doesn't get a prompt
200 from, and grading takes a model call plus a table scan. Because grading is
slow and Telegram sends one update per tap, invocations for the same post can
overlap; every write carries the Telegram update id and is refused if a later
one already landed, so the newest reaction always wins. A failed cache rebuild
raises, so the async retry rebuilds it again.

## Emoji

| Grade | Emoji | Effect |
| --- | --- | --- |
| outstanding | 🔥 | Kept, plus a rationale, plus becomes an exemplar in the Tournament judge's system prompt |
| solid | 👍 🤣 | Kept and served, no exemplar |
| bad | 👎 | Pulled from the site (Get filters meh/bad) |

No emoji means meh; that grade stays a CLI-only call. Anything unmapped is
ignored, including premium custom emoji. Telegram only permits reactions from
its own fixed set, which is why it's 🤣 and not 😂. When a post carries several
graded emoji the most-reacted one wins, ties breaking toward the stronger
opinion (outstanding, then bad, then solid).

**Taking the reaction back off undoes the grade** and replies "Grade cleared". A grade
set from the CLI is never cleared this way; only reaction-set grades are.

## Who can grade

Reactions in a broadcast channel are anonymous: Telegram sends aggregate counts
with no user attached, so **any subscriber's tap grades the headline**.

## Setup

1. Deploy: `news deploy`.
2. Make the bot an **administrator** of the channel. Telegram delivers reaction
   updates only to admins.
3. Register the webhook: `news telegram-webhook --register`. This generates
   `/2000news/telegram-webhook-secret` in SSM on first run and subscribes to
   `message_reaction` and `message_reaction_count` (plus `channel_post` for
   TelegramScoop) — these updates are opt-in, and nothing arrives without
   them in `allowed_updates`.
4. Check it any time with `news telegram-webhook --info`; `--delete` stops
   delivery.

Only posts made after this shipped are gradable: the mapping from a message
back to a headline is the `MessageId` the poster now records. Records are kept
for 14 days, so you can scroll back about that far.

## Files

- `../TelegramWebhook/webhook.py` — authenticates and dispatches; nothing else.
  Shared with TelegramScoop, since a bot gets one webhook.
- `grade.py` — the grading worker.
- `reactions.py` — update → grade, pure functions (`tests/test_telegram_reactions.py`).
- `../lib/curation.py` — grade writes, rationale, exemplar cache. Shared with
  `Scratch/curate_headlines.py`, which is what keeps the two paths identical.
