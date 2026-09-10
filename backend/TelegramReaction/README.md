# TelegramReaction

Grade headlines by tapping an emoji on the Telegram post. A reaction does what
`news curate` does at the keyboard — same grades, same Claude-written rationale,
same exemplar cache — and the bot replies in-thread with the verdict.

```
you tap 🏆 on a channel post
        |
Telegram --POST /telegram/reaction--> webhook.py   (verifies the secret token,
        |                                           returns 200 immediately)
        | async invoke
        v
     grade.py  -> resolve message -> headline   (TelegramSentHeadlines.MessageIdIndex)
               -> write Grade + GradedAt        (SubvertedHeadlines)
               -> Claude writes the rationale   (outstanding only)
               -> rebuild the exemplar cache    (META/outstanding_exemplars)
               -> reply in the channel
```

The split exists because Telegram retries any webhook it doesn't get a prompt
200 from, and grading takes a model call plus a table scan.

## Emoji

Telegram only permits reactions from its own fixed set, which has no ⭐ — 🏆 is
the closest thing, and 😂 isn't in it either (🤣 is).

| Grade | Emoji | Effect |
| --- | --- | --- |
| outstanding | 🏆 ❤️ 💯 🔥 | Kept, plus a rationale, plus becomes an exemplar in the Tournament judge's system prompt |
| solid | 😁 🤣 👍 👏 | Kept and served, no exemplar |
| meh | 😐 🤨 | Pulled from the site (Get filters meh/bad) |
| bad | 👎 💩 🥱 | Pulled from the site |

Anything unmapped is ignored. When a post carries several graded emoji the
most-reacted one wins, ties breaking toward the stronger opinion (outstanding,
then bad, then solid, then meh).

**Taking the reaction back off undoes the grade** — silently, no reply. A grade
set from the CLI is never cleared this way; only reaction-set grades are.

## Who can grade

Reactions in a broadcast channel are anonymous: Telegram sends aggregate counts
with no user attached, so **any subscriber's tap grades the headline**. That is
usually fine for a small channel and is the default.

To require an identified reactor instead, set `TELEGRAM_REACTION_USER_IDS` on
the `TelegramReactionGrade` function to a comma-separated list of Telegram user
ids. Anonymous updates are then refused — which means channel reactions stop
working, and grading only happens in chats that report the reactor (groups,
private chats).

## Setup

1. Deploy: `news deploy`.
2. Make the bot an **administrator** of the channel. Telegram delivers reaction
   updates only to admins.
3. Register the webhook: `news telegram-webhook --register`. This generates
   `/2000news/telegram-webhook-secret` in SSM on first run and subscribes to
   `message_reaction` and `message_reaction_count` — reaction updates are
   opt-in, and nothing arrives without them in `allowed_updates`.
4. Check it any time with `news telegram-webhook --info`; `--delete` stops
   delivery.

Only posts made after this shipped are gradable: the mapping from a message
back to a headline is the `MessageId` the poster now records. Records are kept
for 14 days, so you can scroll back about that far.

## Configuration

Environment variables on `TelegramReactionGrade`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `TELEGRAM_EMOJI_GRADES` | built-in table above | JSON object of emoji → grade, replacing the defaults wholesale |
| `TELEGRAM_REACTION_USER_IDS` | *(empty)* | Allowlist of reactor user ids; empty accepts anonymous channel taps |
| `TELEGRAM_REPLY_ON_GRADE` | `all` | `all`, `outstanding`, or `none` — which grades get a reply posted |

## Files

- `webhook.py` — authenticates and dispatches; nothing else.
- `grade.py` — the grading worker.
- `reactions.py` — update → grade, pure functions (`tests/test_telegram_reactions.py`).
- `../lib/curation.py` — grade writes, rationale, exemplar cache. Shared with
  `Scratch/curate_headlines.py`, which is what keeps the two paths identical.
