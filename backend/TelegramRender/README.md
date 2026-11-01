# TelegramRender

Make an Instagram post from the channel. Post `/render` with a permalink and
the bot replies with the 4:5 feed image `news insta` would make, as a file you
can save and post.

```
/render https://www.2000.news/20261021/5c31ed2b
/render https://www.2000.news/20261021/5c31ed2b https://www.2000.news/20261021/a406578e
```

The first link is the headline. A second link pins the second story; without
one it's the day's next headline.

Don't like it? Reply `rerender` to the image. That rolls the paper name, photo
column and filler again and keeps the links: the image's caption carries them,
and the rerender reads them back. Replying `/render` to any post with a
permalink in it, like an hourly headline, renders that one.

```
you post /render <permalink>
        |
Telegram --POST /telegram/webhook--> TelegramWebhook/webhook.py
        |                                (verifies the secret token, returns 200 immediately)
        | async invoke
        v
     render.py  -> api.2000.news/<day>/<id>      the paper, led by the headline
                -> Instagram/render.py           draw the front page with Pillow
                -> sendDocument, as a reply      the JPEG, captioned with headline + links
```

It goes back as a document rather than a photo, since Telegram recompresses
photos and shrinks them to 1280px. On a phone, open it and save the image.

## The renderer

`Instagram/render.py` draws the site's newspaper layout with Pillow: the
backdrop, paper name, headline shrunk to fit, the photo at 75% grayscale, and
gray filler bars. It brings its own backdrop and fonts (Liberation Serif and
Mono, metric twins of Times New Roman and Courier New), so the Lambda and
`news insta` on a laptop make the same image.

## Files

- `render.py` — the worker.
- `../Instagram/render.py` — the renderer, shared with `Scratch/instagram_render.py` (`tests/test_instagram_render.py`).
- `../TelegramScoop/commands.py` — what counts as a render request (`tests/test_telegram_commands.py`).
