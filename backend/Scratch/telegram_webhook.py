#!/usr/bin/env python3
"""
Point the Telegram bot at the reaction webhook (and check on it later).

Reaction updates are opt-in: Telegram only delivers them if the bot is an
administrator in the channel AND the reaction update types are named in
allowed_updates. Registering here does both halves of that, plus generates a
shared secret Telegram echoes back on every delivery so the public endpoint
can tell real updates from anything else that finds the URL.

Usage (from backend/ with venv active, or `news telegram-webhook ...`):
    python3 Scratch/telegram_webhook.py --register
    python3 Scratch/telegram_webhook.py --info
    python3 Scratch/telegram_webhook.py --delete

The secret lives in SSM at /2000news/telegram-webhook-secret and is created on
first --register. Re-run --register after any change to it.
"""

import argparse
import os
import secrets
import sys

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from lib.telegram import call  # noqa: E402

DEFAULT_URL = 'https://api.2000.news/telegram/reaction'
SECRET_PARAMETER = '/2000news/telegram-webhook-secret'
# Without these two, Telegram sends nothing when someone taps an emoji.
ALLOWED_UPDATES = ['message_reaction', 'message_reaction_count']
REGION = os.getenv('CURATION_REGION', 'us-east-2')


def ensure_secret(ssm) -> str:
    try:
        return ssm.get_parameter(Name=SECRET_PARAMETER, WithDecryption=True)['Parameter']['Value']
    except ssm.exceptions.ParameterNotFound:
        secret = secrets.token_urlsafe(32)
        ssm.put_parameter(Name=SECRET_PARAMETER, Value=secret, Type='SecureString')
        print(f"Created {SECRET_PARAMETER}.")
        return secret


def main():
    ap = argparse.ArgumentParser()
    action = ap.add_mutually_exclusive_group(required=True)
    action.add_argument('--register', action='store_true',
                        help='point the bot at the webhook and subscribe to reaction updates')
    action.add_argument('--info', action='store_true', help='show the current webhook status')
    action.add_argument('--delete', action='store_true', help='stop delivering updates')
    ap.add_argument('--url', default=DEFAULT_URL, help=f'webhook URL (default: {DEFAULT_URL})')
    ap.add_argument('--drop-pending', action='store_true',
                    help='[--register] discard updates queued while the webhook was down')
    args = ap.parse_args()

    if args.info:
        info = call('getWebhookInfo', {})
        for key in sorted(info):
            print(f"{key}: {info[key]}")
        if not info.get('url'):
            print("\nNo webhook registered. Run with --register.")
        elif 'message_reaction' not in (info.get('allowed_updates') or []):
            print("\nWarning: reaction updates are NOT in allowed_updates — "
                  "emoji taps will not be delivered. Re-run with --register.")
        return

    if args.delete:
        call('deleteWebhook', {})
        print("Webhook deleted; the bot will no longer receive updates.")
        return

    secret = ensure_secret(boto3.client('ssm', region_name=REGION))
    call('setWebhook', {
        'url': args.url,
        'secret_token': secret,
        'allowed_updates': ALLOWED_UPDATES,
        'drop_pending_updates': args.drop_pending,
    })
    print(f"Webhook set to {args.url}")
    print(f"Subscribed to: {', '.join(ALLOWED_UPDATES)}")
    print("\nThe bot must be an administrator in the channel for reaction "
          "updates to be delivered.")


if __name__ == '__main__':
    main()
