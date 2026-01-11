"""Fetch secrets from SSM Parameter Store with caching."""

import boto3

_cache = {}
_ssm = None


def get_secret(name):
    """Fetch secret from SSM Parameter Store with caching."""
    if name not in _cache:
        global _ssm
        if _ssm is None:
            _ssm = boto3.client('ssm')
        response = _ssm.get_parameter(Name=f"/2000news/{name}", WithDecryption=True)
        _cache[name] = response['Parameter']['Value']
    return _cache[name]
