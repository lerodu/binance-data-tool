"""Thin S3 access layer for external archives.

Both the Hydromancer Reservoir and the official Hyperliquid archive are
**requester-pays** buckets: the caller must be an authenticated AWS principal
(anonymous access is refused) and pays the transfer cost. `RequestPayer` is
therefore attached to every call. A small per-(region, signed) client cache keeps
boto3 clients reusable across threads (boto3 clients are thread-safe for calls).
"""

import io
import threading

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

_CLIENTS = {}
_LOCK = threading.Lock()

_NOT_FOUND = {"404", "NoSuchKey", "NoSuchBucket"}


def client(region, signed=True):
    """Return a cached boto3 S3 client. signed=False -> anonymous (UNSIGNED)."""
    key = (region, signed)
    with _LOCK:
        c = _CLIENTS.get(key)
        if c is None:
            cfg = Config(retries={"max_attempts": 5, "mode": "adaptive"},
                         signature_version=None if signed else UNSIGNED)
            c = boto3.client("s3", region_name=region, config=cfg)
            _CLIENTS[key] = c
        return c


def _payer(requester_pays):
    return {"RequestPayer": "requester"} if requester_pays else {}


def get_bytes(c, bucket, key, requester_pays=True):
    """Fetch an object's bytes. Returns None on 404/NoSuchKey (the per-key analogue
    of a missing month). Raises on auth/other errors so the caller sees them."""
    try:
        return c.get_object(Bucket=bucket, Key=key, **_payer(requester_pays))["Body"].read()
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in _NOT_FOUND:
            return None
        raise


def list_prefixes(c, bucket, prefix, requester_pays=True):
    """List immediate sub-'directories' (CommonPrefixes) under `prefix`."""
    out, token = [], None
    while True:
        kw = dict(Bucket=bucket, Prefix=prefix, Delimiter="/", **_payer(requester_pays))
        if token:
            kw["ContinuationToken"] = token
        resp = c.list_objects_v2(**kw)
        out += [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        if not resp.get("IsTruncated"):
            return out
        token = resp.get("NextContinuationToken")


def list_keys(c, bucket, prefix, requester_pays=True):
    """List all object keys under `prefix` (paginated)."""
    out, token = [], None
    while True:
        kw = dict(Bucket=bucket, Prefix=prefix, **_payer(requester_pays))
        if token:
            kw["ContinuationToken"] = token
        resp = c.list_objects_v2(**kw)
        out += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            return out
        token = resp.get("NextContinuationToken")


def head_ok(c, bucket, key, requester_pays=True):
    """True if the key exists (cheap existence probe)."""
    try:
        c.head_object(Bucket=bucket, Key=key, **_payer(requester_pays))
        return True
    except ClientError:
        return False
