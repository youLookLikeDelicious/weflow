# WeFlow Live Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add management-configured, multi-source WeFlow live synchronization to sale-agent, syncing WeChat entities first and chat messages second.

**Architecture:** sale-agent owns the ingest lifecycle. WeFlow remains the local WeChat data source exposed through HTTP/SSE. sale-agent stores source configuration in Postgres, encrypts tokens, syncs contacts/sessions/group members, backfills messages, then consumes SSE events with idempotent writes and async analysis job enqueueing.

**Tech Stack:** Python 3.9+, Postgres via `psycopg`, stdlib `http.client`/`urllib`, `cryptography` Fernet for token encryption, existing sale-agent HTTP API.

---

## File Structure

- Modify `/Users/xinmi/www/sale-agent/pyproject.toml`: add `cryptography` dependency and console script for live ingest.
- Modify `/Users/xinmi/www/sale-agent/sale_agent/postgres_schema.sql`: add WeFlow live sync tables and `chat_messages` revoke fields.
- Modify `/Users/xinmi/www/sale-agent/sale_agent/api.py`: add management API routes.
- Modify `/Users/xinmi/www/sale-agent/sale_agent/api_service.py`: expose WeFlow source CRUD/test/sync-now service functions.
- Create `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/__init__.py`: package marker.
- Create `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/crypto.py`: token encryption/decryption.
- Create `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/models.py`: source and payload normalization helpers.
- Create `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/client.py`: WeFlow HTTP/SSE client.
- Create `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/repository.py`: Postgres persistence and idempotency.
- Create `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/sync_service.py`: source discovery, entity sync, backfill, live event handling.
- Create `/Users/xinmi/www/sale-agent/sale_agent/weflow_live_ingest.py`: CLI loop that reads enabled sources from DB.
- Create `/Users/xinmi/www/sale-agent/tests/test_weflow_live_crypto.py`.
- Create `/Users/xinmi/www/sale-agent/tests/test_weflow_live_repository.py`.
- Create `/Users/xinmi/www/sale-agent/tests/test_weflow_live_api.py`.

## Confirmed Decisions

- Support multiple WeFlow sources from day one.
- Configure sources in the management backend, not only JSON/env.
- Use JSON/env only as optional bootstrap fallback later if needed.
- sale-agent actively connects to WeFlow; WeFlow does not write sale-agent DB.
- Cross-machine supported through `base_url`; WeFlow should listen on `0.0.0.0` only on trusted LAN/VPN.
- Source `seller_wechat_id` is auto-discovered from WeFlow when possible and can be manually overridden.
- Sync order: source/account, contacts, sessions, group members, message backfill, SSE live.
- Default initial backfill: recent 365 days.
- Reconnect backfill overlap: 300 seconds.
- Alias history is recorded when `alias` changes.
- No cross-sales automatic customer merge.
- Media is recorded as reference/payload first; heavy media processing remains async.
- `message.revoke` is recorded and marks original `chat_messages` as revoked; messages are not deleted.
- Ingest and analysis are decoupled through lightweight job records.

## Task 1: Dependencies and Schema

**Files:**
- Modify: `/Users/xinmi/www/sale-agent/pyproject.toml`
- Modify: `/Users/xinmi/www/sale-agent/sale_agent/postgres_schema.sql`
- Test: `/Users/xinmi/www/sale-agent/tests/test_weflow_live_repository.py`

- [ ] **Step 1: Add dependency and script**

Add `cryptography>=42` to dependencies and script:

```toml
dependencies = ["psycopg[binary]>=3.2", "cryptography>=42"]

[project.scripts]
sale-agent = "sale_agent.cli:main"
sale-agent-api = "sale_agent.api:main"
sale-agent-dev-api = "sale_agent.dev_api:main"
sale-agent-weflow-live = "sale_agent.weflow_live_ingest:main"
```

- [ ] **Step 2: Add schema**

Append Postgres tables:

```sql
ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;
ALTER TABLE chat_messages ADD COLUMN IF NOT EXISTS revoked_raw_json JSONB;

CREATE TABLE IF NOT EXISTS weflow_sources (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    base_url TEXT NOT NULL,
    access_token_encrypted TEXT,
    access_token_hint TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    seller_wechat_id TEXT,
    seller_display_name TEXT,
    include_sessions JSONB NOT NULL DEFAULT '[]'::jsonb,
    exclude_sessions JSONB NOT NULL DEFAULT '[]'::jsonb,
    include_session_types JSONB NOT NULL DEFAULT '["private","group"]'::jsonb,
    initial_backfill_mode TEXT NOT NULL DEFAULT 'recent',
    initial_backfill_days INTEGER NOT NULL DEFAULT 365,
    last_test_status TEXT,
    last_test_error TEXT,
    last_test_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS weflow_contacts (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES weflow_sources(id) ON DELETE CASCADE,
    seller_wechat_id TEXT NOT NULL,
    contact_wechat_id TEXT NOT NULL,
    contact_alias TEXT,
    display_name TEXT,
    remark TEXT,
    nickname TEXT,
    avatar_url TEXT,
    contact_type TEXT,
    raw_contact_json JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_id, seller_wechat_id, contact_wechat_id)
);

CREATE TABLE IF NOT EXISTS weflow_contact_alias_history (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES weflow_sources(id) ON DELETE CASCADE,
    seller_wechat_id TEXT NOT NULL,
    contact_wechat_id TEXT NOT NULL,
    old_alias TEXT,
    new_alias TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_contact_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS weflow_sessions (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES weflow_sources(id) ON DELETE CASCADE,
    seller_wechat_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    conversation_key TEXT NOT NULL UNIQUE,
    session_type TEXT NOT NULL,
    display_name TEXT,
    last_timestamp BIGINT,
    unread_count INTEGER,
    raw_session_json JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_id, seller_wechat_id, session_id)
);

CREATE TABLE IF NOT EXISTS weflow_group_members (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES weflow_sources(id) ON DELETE CASCADE,
    seller_wechat_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    member_wechat_id TEXT NOT NULL,
    display_name TEXT,
    nickname TEXT,
    remark TEXT,
    alias TEXT,
    group_nickname TEXT,
    avatar_url TEXT,
    is_owner BOOLEAN,
    is_friend BOOLEAN,
    message_count INTEGER,
    raw_member_json JSONB NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_id, seller_wechat_id, session_id, member_wechat_id)
);

CREATE TABLE IF NOT EXISTS weflow_message_sync (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES weflow_sources(id) ON DELETE CASCADE,
    seller_wechat_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    conversation_key TEXT NOT NULL,
    rawid TEXT NOT NULL,
    event_type TEXT NOT NULL,
    chat_message_id BIGINT REFERENCES chat_messages(id) ON DELETE SET NULL,
    event_timestamp BIGINT,
    raw_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_id, seller_wechat_id, session_id, rawid, event_type)
);

CREATE TABLE IF NOT EXISTS weflow_sync_state (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES weflow_sources(id) ON DELETE CASCADE,
    seller_wechat_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    last_synced_at BIGINT,
    last_event_id TEXT,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_id, seller_wechat_id, session_id)
);

CREATE TABLE IF NOT EXISTS analysis_jobs (
    id BIGSERIAL PRIMARY KEY,
    conversation_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(conversation_key, reason, status)
);
```

- [ ] **Step 3: Run schema smoke test**

Run:

```bash
cd /Users/xinmi/www/sale-agent
python3 -m pytest tests/test_runtime_db.py -v
```

Expected: existing runtime DB tests still pass.

## Task 2: Token Encryption

**Files:**
- Create: `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/crypto.py`
- Test: `/Users/xinmi/www/sale-agent/tests/test_weflow_live_crypto.py`

- [ ] **Step 1: Write crypto tests**

```python
import os

from sale_agent.weflow_live.crypto import decrypt_secret, encrypt_secret, secret_hint


def test_encrypt_secret_roundtrip(monkeypatch):
    monkeypatch.setenv("SALE_AGENT_SECRET_KEY", "x" * 32)
    encrypted = encrypt_secret("token-1234")
    assert encrypted != "token-1234"
    assert decrypt_secret(encrypted) == "token-1234"


def test_secret_hint_masks_value():
    assert secret_hint("abcdef1234") == "****1234"
    assert secret_hint("") == ""
```

- [ ] **Step 2: Implement crypto module**

Use Fernet with a stable key derived from `SALE_AGENT_SECRET_KEY`:

```python
from __future__ import annotations

import base64
import hashlib
import os

from cryptography.fernet import Fernet


def _fernet() -> Fernet:
    secret = os.environ.get("SALE_AGENT_SECRET_KEY", "")
    if len(secret) < 32:
        raise RuntimeError("SALE_AGENT_SECRET_KEY must be at least 32 characters")
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_secret(value: str | None) -> str | None:
    if not value:
        return None
    return _fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_secret(value: str | None) -> str | None:
    if not value:
        return None
    return _fernet().decrypt(value.encode("utf-8")).decode("utf-8")


def secret_hint(value: str | None) -> str:
    if not value:
        return ""
    return f"****{value[-4:]}"
```

- [ ] **Step 3: Run crypto tests**

Run:

```bash
cd /Users/xinmi/www/sale-agent
python3 -m pytest tests/test_weflow_live_crypto.py -v
```

Expected: both tests pass.

## Task 3: Management API

**Files:**
- Create: `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/repository.py`
- Modify: `/Users/xinmi/www/sale-agent/sale_agent/api_service.py`
- Modify: `/Users/xinmi/www/sale-agent/sale_agent/api.py`
- Test: `/Users/xinmi/www/sale-agent/tests/test_weflow_live_api.py`

- [ ] **Step 1: Add API tests**

Test create/list/update behavior and token masking through `api.handle_request`.

- [ ] **Step 2: Implement repository source CRUD**

Functions:

```python
list_sources(conn) -> list[dict]
create_source(conn, payload) -> dict
update_source(conn, source_id, payload) -> dict | None
get_source(conn, source_id) -> dict | None
enabled_sources(conn) -> list[dict]
```

Rules:

- Encrypt token on create/update.
- Never return encrypted token.
- Return `accessTokenConfigured` and `accessTokenHint`.
- Preserve existing token when PATCH omits `access_token`.

- [ ] **Step 3: Add API service wrappers**

Add:

```python
weflow_sources(conn)
create_weflow_source(conn, payload)
update_weflow_source(conn, source_id, payload)
test_weflow_source(conn, source_id)
sync_weflow_source_now(conn, source_id)
```

- [ ] **Step 4: Add HTTP routes**

Add routes:

```text
GET    /sales-api/v1/weflow-sources
POST   /sales-api/v1/weflow-sources
PATCH  /sales-api/v1/weflow-sources/{sourceId}
POST   /sales-api/v1/weflow-sources/{sourceId}/test
POST   /sales-api/v1/weflow-sources/{sourceId}/sync-now
```

- [ ] **Step 5: Run API tests**

Run:

```bash
cd /Users/xinmi/www/sale-agent
python3 -m pytest tests/test_weflow_live_api.py -v
```

Expected: management API passes.

## Task 4: WeFlow Client

**Files:**
- Create: `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/client.py`
- Create: `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/models.py`

- [ ] **Step 1: Implement HTTP client**

Client methods:

```python
health()
sessions(limit=10000)
contacts(limit=10000)
group_members(session_id)
messages(session_id, since=None, limit=5000, offset=0)
event_stream(last_event_id=None)
```

Use `Authorization: Bearer <token>` for normal HTTP and `?access_token=` for SSE.

- [ ] **Step 2: Normalize message IDs**

Rules:

```text
rawid = serverId if present else localId
conversation_key = weflow:{source_name}:{seller_wechat_id}:{session_id}
sent_at = createTime as Unix seconds string for current schema compatibility
sender_role = "我" when isSend true else "对方"
is_seller = isSend
message_type = mediaType if present else localType
content = parsedContent or content or rawContent
```

## Task 5: Sync Service and Worker

**Files:**
- Create: `/Users/xinmi/www/sale-agent/sale_agent/weflow_live/sync_service.py`
- Create: `/Users/xinmi/www/sale-agent/sale_agent/weflow_live_ingest.py`
- Modify: `/Users/xinmi/www/sale-agent/pyproject.toml`

- [ ] **Step 1: Implement source test**

Test flow:

```text
GET /health
GET /api/v1/sessions?format=chatlab&limit=1
If one session exists, GET /api/v1/sessions/{id}/messages?limit=1 and discover meta.ownerId
```

- [ ] **Step 2: Implement entity sync**

For each enabled source:

```text
discover seller_wechat_id if not configured
upsert contacts
upsert sessions
for group sessions: upsert group members
```

Record alias history when alias changes.

- [ ] **Step 3: Implement message backfill**

For each included session:

```text
since = now - 365 days for first run
since = last_synced_at - 300 seconds for later runs
page through /api/v1/sessions/{id}/messages
upsert chat_messages
upsert weflow_message_sync
update weflow_sync_state
enqueue analysis_jobs for message.new
```

- [ ] **Step 4: Implement SSE loop**

For each enabled source:

```text
open /api/v1/push/messages
on message.new: upsert message and enqueue analysis
on message.revoke: insert sync event and mark chat_messages revoked
on disconnect: sleep reconnect_seconds, run overlap backfill, reconnect
```

- [ ] **Step 5: Add CLI**

CLI:

```bash
python3 -m sale_agent.weflow_live_ingest --once
python3 -m sale_agent.weflow_live_ingest
```

`--once` runs entity sync + backfill and exits. Default runs continuous SSE.

## Task 6: Verification

**Files:**
- Existing test suite.

- [ ] **Step 1: Run focused tests**

```bash
cd /Users/xinmi/www/sale-agent
python3 -m pytest tests/test_weflow_live_crypto.py tests/test_weflow_live_api.py tests/test_weflow_live_repository.py -v
```

- [ ] **Step 2: Run full suite**

```bash
cd /Users/xinmi/www/sale-agent
python3 -m pytest -v
```

- [ ] **Step 3: Manual integration smoke**

With WeFlow API enabled:

```bash
export SALE_AGENT_SECRET_KEY="replace-with-at-least-32-random-chars"
python3 -m sale_agent.api --host 127.0.0.1 --port 8765
curl -X POST http://127.0.0.1:8765/sales-api/v1/weflow-sources \
  -H 'Content-Type: application/json' \
  -d '{"name":"shop-pc-1","baseUrl":"http://WEFLOW_HOST:5031","accessToken":"TOKEN","enabled":true}'
python3 -m sale_agent.weflow_live_ingest --once
```

Expected:

- `weflow_sources` has one configured source.
- `weflow_contacts` and `weflow_sessions` contain rows.
- `chat_messages` contains backfilled rows.
- `weflow_message_sync` contains dedupe records.

## Self-Review

- Spec coverage: Covers multi-source, admin config, encrypted tokens, contacts/sessions/group members first, 365-day backfill, SSE, reconnect compensation, alias history, revoke handling, async analysis job enqueueing, and no cross-sales merge.
- Placeholder scan: No implementation placeholders remain in task objectives; low-level function bodies will be written during execution.
- Type consistency: Uses `source_id`, `seller_wechat_id`, `session_id`, `conversation_key`, `rawid`, and `event_type` consistently across schema and services.
