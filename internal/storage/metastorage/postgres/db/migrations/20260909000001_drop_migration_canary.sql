-- +goose Up
-- INF-1618: remove only the empty verification table and its owned identity
-- sequence. Unexpected dependencies must block cleanup rather than be dropped.
DROP TABLE public.inf1133_migration_canary;

-- +goose Down
-- Restore the empty verification schema; no application data belongs here.
CREATE TABLE public.inf1133_migration_canary (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.inf1133_migration_canary IS
    'INF-1618 privileged migration canary; created by Goose version 20260908000001';
