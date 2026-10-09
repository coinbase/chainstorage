-- +goose Up
-- INF-1618: verify pending DDL through the privileged deployment/admin path.
-- Keep this table empty in deployed environments. No application uses it.
-- Cleanup must be a separate forward migration after dev and prod verification.
CREATE TABLE public.inf1133_migration_canary (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.inf1133_migration_canary IS
    'INF-1618 privileged migration canary; created by Goose version 20260908000001';

-- +goose Down
DROP TABLE public.inf1133_migration_canary;
