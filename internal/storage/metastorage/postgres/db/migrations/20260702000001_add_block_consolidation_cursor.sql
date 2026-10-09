-- +goose Up
CREATE TABLE IF NOT EXISTS block_consolidation_cursor (
    name TEXT NOT NULL,
    tag INTEGER NOT NULL,
    height BIGINT NOT NULL CHECK (height >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, tag)
);

-- +goose Down
DROP TABLE IF EXISTS block_consolidation_cursor;
