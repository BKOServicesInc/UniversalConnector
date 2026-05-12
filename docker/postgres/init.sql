-- PostgreSQL initialisation for UniversalConnector
-- Runs once when the container is first created (docker-entrypoint-initdb.d).
--
-- What this does:
--   1. Grants the connector user REPLICATION privilege (required for logical replication / CDC).
--   2. Creates the orders schema and seed tables.
--   3. Creates a logical replication publication covering those tables.
--   4. Creates a replication slot that the connector will consume.
--
-- The database and user are already created by the POSTGRES_DB / POSTGRES_USER
-- environment variables before this script runs.

-- ── 1. Replication privileges ─────────────────────────────────────────────────
-- The role must have LOGIN + REPLICATION to create and use a replication slot.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = current_user
    ) THEN
        RAISE NOTICE 'Role % not found — skipping privilege grant', current_user;
    ELSE
        EXECUTE format('ALTER ROLE %I REPLICATION LOGIN', current_user);
    END IF;
END;
$$;

-- ── 2. Schema and tables ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.orders (
    id           BIGSERIAL    PRIMARY KEY,
    customer_id  BIGINT       NOT NULL,
    status       VARCHAR(50)  NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(12,2),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.order_items (
    id           BIGSERIAL    PRIMARY KEY,
    order_id     BIGINT       NOT NULL REFERENCES public.orders(id) ON DELETE CASCADE,
    product_id   BIGINT       NOT NULL,
    quantity     INT          NOT NULL DEFAULT 1,
    unit_price   NUMERIC(10,2),
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Index for efficient polling/watermark queries
CREATE INDEX IF NOT EXISTS idx_orders_updated_at      ON public.orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at ON public.order_items(updated_at);

-- ── 3. Logical replication publication ───────────────────────────────────────
-- The connector reads from this publication via WAL streaming.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'uc_pub') THEN
        CREATE PUBLICATION uc_pub FOR TABLE public.orders, public.order_items;
        RAISE NOTICE 'Publication uc_pub created';
    ELSE
        RAISE NOTICE 'Publication uc_pub already exists — skipping';
    END IF;
END;
$$;

-- ── 4. Replication slot ───────────────────────────────────────────────────────
-- pgoutput is the built-in logical decoder used by the connector.
-- The slot persists WAL until the connector acknowledges it — keep the
-- connector running to avoid disk exhaustion.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_replication_slots WHERE slot_name = 'uc_slot'
    ) THEN
        PERFORM pg_create_logical_replication_slot('uc_slot', 'pgoutput');
        RAISE NOTICE 'Replication slot uc_slot created';
    ELSE
        RAISE NOTICE 'Replication slot uc_slot already exists — skipping';
    END IF;
END;
$$;

-- ── 5. Seed data (optional — useful for smoke-testing the pipeline) ───────────
INSERT INTO public.orders (customer_id, status, total_amount)
VALUES (1001, 'pending',   99.95),
       (1002, 'confirmed', 249.00),
       (1003, 'shipped',   14.50)
ON CONFLICT DO NOTHING;
