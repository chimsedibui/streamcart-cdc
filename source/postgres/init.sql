-- Operational database of a multi-seller marketplace (Shopee / TikTok Shop style).
-- No business data is seeded here: catalog, shops and customers are created through the
-- Source API by the simulator, so every row has a matching domain event in the outbox.
-- Money is BIGINT in VND (prices include VAT).

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SCHEMA IF NOT EXISTS ecommerce;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_user') THEN
        CREATE ROLE cdc_user WITH LOGIN REPLICATION;   -- password: passwords.sh
    END IF;
END
$$;

-- ---------------------------------------------------------------- business clock
-- Business time runs at `speed` × wall-clock time from an anchor, so a simulated day can pass in minutes.
-- Every business timestamp, deadline and time-based rule uses ecommerce.business_now(); the commit time
-- Debezium records (source.ts_ms) stays wall-clock, so pipeline lag is still measured in real seconds.
CREATE TABLE ecommerce.sim_clock (
    id          BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),     -- single row
    anchor_real TIMESTAMPTZ      NOT NULL,
    anchor_sim  TIMESTAMPTZ      NOT NULL,
    speed       DOUBLE PRECISION NOT NULL CHECK (speed > 0 AND speed <= 100000)
);
INSERT INTO ecommerce.sim_clock VALUES (TRUE, clock_timestamp(), clock_timestamp(), 1);

CREATE OR REPLACE FUNCTION ecommerce.business_now()
RETURNS TIMESTAMPTZ AS $$
    SELECT anchor_sim + (clock_timestamp() - anchor_real) * speed FROM ecommerce.sim_clock
$$ LANGUAGE sql VOLATILE;

CREATE OR REPLACE FUNCTION ecommerce.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = ecommerce.business_now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------------- customers
CREATE TABLE ecommerce.customers (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name          VARCHAR(80)  NOT NULL,
    email              VARCHAR(80)  NOT NULL UNIQUE,
    phone              VARCHAR(15)  NOT NULL,
    segment            VARCHAR(20)  NOT NULL DEFAULT 'REGULAR'
                       CHECK (segment IN ('NEW', 'REGULAR', 'BARGAIN_HUNTER', 'LOYAL', 'BUSINESS')),
    registered_channel VARCHAR(10)  NOT NULL DEFAULT 'APP' CHECK (registered_channel IN ('APP', 'WEB')),
    tier               VARCHAR(10)  NOT NULL DEFAULT 'BRONZE'
                       CHECK (tier IN ('BRONZE', 'SILVER', 'GOLD', 'PLATINUM')),
    total_spent        BIGINT       NOT NULL DEFAULT 0 CHECK (total_spent >= 0),
    is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);

CREATE TABLE ecommerce.customer_addresses (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id    UUID         NOT NULL REFERENCES ecommerce.customers(id),
    recipient_name VARCHAR(80)  NOT NULL,
    phone          VARCHAR(15)  NOT NULL,
    street         VARCHAR(120) NOT NULL,
    district       VARCHAR(60)  NOT NULL,
    province       VARCHAR(40)  NOT NULL,
    is_default     BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_customer_addresses_customer ON ecommerce.customer_addresses (customer_id);
CREATE UNIQUE INDEX uq_customer_default_address ON ecommerce.customer_addresses (customer_id) WHERE is_default;


-- ---------------------------------------------------------------- sellers & shops
CREATE TABLE ecommerce.sellers (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    legal_name  VARCHAR(120) NOT NULL,
    email       VARCHAR(80)  NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);

CREATE TABLE ecommerce.shops (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seller_id           UUID         NOT NULL REFERENCES ecommerce.sellers(id),
    name                VARCHAR(80)  NOT NULL UNIQUE,
    shop_tier           VARCHAR(10)  NOT NULL CHECK (shop_tier IN ('MALL', 'PREFERRED', 'NORMAL')),
    province            VARCHAR(40)  NOT NULL,          -- ship-from location
    commission_rate_bps INTEGER      NOT NULL CHECK (commission_rate_bps BETWEEN 0 AND 3000),
    penalty_points      INTEGER      NOT NULL DEFAULT 0 CHECK (penalty_points >= 0),
    is_active           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_shops_seller ON ecommerce.shops (seller_id);


-- ---------------------------------------------------------------- catalog
CREATE TABLE ecommerce.categories (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id   UUID         REFERENCES ecommerce.categories(id),
    name        VARCHAR(80)  NOT NULL,
    slug        VARCHAR(80)  NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);

CREATE TABLE ecommerce.brands (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(80)  NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);

CREATE TABLE ecommerce.products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shop_id     UUID         NOT NULL REFERENCES ecommerce.shops(id),
    category_id UUID         NOT NULL REFERENCES ecommerce.categories(id),
    brand_id    UUID         REFERENCES ecommerce.brands(id),
    name        VARCHAR(200) NOT NULL,
    description TEXT         NOT NULL DEFAULT '',
    status      VARCHAR(10)  NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE')),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_products_shop ON ecommerce.products (shop_id);
CREATE INDEX idx_products_category ON ecommerce.products (category_id);

CREATE TABLE ecommerce.product_skus (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id   UUID         NOT NULL REFERENCES ecommerce.products(id),
    sku_code     VARCHAR(60)  NOT NULL UNIQUE,
    variant_name VARCHAR(80)  NOT NULL,
    list_price   BIGINT       NOT NULL CHECK (list_price > 0),
    price        BIGINT       NOT NULL CHECK (price > 0 AND price <= list_price),
    weight_g     INTEGER      NOT NULL CHECK (weight_g > 0),
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_product_skus_product ON ecommerce.product_skus (product_id);

-- Append-only: every price change of a SKU (the listing price is the first row)
CREATE TABLE ecommerce.price_history (
    id          BIGSERIAL PRIMARY KEY,
    sku_id      UUID        NOT NULL REFERENCES ecommerce.product_skus(id),
    old_price   BIGINT,
    new_price   BIGINT      NOT NULL CHECK (new_price > 0),
    reason      VARCHAR(30) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_price_history_sku ON ecommerce.price_history (sku_id, created_at);

-- One stock row per SKU, held in the owning shop's warehouse
CREATE TABLE ecommerce.inventory (
    sku_id            UUID PRIMARY KEY REFERENCES ecommerce.product_skus(id),
    shop_id           UUID        NOT NULL REFERENCES ecommerce.shops(id),
    on_hand           INTEGER     NOT NULL CHECK (on_hand >= 0),
    reserved          INTEGER     NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    available         INTEGER     NOT NULL CHECK (available >= 0),
    reorder_point     INTEGER     NOT NULL DEFAULT 10 CHECK (reorder_point >= 0),
    last_restocked_at TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    CHECK (available = on_hand - reserved)
);
CREATE INDEX idx_inventory_shop ON ecommerce.inventory (shop_id);

-- Append-only stock ledger: every change of on_hand / reserved, with what caused it.
-- inventory = Σ deltas per SKU; *_after give the running balance.
CREATE TABLE ecommerce.inventory_movements (
    id             BIGSERIAL PRIMARY KEY,
    sku_id         UUID        NOT NULL REFERENCES ecommerce.product_skus(id),
    movement_type  VARCHAR(16) NOT NULL CHECK (movement_type IN ('LISTED', 'RESTOCK', 'RESERVE', 'RELEASE', 'SHIP',
                                                                 'RETURN_TO_SENDER', 'CUSTOMER_RETURN')),
    on_hand_delta  INTEGER     NOT NULL,
    reserved_delta INTEGER     NOT NULL,
    on_hand_after  INTEGER     NOT NULL,
    reserved_after INTEGER     NOT NULL,
    ref_type       VARCHAR(10) NOT NULL CHECK (ref_type IN ('LISTING', 'RESTOCK', 'ORDER', 'RETURN')),
    ref_id         UUID        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_inventory_movements_sku ON ecommerce.inventory_movements (sku_id, id);


-- ---------------------------------------------------------------- cart
CREATE TABLE ecommerce.carts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID        NOT NULL REFERENCES ecommerce.customers(id),
    status      VARCHAR(12) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'CHECKED_OUT', 'ABANDONED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE UNIQUE INDEX uq_carts_one_active ON ecommerce.carts (customer_id) WHERE status = 'ACTIVE';

CREATE TABLE ecommerce.cart_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cart_id     UUID        NOT NULL REFERENCES ecommerce.carts(id),
    sku_id      UUID        NOT NULL REFERENCES ecommerce.product_skus(id),
    quantity    INTEGER     NOT NULL CHECK (quantity BETWEEN 1 AND 100),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    UNIQUE (cart_id, sku_id)
);


-- ---------------------------------------------------------------- promotions
CREATE TABLE ecommerce.vouchers (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code               VARCHAR(30) NOT NULL UNIQUE,
    scope              VARCHAR(10) NOT NULL CHECK (scope IN ('PLATFORM', 'SHOP')),
    shop_id            UUID        REFERENCES ecommerce.shops(id),
    kind               VARCHAR(10) NOT NULL CHECK (kind IN ('FIXED', 'PERCENT', 'FREESHIP')),
    value              BIGINT      NOT NULL CHECK (value > 0),        -- VND, or basis points for PERCENT
    max_discount       BIGINT,                                         -- cap for PERCENT / FREESHIP
    min_spend          BIGINT      NOT NULL DEFAULT 0,
    usage_limit        INTEGER     NOT NULL CHECK (usage_limit > 0),
    per_customer_limit INTEGER     NOT NULL DEFAULT 1,
    starts_at          TIMESTAMPTZ NOT NULL,
    ends_at            TIMESTAMPTZ NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    CHECK ((scope = 'SHOP') = (shop_id IS NOT NULL)),
    CHECK (kind <> 'FREESHIP' OR scope = 'PLATFORM'),
    CHECK (ends_at > starts_at)
);

-- Redemption quota of a voucher, split into slots so concurrent checkouts don't all queue on one row
-- (a platform voucher in the first minute of 11.11). Σ capacity = usage_limit; Σ used = applied usages.
-- Not streamed: voucher_usages carries the same information.
CREATE TABLE ecommerce.voucher_quota_slots (
    voucher_id UUID     NOT NULL REFERENCES ecommerce.vouchers(id),
    slot       SMALLINT NOT NULL,
    capacity   INTEGER  NOT NULL CHECK (capacity > 0),
    used       INTEGER  NOT NULL DEFAULT 0,
    PRIMARY KEY (voucher_id, slot),
    CHECK (used BETWEEN 0 AND capacity)
);

CREATE TABLE ecommerce.campaigns (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(80) NOT NULL,
    kind        VARCHAR(12) NOT NULL CHECK (kind IN ('FLASH_SALE', 'MEGA_SALE')),
    starts_at   TIMESTAMPTZ NOT NULL,
    ends_at     TIMESTAMPTZ NOT NULL CHECK (ends_at > starts_at),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);

CREATE TABLE ecommerce.flash_sale_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id UUID        NOT NULL REFERENCES ecommerce.campaigns(id),
    sku_id      UUID        NOT NULL REFERENCES ecommerce.product_skus(id),
    sale_price  BIGINT      NOT NULL CHECK (sale_price > 0),
    quota       INTEGER     NOT NULL CHECK (quota > 0),
    sold        INTEGER     NOT NULL DEFAULT 0 CHECK (sold >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    UNIQUE (campaign_id, sku_id),
    CHECK (sold <= quota)
);


-- ---------------------------------------------------------------- checkout, orders, payments
CREATE TABLE ecommerce.checkouts (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id      UUID        NOT NULL REFERENCES ecommerce.customers(id),
    cart_id          UUID        NOT NULL UNIQUE REFERENCES ecommerce.carts(id),
    address_id       UUID        NOT NULL REFERENCES ecommerce.customer_addresses(id),
    payment_method   VARCHAR(15) NOT NULL CHECK (payment_method IN ('COD', 'WALLET', 'CARD', 'BANK_TRANSFER')),
    status           VARCHAR(16) NOT NULL CHECK (status IN ('PENDING_PAYMENT', 'PAID', 'COD_CONFIRMED', 'EXPIRED', 'CANCELLED')),
    items_total      BIGINT      NOT NULL CHECK (items_total > 0),
    shipping_total   BIGINT      NOT NULL CHECK (shipping_total >= 0),
    discount_total   BIGINT      NOT NULL DEFAULT 0 CHECK (discount_total >= 0),
    grand_total      BIGINT      NOT NULL CHECK (grand_total > 0),
    pay_by           TIMESTAMPTZ,                       -- online payment deadline
    created_at       TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_checkouts_status ON ecommerce.checkouts (status, pay_by);

CREATE SEQUENCE ecommerce.order_number_seq START WITH 1;

CREATE TABLE ecommerce.orders (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number       VARCHAR(24) NOT NULL UNIQUE,
    checkout_id        UUID        NOT NULL REFERENCES ecommerce.checkouts(id),
    customer_id        UUID        NOT NULL REFERENCES ecommerce.customers(id),
    shop_id            UUID        NOT NULL REFERENCES ecommerce.shops(id),
    status             VARCHAR(20) NOT NULL CHECK (status IN (
                           'UNPAID', 'TO_SHIP', 'READY_TO_SHIP', 'SHIPPED', 'DELIVERY_FAILED', 'DELIVERED',
                           'COMPLETED', 'CANCELLED', 'RETURNED_TO_SELLER', 'RETURN_REFUND')),
    payment_method     VARCHAR(15) NOT NULL,
    subtotal           BIGINT      NOT NULL CHECK (subtotal > 0),
    shop_discount      BIGINT      NOT NULL DEFAULT 0 CHECK (shop_discount >= 0),
    platform_discount  BIGINT      NOT NULL DEFAULT 0 CHECK (platform_discount >= 0),
    shipping_fee       BIGINT      NOT NULL CHECK (shipping_fee >= 0),
    shipping_subsidy   BIGINT      NOT NULL DEFAULT 0 CHECK (shipping_subsidy >= 0),
    total              BIGINT      NOT NULL CHECK (total > 0),
    ship_to_name       VARCHAR(80) NOT NULL,
    ship_to_address    VARCHAR(250) NOT NULL,
    ship_to_province   VARCHAR(40) NOT NULL,
    delivery_attempts  INTEGER     NOT NULL DEFAULT 0,
    confirm_by         TIMESTAMPTZ,                       -- seller SLA deadline
    cancel_reason      VARCHAR(40),
    cancelled_by       VARCHAR(10) CHECK (cancelled_by IN ('BUYER', 'SELLER', 'SYSTEM', 'LOGISTICS')),
    paid_at            TIMESTAMPTZ,
    ready_at           TIMESTAMPTZ,
    shipped_at         TIMESTAMPTZ,
    delivered_at       TIMESTAMPTZ,
    completed_at       TIMESTAMPTZ,
    cancelled_at       TIMESTAMPTZ,
    version            INTEGER     NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    CHECK (total = subtotal - shop_discount - platform_discount + shipping_fee - shipping_subsidy)
);
CREATE INDEX idx_orders_status ON ecommerce.orders (status);
CREATE INDEX idx_orders_checkout ON ecommerce.orders (checkout_id);
CREATE INDEX idx_orders_customer ON ecommerce.orders (customer_id);
CREATE INDEX idx_orders_shop_status ON ecommerce.orders (shop_id, status);

CREATE TABLE ecommerce.order_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID        NOT NULL REFERENCES ecommerce.orders(id),
    sku_id      UUID        NOT NULL REFERENCES ecommerce.product_skus(id),
    product_id  UUID        NOT NULL REFERENCES ecommerce.products(id),
    quantity    INTEGER     NOT NULL CHECK (quantity > 0),
    list_price  BIGINT      NOT NULL CHECK (list_price > 0),
    unit_price  BIGINT      NOT NULL CHECK (unit_price > 0),   -- price at checkout time (flash price if any)
    line_total  BIGINT      NOT NULL CHECK (line_total = quantity * unit_price),
    shop_discount     BIGINT NOT NULL DEFAULT 0 CHECK (shop_discount >= 0),      -- share of the order's vouchers
    platform_discount BIGINT NOT NULL DEFAULT 0 CHECK (platform_discount >= 0),
    net_amount        BIGINT NOT NULL,                                            -- what the buyer pays for the line
    campaign_item_id  UUID   REFERENCES ecommerce.flash_sale_items(id),
    CHECK (net_amount = line_total - shop_discount - platform_discount AND net_amount >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_order_items_order ON ecommerce.order_items (order_id);
CREATE INDEX idx_order_items_sku ON ecommerce.order_items (sku_id);

CREATE TABLE ecommerce.voucher_usages (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    voucher_id  UUID        NOT NULL REFERENCES ecommerce.vouchers(id),
    customer_id UUID        NOT NULL REFERENCES ecommerce.customers(id),
    checkout_id UUID        NOT NULL REFERENCES ecommerce.checkouts(id),
    order_id    UUID        REFERENCES ecommerce.orders(id),      -- set for shop vouchers
    amount      BIGINT      NOT NULL CHECK (amount > 0),
    quota_slot  SMALLINT    NOT NULL,                              -- voucher_quota_slots.slot it took
    status      VARCHAR(10) NOT NULL DEFAULT 'APPLIED' CHECK (status IN ('APPLIED', 'RELEASED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    UNIQUE (voucher_id, checkout_id)
);
CREATE INDEX idx_voucher_usages_customer ON ecommerce.voucher_usages (voucher_id, customer_id);

-- Append-only: one row per status transition
CREATE TABLE ecommerce.order_status_history (
    id          BIGSERIAL PRIMARY KEY,
    order_id    UUID        NOT NULL REFERENCES ecommerce.orders(id),
    from_status VARCHAR(20),
    to_status   VARCHAR(20) NOT NULL,
    actor       VARCHAR(10) NOT NULL CHECK (actor IN ('BUYER', 'SELLER', 'SYSTEM', 'PAYMENT', 'LOGISTICS')),
    reason      VARCHAR(40),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_order_status_history_order ON ecommerce.order_status_history (order_id, id);

-- Online: one row per payment attempt of a checkout. COD: one row per order, collected on delivery.
CREATE TABLE ecommerce.payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    checkout_id     UUID        NOT NULL REFERENCES ecommerce.checkouts(id),
    order_id        UUID        REFERENCES ecommerce.orders(id),
    method          VARCHAR(15) NOT NULL,
    provider        VARCHAR(20) NOT NULL,
    transaction_ref VARCHAR(40) NOT NULL UNIQUE,
    amount          BIGINT      NOT NULL CHECK (amount > 0),
    status          VARCHAR(10) NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'EXPIRED', 'CANCELLED')),
    failure_reason  VARCHAR(40),
    paid_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_payments_checkout ON ecommerce.payments (checkout_id);
CREATE INDEX idx_payments_order ON ecommerce.payments (order_id);

CREATE TABLE ecommerce.refunds (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id  UUID        NOT NULL REFERENCES ecommerce.payments(id),
    order_id    UUID        REFERENCES ecommerce.orders(id),   -- NULL for a late payment on an expired checkout
    return_id   UUID,                                          -- set when the refund settles a return
    amount      BIGINT      NOT NULL CHECK (amount > 0),
    reason      VARCHAR(40) NOT NULL,
    status      VARCHAR(10) NOT NULL DEFAULT 'SUCCEEDED' CHECK (status IN ('PENDING', 'SUCCEEDED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_refunds_payment ON ecommerce.refunds (payment_id);


-- ---------------------------------------------------------------- logistics
-- FORWARD parcels go seller → buyer; RETURN parcels carry a customer return back to the seller.
CREATE TABLE ecommerce.shipments (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id         UUID        NOT NULL REFERENCES ecommerce.orders(id),
    return_id        UUID,                                       -- set for RETURN parcels
    direction        VARCHAR(8)  NOT NULL CHECK (direction IN ('FORWARD', 'RETURN')),
    carrier          VARCHAR(12) NOT NULL CHECK (carrier IN ('GHN', 'GHTK', 'J&T', 'SPX', 'VTP')),
    tracking_number  VARCHAR(30) NOT NULL UNIQUE,
    status           VARCHAR(18) NOT NULL CHECK (status IN ('CREATED', 'PICKED_UP', 'IN_TRANSIT', 'OUT_FOR_DELIVERY',
                                                            'DELIVERED', 'DELIVERY_FAILED', 'RETURNED_TO_SENDER')),
    from_province    VARCHAR(40) NOT NULL,
    to_province      VARCHAR(40) NOT NULL,
    attempts         INTEGER     NOT NULL DEFAULT 0,
    last_event_time  TIMESTAMPTZ,                                -- carrier time of the latest applied event
    picked_up_at     TIMESTAMPTZ,
    delivered_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    UNIQUE (order_id, direction)
);
CREATE INDEX idx_shipments_status ON ecommerce.shipments (direction, status);

-- Append-only carrier tracking. Carriers redeliver and reorder callbacks: an event older than the parcel's
-- latest applied event is stored with applied = FALSE and does not move the parcel back.
CREATE TABLE ecommerce.shipment_events (
    id               BIGSERIAL PRIMARY KEY,
    shipment_id      UUID        NOT NULL REFERENCES ecommerce.shipments(id),
    carrier_event_id VARCHAR(60) NOT NULL UNIQUE,
    event_type       VARCHAR(18) NOT NULL,
    event_time       TIMESTAMPTZ NOT NULL,                       -- when it happened, per the carrier
    location         VARCHAR(40),
    applied          BOOLEAN     NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()   -- when we received it
);
CREATE INDEX idx_shipment_events_shipment ON ecommerce.shipment_events (shipment_id, event_time);


-- ---------------------------------------------------------------- returns
CREATE TABLE ecommerce.returns (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id         UUID        NOT NULL UNIQUE REFERENCES ecommerce.orders(id),   -- one return per order
    customer_id      UUID        NOT NULL REFERENCES ecommerce.customers(id),
    shop_id          UUID        NOT NULL REFERENCES ecommerce.shops(id),
    reason           VARCHAR(20) NOT NULL CHECK (reason IN ('DAMAGED', 'WRONG_ITEM', 'NOT_AS_DESCRIBED', 'MISSING_PARTS', 'CHANGED_MIND')),
    status           VARCHAR(10) NOT NULL CHECK (status IN ('REQUESTED', 'APPROVED', 'REJECTED', 'DISPUTED', 'RETURNING',
                                                            'RECEIVED', 'REFUNDED', 'CLOSED')),
    refund_amount    BIGINT      NOT NULL DEFAULT 0 CHECK (refund_amount >= 0),
    respond_by       TIMESTAMPTZ NOT NULL,                      -- seller must answer by then (else auto-approve)
    resolved_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_returns_status ON ecommerce.returns (status);
ALTER TABLE ecommerce.refunds ADD CONSTRAINT fk_refunds_return FOREIGN KEY (return_id) REFERENCES ecommerce.returns(id);
ALTER TABLE ecommerce.shipments ADD CONSTRAINT fk_shipments_return FOREIGN KEY (return_id) REFERENCES ecommerce.returns(id);

CREATE TABLE ecommerce.return_items (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    return_id      UUID        NOT NULL REFERENCES ecommerce.returns(id),
    order_item_id  UUID        NOT NULL UNIQUE REFERENCES ecommerce.order_items(id),
    quantity       INTEGER     NOT NULL CHECK (quantity > 0),
    refund_amount  BIGINT      NOT NULL CHECK (refund_amount >= 0),
    condition      VARCHAR(10) CHECK (condition IN ('SELLABLE', 'DAMAGED')),   -- set when the seller receives it
    created_at     TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);


-- ---------------------------------------------------------------- seller settlement
-- Append-only ledger of the seller wallet: SALE (+), COMMISSION / PAYMENT_FEE (−) when an order completes,
-- PAYOUT (−) when the balance is paid out. Entries are only inserted; shop_wallets.balance catches up in batches
-- (wallet_pending), so completing orders never queues on the shop's row.
CREATE TABLE ecommerce.seller_payouts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shop_id     UUID        NOT NULL REFERENCES ecommerce.shops(id),
    amount      BIGINT      NOT NULL CHECK (amount > 0),
    status      VARCHAR(10) NOT NULL DEFAULT 'PAID' CHECK (status IN ('PAID')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);

CREATE TABLE ecommerce.seller_ledger_entries (
    id            BIGSERIAL PRIMARY KEY,
    shop_id       UUID        NOT NULL REFERENCES ecommerce.shops(id),
    order_id      UUID        REFERENCES ecommerce.orders(id),
    payout_id     UUID        REFERENCES ecommerce.seller_payouts(id),
    entry_type    VARCHAR(12) NOT NULL CHECK (entry_type IN ('SALE', 'COMMISSION', 'PAYMENT_FEE', 'PAYOUT')),
    amount        BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_seller_ledger_shop ON ecommerce.seller_ledger_entries (shop_id, id);

-- Wallet balance = Σ ledger entries already rolled up; Σ wallet_pending = entries not rolled up yet
CREATE TABLE ecommerce.shop_wallets (
    shop_id    UUID PRIMARY KEY REFERENCES ecommerce.shops(id),
    balance    BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);

-- Ledger entries waiting to be added to shop_wallets (not streamed)
CREATE TABLE ecommerce.wallet_pending (
    entry_id BIGINT PRIMARY KEY REFERENCES ecommerce.seller_ledger_entries(id),
    shop_id  UUID   NOT NULL,
    amount   BIGINT NOT NULL
);


-- ---------------------------------------------------------------- reviews
CREATE TABLE ecommerce.product_reviews (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_item_id UUID        NOT NULL UNIQUE REFERENCES ecommerce.order_items(id),
    order_id      UUID        NOT NULL REFERENCES ecommerce.orders(id),
    product_id    UUID        NOT NULL REFERENCES ecommerce.products(id),
    shop_id       UUID        NOT NULL REFERENCES ecommerce.shops(id),
    customer_id   UUID        NOT NULL REFERENCES ecommerce.customers(id),
    rating        SMALLINT    NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment       VARCHAR(300),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT ecommerce.business_now()
);
CREATE INDEX idx_product_reviews_product ON ecommerce.product_reviews (product_id);


-- ---------------------------------------------------------------- updated_at triggers
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers', 'customer_addresses', 'sellers', 'shops', 'categories', 'brands',
                             'products', 'product_skus', 'inventory', 'carts', 'cart_items', 'checkouts',
                             'orders', 'order_items', 'payments', 'refunds', 'vouchers', 'campaigns',
                             'flash_sale_items', 'voucher_usages', 'returns', 'return_items', 'seller_payouts',
                             'product_reviews', 'shipments', 'shop_wallets']
    LOOP
        EXECUTE format('CREATE TRIGGER trg_%s_touch BEFORE UPDATE ON ecommerce.%I
                        FOR EACH ROW EXECUTE FUNCTION ecommerce.touch_updated_at()', t, t);
    END LOOP;
END
$$;


-- ---------------------------------------------------------------- integration / pipeline tables
-- OUTBOX: domain events written in the same transaction as the business change.
-- Each row is deleted again before commit: Debezium reads the insert from the WAL and its Outbox Event Router
-- reroutes it to topic events.<aggregatetype>, so the table stays empty and never needs cleaning.
CREATE TABLE ecommerce.outbox_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype VARCHAR(50)  NOT NULL,
    aggregateid   VARCHAR(64)  NOT NULL,
    type          VARCHAR(80)  NOT NULL,
    payload       JSONB        NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT ecommerce.business_now()
);

-- IDEMPOTENCY: stored responses of commands sent with an Idempotency-Key header (not captured by CDC)
CREATE TABLE ecommerce.idempotency_keys (
    key          VARCHAR(100) PRIMARY KEY,
    endpoint     VARCHAR(200) NOT NULL,
    request_hash VARCHAR(64)  NOT NULL,
    status_code  INTEGER,
    response     JSONB,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP      -- wall clock: client retries are real-time
);
CREATE INDEX idx_idempotency_keys_created ON ecommerce.idempotency_keys (created_at);

-- Debezium heartbeat (keeps the replication slot moving) and signal table
CREATE TABLE ecommerce.cdc_heartbeat (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO ecommerce.cdc_heartbeat (id, ts) VALUES (1, NOW());

CREATE TABLE ecommerce.debezium_signals (
    id   VARCHAR(64) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    data TEXT
);

-- Every table in the schema; Debezium's table.include.list decides what is actually streamed
CREATE PUBLICATION cdc_publication FOR TABLES IN SCHEMA ecommerce;

-- REPLICA IDENTITY is set here, by the table owner: Debezium connects as cdc_user, which may not ALTER tables.
-- State tables: FULL, the whole old row in `before` (silver deletes and consistent.* need it).
-- Append-only tables and the outbox (deleted right after insert): DEFAULT, only the key, less WAL.
DO $$
DECLARE t TEXT;
BEGIN
    FOR t IN SELECT tablename FROM pg_tables WHERE schemaname = 'ecommerce'
             AND tablename NOT IN ('order_status_history', 'inventory_movements', 'shipment_events', 'price_history',
                                   'seller_ledger_entries', 'outbox_events')
    LOOP
        EXECUTE format('ALTER TABLE ecommerce.%I REPLICA IDENTITY FULL', t);
    END LOOP;
END
$$;

-- cdc_user: what Debezium needs and nothing more. REPLICATION (role attribute) for the slot, SELECT for snapshots,
-- writes only to its own heartbeat and signal tables. The publication is created above by the owner.
GRANT USAGE ON SCHEMA ecommerce TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT SELECT ON TABLES TO cdc_user;   -- tables added later
GRANT INSERT, UPDATE ON ecommerce.cdc_heartbeat TO cdc_user;                       -- heartbeat.action.query
GRANT INSERT ON ecommerce.debezium_signals TO cdc_user;                            -- incremental snapshot windows

CREATE OR REPLACE VIEW ecommerce.cdc_replication_status AS
SELECT
    slot_name,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_size
FROM pg_replication_slots
WHERE slot_name LIKE 'debezium%';
