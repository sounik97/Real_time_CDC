-- ENABLE EXTENSIONS UUID-OSSP AND PGCRYPTO
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- SCHEMA ecommerce
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- TABLE customers
CREATE TABLE IF NOT EXISTS ecommerce.customers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(50),
    tier VARCHAR(50) DEFAULT 'bronze' CHECK (tier IN ('bronze', 'silver', 'gold', 'platinum')),
    total_spent NUMERIC(10, 2) DEFAULT 0.00,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customers_email ON ecommerce.customers(email);
CREATE INDEX idx_customers_tier ON ecommerce.customers(tier);

-- product
CREATE TABLE IF NOT EXISTS ecommerce.products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50),
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    cost NUMERIC(10, 2) NOT NULL CHECK (cost >= 0),
    weight NUMERIC(10, 2),
    is_available BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_products_sku ON ecommerce.products(sku);
CREATE INDEX idx_products_category ON ecommerce.products(category);

-- orders
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    customer_id UUID NOT NULL REFERENCES ecommerce.customers(id),
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded')),
    subtotal DECIMAL(10, 2) NOT NULL CHECK (subtotal >= 0),
    tax DECIMAL(10, 2) NOT NULL CHECK (tax >= 0),
    shipping DECIMAL(10, 2) NOT NULL CHECK (shipping >= 0),
    total DECIMAL(10, 2) NOT NULL CHECK (total >= 0),
    shipping_address TEXT NOT NULL,
    payment_method VARCHAR(50),
    notes text,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_orders_order_number ON ecommerce.orders(order_number);
CREATE INDEX idx_orders_customer_id ON ecommerce.orders(customer_id);
CREATE INDEX idx_orders_status ON ecommerce.orders(status);
CREATE INDEX idx_orders_created_at ON ecommerce.orders(created_at DESC);


-- order_items
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES ecommerce.orders(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES ecommerce.products(id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_price NUMERIC(10, 2) NOT NULL CHECK (total_price >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_order_items_order_id ON ecommerce.order_items(order_id);
CREATE INDEX idx_order_items_product_id ON ecommerce.order_items(product_id);

-- TABLE inventory
CREATE TABLE IF NOT EXISTS ecommerce.inventory (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id UUID NOT NULL REFERENCES ecommerce.products(id),
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    reserved INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    available INTEGER NOT NULL DEFAULT 0 CHECK (available >= 0),
    reorder_point INTEGER NOT NULL DEFAULT 0 CHECK (reorder_point >= 0),
    reorder_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reorder_quantity >= 0),
    warehouse_location VARCHAR(100),
    last_restocked_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_inventory_product_id ON ecommerce.inventory(product_id);

-- audit_log
CREATE TABLE IF NOT EXISTS ecommerce.audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(100) NOT NULL,
    record_id UUID NOT NULL,
    action VARCHAR(50) NOT NULL CHECK (action IN ('INSERT', 'UPDATE', 'DELETE')),
    old_data JSONB,
    new_data JSONB,
    changed_by VARCHAR(100) DEFAULT CURRENT_USER,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_log_table_name ON ecommerce.audit_log(table_name);
CREATE INDEX idx_audit_log_record_id ON ecommerce.audit_log(record_id);
CREATE INDEX idx_audit_log_changed_at ON ecommerce.audit_log(changed_at DESC);

-- FUNCTION update_timestamp
CREATE OR REPLACE FUNCTION ecommerce.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- TRIGGERS FOR UPDATING updated_at for all tables
CREATE TRIGGER trg_update_customers_timestamp
    BEFORE UPDATE ON ecommerce.customers
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

CREATE TRIGGER trg_update_products_timestamp
    BEFORE UPDATE ON ecommerce.products
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

CREATE TRIGGER trg_update_orders_timestamp
    BEFORE UPDATE ON ecommerce.orders
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

CREATE TRIGGER trg_update_inventory_timestamp
    BEFORE UPDATE ON ecommerce.inventory
    FOR EACH ROW EXECUTE FUNCTION ecommerce.update_timestamp();

-- FUNCTION: GENERATE ORDER NUMBER
CREATE OR REPLACE FUNCTION ecommerce.generate_order_number()
RETURNS TRIGGER AS $$
BEGIN
    NEW.order_number := 'ORD-' || TO_CHAR(NOW(), 'YYYYMMDD') || '-' || LPAD((NEXTVAL('ecommerce.order_seq'))::TEXT, 6, '0');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE SEQUENCE ecommerce.order_seq START 1;

-- TRIGGER TO SET ORDER NUMBER BEFORE INSERT
CREATE TRIGGER trg_generate_order_number
    BEFORE INSERT ON ecommerce.orders
    FOR EACH ROW 
        WHEN (NEW.order_number IS NULL  OR NEW.order_number = '')
        EXECUTE FUNCTION ecommerce.generate_order_number();


INSERT INTO ecommerce.products (sku, name, description, category, price, cost, weight) VALUES
    ('LAPTOP-PRO-15', 'ProBook Laptop 15"', 'High-performance laptop with 16GB RAM and 512GB SSD', 'Electronics', 1299.99, 899.00, 2.1),
    ('PHONE-ULTRA-X', 'UltraPhone X', 'Flagship smartphone with 256GB storage', 'Electronics', 999.99, 650.00, 0.2),
    ('HEADPHONE-BT', 'Wireless Pro Headphones', 'Noise-cancelling Bluetooth headphones', 'Electronics', 349.99, 180.00, 0.3),
    ('TABLET-AIR', 'AirTab 10"', '10-inch tablet with Retina display', 'Electronics', 599.99, 350.00, 0.5),
    ('WATCH-SMART', 'SmartWatch Pro', 'Fitness tracking smartwatch with GPS', 'Electronics', 299.99, 150.00, 0.1),
    ('KEYBOARD-MECH', 'Mechanical Gaming Keyboard', 'RGB backlit mechanical keyboard', 'Accessories', 149.99, 65.00, 1.2),
    ('MOUSE-GAMING', 'Precision Gaming Mouse', 'High-DPI gaming mouse with programmable buttons', 'Accessories', 79.99, 35.00, 0.15),
    ('MONITOR-4K', '4K Ultra Monitor 27"', '27-inch 4K IPS display with HDR', 'Electronics', 449.99, 280.00, 6.5),
    ('CHARGER-FAST', 'Fast Charge Hub', '100W USB-C charging station', 'Accessories', 69.99, 25.00, 0.3),
    ('CASE-LAPTOP', 'Premium Laptop Case', 'Water-resistant laptop carrying case', 'Accessories', 49.99, 15.00, 0.8);

INSERT INTO ecommerce.inventory (product_id, quantity, reserved, warehouse_location, reorder_point, reorder_quantity)
SELECT 
    id, 
    (RANDOM() * 200 + 50)::INTEGER,
    (RANDOM() * 10)::INTEGER,
    CASE (RANDOM() * 3)::INTEGER 
        WHEN 0 THEN 'WAREHOUSE-A'
        WHEN 1 THEN 'WAREHOUSE-B'
        ELSE 'WAREHOUSE-C'
    END,
    20,
    100
FROM ecommerce.products;

INSERT INTO ecommerce.customers (email, first_name, last_name, phone, tier, total_spent) VALUES
    ('john.doe@example.com', 'John', 'Doe', '+1-555-0101', 'gold', 5420.50),
    ('jane.smith@example.com', 'Jane', 'Smith', '+1-555-0102', 'platinum', 12350.00),
    ('bob.johnson@example.com', 'Bob', 'Johnson', '+1-555-0103', 'silver', 1250.75),
    ('alice.williams@example.com', 'Alice', 'Williams', '+1-555-0104', 'bronze', 450.00),
    ('charlie.brown@example.com', 'Charlie', 'Brown', '+1-555-0105', 'gold', 3200.25);


-- CDC HEARTBEAT
CREATE TABLE IF NOT EXISTS ecommerce.cdc_heartbeat (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO ecommerce.cdc_heartbeat (id, ts) VALUES (1, NOW());


CREATE TABLE ecommerce.debezium_signals (
    id SERIAL PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    data TEXT
);

CREATE TABLE ecommerce.cdc_metrics (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL check (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_count BIGINT NOT NULL default 1,
    measured_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cdc_metrics_table_operation ON ecommerce.cdc_metrics(table_name, operation);


CREATE PUBLICATION cdc_publication FOR TABLE 
    ecommerce.customers,
    ecommerce.products,
    ecommerce.orders,
    ecommerce.order_items,
    ecommerce.inventory,
    ecommerce.cdc_heartbeat;


GRANT USAGE ON SCHEMA ecommerce TO cdc_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce TO cdc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ecommerce TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce TO cdc_user;


ALTER USER cdc_user WITH REPLICATION;

CREATE OR REPLACE VIEW ecommerce.dashboard_metrics AS
SELECT 
    (SELECT COUNT(*) FROM ecommerce.customers WHERE is_active = true) as active_customers,
    (SELECT COUNT(*) FROM ecommerce.products WHERE is_available = true) as available_products,
    (SELECT COUNT(*) FROM ecommerce.orders WHERE created_at >= CURRENT_DATE) as orders_today,
    (SELECT COALESCE(SUM(total), 0) FROM ecommerce.orders WHERE created_at >= CURRENT_DATE) as revenue_today,
    (SELECT COUNT(*) FROM ecommerce.inventory WHERE available <= reorder_point) as low_stock_items;

GRANT SELECT ON ecommerce.dashboard_metrics TO cdc_user;

CREATE OR REPLACE VIEW ecommerce.cdc_replication_status AS
SELECT 
    slot_name,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag_size,
    EXTRACT(EPOCH FROM (now() - (pg_last_xact_replay_timestamp()))) as lag_seconds
FROM pg_replication_slots
WHERE slot_name LIKE 'debezium%';


CREATE OR REPLACE FUNCTION ecommerce.get_table_counts()
RETURNS TABLE (
    table_name TEXT,
    row_count BIGINT,
    last_updated TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'customers'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.customers
    UNION ALL
    SELECT 'products'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.products
    UNION ALL
    SELECT 'orders'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.orders
    UNION ALL
    SELECT 'order_items'::TEXT, COUNT(*)::BIGINT, MAX(created_at) FROM ecommerce.order_items
    UNION ALL
    SELECT 'inventory'::TEXT, COUNT(*)::BIGINT, MAX(updated_at) FROM ecommerce.inventory;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION ecommerce.track_cdc_event()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ecommerce.cdc_metrics (table_name, operation)
    VALUES (TG_TABLE_NAME, TG_OP);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Add tracking triggers (after triggers, won't affect CDC)
CREATE TRIGGER trg_track_customers_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.customers
    FOR EACH ROW EXECUTE FUNCTION ecommerce.track_cdc_event();
CREATE TRIGGER trg_track_products_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.products
    FOR EACH ROW EXECUTE FUNCTION ecommerce.track_cdc_event();
CREATE TRIGGER trg_track_orders_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.orders
    FOR EACH ROW EXECUTE FUNCTION ecommerce.track_cdc_event();
CREATE TRIGGER trg_track_inventory_cdc AFTER INSERT OR UPDATE OR DELETE ON ecommerce.inventory
    FOR EACH ROW EXECUTE FUNCTION ecommerce.track_cdc_event();


