-- Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Staging: raw API response storage
CREATE TABLE IF NOT EXISTS staging.stg_exchange_rates (
    id SERIAL PRIMARY KEY,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    rate DECIMAL(18,6) NOT NULL,
    rate_date DATE NOT NULL,
    raw_json JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_rate_per_day UNIQUE (base_currency, target_currency, rate_date)
);

-- Warehouse: dimension table for currencies
CREATE TABLE IF NOT EXISTS warehouse.dim_currencies (
    currency_id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL UNIQUE,
    currency_name VARCHAR(50) NOT NULL
);

-- Warehouse: date dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Warehouse: fact table for exchange rates
CREATE TABLE IF NOT EXISTS warehouse.fact_exchange_rates (
    id SERIAL PRIMARY KEY,
    currency_id INT NOT NULL REFERENCES warehouse.dim_currencies(currency_id),
    date_id DATE NOT NULL REFERENCES warehouse.dim_date(date_id),
    rate DECIMAL(18,6) NOT NULL,
    prev_day_rate DECIMAL(18,6),
    daily_change DECIMAL(18,6),
    daily_change_pct DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_currency_date UNIQUE (currency_id, date_id)
);

-- Seed dim_currencies with target currencies
INSERT INTO warehouse.dim_currencies (currency_code, currency_name) VALUES
    ('USD', 'US Dollar'),
    ('EUR', 'Euro'),
    ('SGD', 'Singapore Dollar'),
    ('JPY', 'Japanese Yen'),
    ('MYR', 'Malaysian Ringgit'),
    ('IDR', 'Indonesian Rupiah')
ON CONFLICT (currency_code) DO NOTHING;
