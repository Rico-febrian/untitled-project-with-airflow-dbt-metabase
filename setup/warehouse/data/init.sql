-- 1. Create Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- 2. Staging Table (Raw Data Storage)
CREATE TABLE staging.stg_weather_api (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100),
    raw_json JSONB NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Warehouse Dimension Table
CREATE TABLE warehouse.dim_locations (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    lat DECIMAL(9,6) NOT NULL,
    lon DECIMAL(9,6) NOT NULL,
    country VARCHAR(10),
    CONSTRAINT unique_city_country UNIQUE (city_name, country)
);

-- 4. Warehouse Fact Table
CREATE TABLE warehouse.fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    temp FLOAT,
    feels_like FLOAT,
    humidity INT,
    wind_speed FLOAT,
    visibility INT,
    condition VARCHAR(100),
    weather_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,       
    CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES warehouse.dim_locations(city_id),
    CONSTRAINT unique_city_time UNIQUE (city_id, weather_timestamp)
);