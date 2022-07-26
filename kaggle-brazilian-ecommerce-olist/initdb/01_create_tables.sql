CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS geolocation(
    zip_code_prefix CHAR(5) NOT NULL,
    coordinates POINT NOT NULL,
    city TEXT NOT NULL,
    STATE CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS customers(
    id CHAR(32) PRIMARY KEY,
    unique_id CHAR(32) NOT NULL,
    zip_code_prefix CHAR(5) NOT NULL,
    city TEXT NOT NULL,
    state CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS sellers(
    id CHAR(32) PRIMARY KEY,
    zip_code_prefix CHAR(5) NOT NULL,
    city TEXT NOT NULL,
    state CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS products(
    id CHAR(32) PRIMARY KEY,
    category_name TEXT NOT NULL,
    name_length INT,
    description_length INT,
    photos_qty INT,
    weight_g INT,
    length_cm INT,
    height_cm INT,
    width_cm INT
);

CREATE TABLE IF NOT EXISTS orders(
    id CHAR(32) PRIMARY KEY,
    customer_id CHAR(32) NOT NULL, -- REFERENCES customers(id) does not work because some customer_ids are not present in the customers table
    status TEXT NOT NULL,
    purchase_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    approved_at TIMESTAMP WITH TIME ZONE,
    delivered_carrier_date TIMESTAMP WITH TIME ZONE,
    delivered_customer_date TIMESTAMP WITH TIME ZONE,
    estimated_delivery_date TIMESTAMP WITH TIME ZONE
);