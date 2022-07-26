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