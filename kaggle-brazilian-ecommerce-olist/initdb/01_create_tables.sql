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