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
    category_name TEXT,
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
    customer_id CHAR(32) REFERENCES customers(id),
    status TEXT NOT NULL,
    purchase_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    approved_at TIMESTAMP WITH TIME ZONE,
    delivered_carrier_date TIMESTAMP WITH TIME ZONE,
    delivered_customer_date TIMESTAMP WITH TIME ZONE,
    estimated_delivery_date TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS order_items(
    order_id CHAR(32) REFERENCES orders(id),
    order_item_id INT NOT NULL,
    product_id CHAR(32) REFERENCES products(id),
    seller_id CHAR(32) REFERENCES sellers(id),
    limit_date TIMESTAMP WITH TIME ZONE NOT NULL,
    price DECIMAL NOT NULL,
    freight_value DECIMAL NOT NULL,
    UNIQUE(order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS iso_4217_currency_codes(
    code CHAR(3) PRIMARY KEY,
    num SMALLINT UNIQUE,
    currency TEXT NOT NULL
);

INSERT INTO iso_4217_currency_codes(code, num, currency) VALUES('BRL', 986, 'Brazilian real');

CREATE TABLE IF NOT EXISTS order_payments(
    order_id CHAR(32) REFERENCES orders(id),
    sequential INT NOT NULL,
    type TEXT NOT NULL,
    installments INT NOT NULL,
    payment_value DECIMAL NOT NULL,
    currency_code CHAR(3) REFERENCES iso_4217_currency_codes
);

CREATE TABLE IF NOT EXISTS order_reviews(
    id CHAR(32), -- Not a PRIMARY KEY because there are duplicates ids with different row data in the dataset
    order_id CHAR(32) REFERENCES orders(id),
    score INT NOT NULL,
    comment_title TEXT,
    comment_message TEXT,
    creation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    answer_timestamp TIMESTAMP WITH TIME ZONE
);

CREATE INDEX id_idx ON order_reviews(id);
