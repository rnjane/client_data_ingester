CREATE TABLE clients (
    id SERIAL PRIMARY KEY NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    sign_up_dt TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    address VARCHAR(512),
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE users (
    id SERIAL PRIMARY KEY NOT NULL,
    client_id INTEGER NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    created_on TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    password_hash VARCHAR(255) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    session_token VARCHAR(255),
    last_login TIMESTAMP WITHOUT TIME ZONE,
    FOREIGN KEY (client_id) REFERENCES clients(id)
);

CREATE TABLE client_products (
    id SERIAL PRIMARY KEY NOT NULL,
    client_id INTEGER NOT NULL,
    sku VARCHAR(100) NOT NULL,
    remote_id VARCHAR(100),
    brand VARCHAR(100),
    title VARCHAR(255),
    last_changed_on TIMESTAMP WITHOUT TIME ZONE DEFAULT (CURRENT_TIMESTAMP),
    stock_quantity INTEGER,
    active BOOLEAN NOT NULL DEFAULT true,
    max_price DECIMAL(12,2),
    min_price DECIMAL(12,2),
    reference_price DECIMAL(12,2),
    FOREIGN KEY (client_id) REFERENCES clients(id)
);



