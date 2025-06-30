CREATE TABLE clients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name VARCHAR(255) NOT NULL,
    sign_up_dt TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    address VARCHAR(512),
    active BOOLEAN NOT NULL DEFAULT 1
);

CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    created_on DATETIME NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT 1,
    session_token VARCHAR(255),
    last_login DATETIME,
    FOREIGN KEY (client_id) REFERENCES clients(id)
);

CREATE TABLE client_products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    sku VARCHAR(100) NOT NULL,
    remote_id VARCHAR(100),
    brand VARCHAR(100),
    title VARCHAR(255),
    last_changed_on TIMESTAMP,
    stock_quantity INTEGER,
    active BOOLEAN NOT NULL DEFAULT 1,
    max_price DECIMAL(12,2),
    min_price DECIMAL(12,2),
    reference_price DECIMAL(12,2),
    FOREIGN KEY (client_id) REFERENCES clients(id)
);



