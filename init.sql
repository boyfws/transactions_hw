CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    balance NUMERIC NOT NULL
    CONSTRAINT balance_positive CHECK (balance >= 0)
);

INSERT INTO accounts (balance) VALUES (1000), (1500), (2000);