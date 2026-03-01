-- SQL DDL for creating the dimension table for SCD Type 2 in PostgreSQL

CREATE TABLE dim_cliente_scd2 (
    sk_cliente SERIAL PRIMARY KEY,  -- Surrogate Key
    id_cliente INT NOT NULL,         -- Business Key
    nome VARCHAR(255) NOT NULL,      -- Customer Name
    cidade VARCHAR(255) NOT NULL,    -- Customer City
    status_ativo BOOLEAN NOT NULL,    -- Active Status
    data_inicio DATE NOT NULL,        -- Start Date of the record
    data_fim DATE NOT NULL,           -- End Date of the record
    UNIQUE (id_cliente, data_fim)     -- Ensure uniqueness for SCD Type 2
);

-- Index for faster querying
CREATE INDEX idx_dim_cliente_id ON dim_cliente_scd2 (id_cliente);