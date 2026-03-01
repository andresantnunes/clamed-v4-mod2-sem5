-- SQL DDL for creating the dimension table for SCD Type 1 in PostgreSQL

CREATE TABLE dim_cliente_scd1 (
    id_cliente SERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    endereco VARCHAR(255),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups
CREATE INDEX idx_nome ON dim_cliente_scd1 (nome);