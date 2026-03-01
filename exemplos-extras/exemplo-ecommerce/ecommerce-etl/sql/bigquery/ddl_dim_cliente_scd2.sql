CREATE TABLE dim_cliente (
    sk_cliente INT64 NOT NULL,
    id_cliente INT64 NOT NULL,
    nome STRING NOT NULL,
    cidade STRING NOT NULL,
    status_ativo BOOLEAN NOT NULL,
    data_inicio DATE NOT NULL,
    data_fim DATE NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (sk_cliente)
) OPTIONS(
    description="Dimension table for clients with Slowly Changing Dimension Type 2"
);