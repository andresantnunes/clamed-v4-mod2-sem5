CREATE TABLE dim_cliente_scd1 (
    id_cliente INT PRIMARY KEY,
    nome STRING NOT NULL,
    endereco STRING,
    preco FLOAT,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);