-- SQL DDL for the fact table in PostgreSQL for sales data

CREATE TABLE fato_vendas (
    id_venda SERIAL PRIMARY KEY,
    id_cliente INT NOT NULL,
    id_produto INT NOT NULL,
    quantidade INT NOT NULL,
    preco_unitario DECIMAL(10, 2) NOT NULL,
    data_venda TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente),
    FOREIGN KEY (id_produto) REFERENCES dim_produto(id_produto)
);

-- Indexes for performance optimization
CREATE INDEX idx_id_cliente ON fato_vendas(id_cliente);
CREATE INDEX idx_data_venda ON fato_vendas(data_venda);