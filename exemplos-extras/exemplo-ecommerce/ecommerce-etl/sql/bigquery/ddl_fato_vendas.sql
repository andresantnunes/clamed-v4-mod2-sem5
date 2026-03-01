-- DDL for the fact table in BigQuery for e-commerce sales
CREATE TABLE `your_project_id.your_dataset_id.fato_vendas` (
    venda_id STRING NOT NULL,  -- Unique identifier for each sale
    cliente_id STRING NOT NULL,  -- Foreign key referencing the client
    produto_id STRING NOT NULL,  -- Foreign key referencing the product
    quantidade INT64 NOT NULL,  -- Quantity of the product sold
    preco NUMERIC NOT NULL,  -- Price of the product at the time of sale
    data_venda TIMESTAMP NOT NULL,  -- Timestamp of the sale
    PRIMARY KEY (venda_id)  -- Primary key constraint
)
OPTIONS(
    description="Fact table for sales in the e-commerce system"
);