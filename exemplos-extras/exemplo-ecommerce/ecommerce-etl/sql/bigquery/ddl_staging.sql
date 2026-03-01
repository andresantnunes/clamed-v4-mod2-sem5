-- SQL DDL for creating the staging table in Google BigQuery

CREATE OR REPLACE TABLE `your_project_id.your_dataset_id.staging_table` (
    id_cliente INT64 NOT NULL,
    nome STRING,
    endereco STRING,
    preco FLOAT64,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Add any additional fields as necessary for your staging table.