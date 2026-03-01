# etl_engine.py

import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
import hashlib
import yaml

# Load configuration for PostgreSQL and BigQuery
def load_config():
    with open('../config/postgres.yaml', 'r') as file:
        postgres_config = yaml.safe_load(file)
    # with open('../config/bigquery.yaml', 'r') as file:
    #     bigquery_config = yaml.safe_load(file)

    bigquery_config = bigquery.Client.from_service_account_json("projeto-bq-488510-8fdf22f4a544.json")
    
    return postgres_config, bigquery_config

# Function to generate a surrogate key using hashing
def generate_surrogate_key(value):
    return int(hashlib.md5(value.encode()).hexdigest(), 16) % (10 ** 10)

# Flow 1: Load data into PostgreSQL staging table and apply SCD Type 2 logic
def load_postgres_scd2(df):
    postgres_config, _ = load_config()
    engine = create_engine(postgres_config['connection_string'])
    
    # Load data into staging table
    df.to_sql('staging_clientes', engine, if_exists='replace', index=False)
    
    # Apply SCD Type 2 logic using MERGE statement
    with engine.connect() as connection:
        connection.execute("""
            MERGE INTO dim_cliente AS target
            USING staging_clientes AS source
            ON target.id_cliente = source.id_cliente
            WHEN MATCHED AND target.nome <> source.nome THEN
                UPDATE SET target.data_fim = CURRENT_DATE, target.status_ativo = FALSE
            WHEN NOT MATCHED THEN
                INSERT (sk_cliente, id_cliente, nome, cidade, status_ativo, data_inicio, data_fim)
                VALUES (generate_surrogate_key(source.id_cliente), source.id_cliente, source.nome, source.cidade, TRUE, CURRENT_DATE, '9999-12-31');
        """)

# Flow 2: Upload DataFrame to BigQuery staging dataset and apply SCD Type 2
def load_bigquery_scd2(df):
    _, bigquery_config = load_config()
    client = bigquery.Client.from_service_account_json(
        bigquery_config['service_account_json']
        )
    
    # Load data into BigQuery staging table
    job = client.load_table_from_dataframe(df, bigquery_config['staging_table'])
    job.result()  # Wait for the job to complete
    
    # Apply SCD Type 2 logic using MERGE query
    merge_query = f"""
        MERGE `{bigquery_config['dataset']}.dim_cliente` AS target
        USING `{bigquery_config['dataset']}.staging_clientes` AS source
        ON target.id_cliente = source.id_cliente
        WHEN MATCHED AND target.nome <> source.nome THEN
            UPDATE SET target.data_fim = CURRENT_DATE(), target.status_ativo = FALSE
        WHEN NOT MATCHED THEN
            INSERT (sk_cliente, id_cliente, nome, cidade, status_ativo, data_inicio, data_fim)
            VALUES (GENERATE_UUID(), source.id_cliente, source.nome, source.cidade, TRUE, CURRENT_DATE(), '9999-12-31');
    """
    client.query(merge_query).result()  # Execute the merge query

# Flow 3: Load product data into PostgreSQL staging table and perform Upsert (SCD Type 1)
def load_postgres_scd1(df):
    postgres_config, _ = load_config()
    engine = create_engine(postgres_config['connection_string'])
    
    # Load data into staging table
    df.to_sql('staging_produtos', engine, if_exists='replace', index=False)
    
    # Perform Upsert (SCD Type 1)
    with engine.connect() as connection:
        for index, row in df.iterrows():
            connection.execute("""
                INSERT INTO dim_produto (id_produto, nome, preco)
                VALUES (:id_produto, :nome, :preco)
                ON CONFLICT (id_produto) DO UPDATE SET
                nome = EXCLUDED.nome,
                preco = EXCLUDED.preco;
            """, {'id_produto': row['id_produto'], 'nome': row['nome'], 'preco': row['preco']})

# Main function to execute ETL processes
def main():
    # Load the transactional data
    df_clientes = pd.read_csv('../data/clientes.csv')
    
    # Execute ETL flows
    # load_postgres_scd2(df_clientes)
    load_bigquery_scd2(df_clientes)
    # load_postgres_scd1(df_clientes)

if __name__ == "__main__":
    main()