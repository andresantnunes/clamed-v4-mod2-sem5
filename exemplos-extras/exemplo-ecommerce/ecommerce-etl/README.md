# E-commerce ETL Project

This project implements an ETL (Extract, Transform, Load) process for an e-commerce system. It includes a data generator script that simulates transactional data and an ETL engine that processes this data using SCD (Slowly Changing Dimensions) techniques.

## Project Structure

```
ecommerce-etl
├── src
│   ├── gerador_dados.py        # Script to generate transactional data
│   └── etl_engine.py           # ETL engine implementing SCD logic
├── sql
│   ├── postgres
│   │   ├── ddl_staging.sql     # DDL for PostgreSQL staging tables
│   │   ├── ddl_dim_cliente_scd1.sql  # DDL for SCD Type 1 dimension table
│   │   ├── ddl_dim_cliente_scd2.sql  # DDL for SCD Type 2 dimension table
│   │   └── ddl_fato_vendas.sql  # DDL for sales fact table
│   └── bigquery
│       ├── ddl_staging.sql     # DDL for BigQuery staging tables
│       ├── ddl_dim_cliente_scd1.sql  # DDL for SCD Type 1 dimension table
│       ├── ddl_dim_cliente_scd2.sql  # DDL for SCD Type 2 dimension table
│       └── ddl_fato_vendas.sql  # DDL for sales fact table
├── config
│   ├── postgres.yaml            # Configuration for PostgreSQL connection
│   └── bigquery.yaml            # Configuration for BigQuery connection
├── requirements.txt             # Required Python packages
└── README.md                    # Project documentation
```

## Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd ecommerce-etl
   ```

2. **Install required packages**:
   Ensure you have Python installed, then run:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure database connections**:
   Update the `config/postgres.yaml` and `config/bigquery.yaml` files with your database connection details.

4. **Generate Data**:
   Run the data generator script to create initial data:
   ```bash
   python src/gerador_dados.py
   ```

5. **Run the ETL Process**:
   Execute the ETL engine script to load data into your databases:
   ```bash
   python src/etl_engine.py
   ```

## Usage Examples

- The `gerador_dados.py` script generates a CSV file named `clientes.csv` with fictitious customer data.
- The `etl_engine.py` script processes this data and applies SCD Type 1 and Type 2 logic to update the PostgreSQL and BigQuery databases.

## License

This project is licensed under the MIT License. See the LICENSE file for details.