# ETL Normal — Instruções

Este repositório contém exemplos simples de ETL/ELT (geração de dados mock, full load e cargas incrementais) e uma DAG de exemplo para Apache Airflow.

Requisitos
- Python 3.10 ou 3.11 (recomendado para Airflow compatível)
- Recomendo criar um virtualenv antes de instalar dependências.

Instalação (Windows PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Observação sobre Airflow
- Siga as instruções oficiais do Airflow para instalação em produção (System deps, constraints file, executors).
- Para testes locais, após instalar as dependências:
```powershell
setx AIRFLOW_HOME "%USERPROFILE%\airflow"
airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
airflow webserver --port 8080
# Em outra sessão
airflow scheduler
```
A DAG de exemplo `pipeline_etl_dag.py` será copiada para a pasta `dags/` e aparecerá na UI do Airflow.

Executando scripts diretamente
- Gerar 500 registros e salvar CSVs:
```powershell
python gerador_dados.py -n 500 --save
```
- Rodar full load (usa `gerador_dados` internamente):
```powershell
python full_load.py -n 500
```
- Rodar carga incremental (SCD1/SCD2):
```powershell
python load_incremental.py -n 100 --tipo ambos
```

Notas sobre Banco de Dados
- As conexões nos scripts usam valores placeholder (usuário/senha/host/db). Atualize conforme seu Postgres local antes de rodar.
- Para testar sem Postgres, muitos scripts aceitam apenas gerar CSVs e executar transformações locais.

Arquivos principais
- `gerador_dados.py`: gerador mock de clientes e vendas (CLI)
- `full_load.py`: transformações + full load para `dim_cliente` e `fato_vendas`
- `load_incremental.py`: funções para SCD Tipo 1 e Tipo 2
- `pipeline.py`, `clientes_snapshot.sql`: exemplos conceituais (dbt / Airflow GCP)
- `dags/pipeline_etl_dag.py`: DAG do Airflow (exemplo de orquestração)

Se quiser, eu posso:
- ajustar a DAG para usar `PythonOperator` com chamadas diretas às funções em vez de `BashOperator`,
- fornecer um `constraints.txt` para instalar Airflow de forma segura,
- ou adaptar os scripts para rodar com SQLite para testes locais sem Postgres.
