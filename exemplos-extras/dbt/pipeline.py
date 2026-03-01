# Imagine o seguinte fluxo diário (Pipeline ELT) rodando na nuvem do Google:

# Extração (Python + GCS): Um script em Python busca os dados novos de clientes na API da empresa e salva um arquivo clientes_novo.csv dentro de um "Bucket" (pasta na nuvem) no Google Cloud Storage (GCS). Essa é a nossa Landing Zone (Área de Pouso).
# Carga Bruta (GCS para BigQuery): O orquestrador pega esse CSV do Storage e joga diretamente para uma tabela temporária chamada stg_clientes dentro do Google BigQuery.
# Transformação Avançada (dbt + BigQuery): O orquestrador avisa o dbt que os dados novos chegaram. O dbt entra em ação, lê a tabela stg_clientes no BigQuery, compara com a tabela final dim_clientes e faz a mágica do SCD Tipo 2 (guardando o histórico de quem mudou de cidade).

# Orquestração Geral: O "maestro" que garante que o passo 1, 2 e 3 rodem na ordem certa (e avise se algo der errado) é o Cloud Composer (versão gerenciada do Apache Airflow no GCP).
# Para esse exemplo, vamos precisar de dois blocos principais de código: O script do dbt (SQL) e a DAG do Airflow (Python).

# A. O Código do dbt (A Mágica do SCD Tipo 2)
# Lembra daquele SQL gigante e complexo que fizemos antes com UPDATE e INSERT? Esqueça ele. No dbt, nós usamos um recurso nativo chamado Snapshot.
# Você só precisa criar um arquivo chamado clientes_snapshot.sql na pasta do dbt:

# Arquivo: clientes_snapshot.sql

# O que acontece aqui? Quando você manda o dbt rodar isso, ele mesmo cria no BigQuery as colunas dbt_valid_from (data de início), dbt_valid_to (data de fim) e faz todo o controle de histórico de forma automática. Se a Maria mudou de SP para o RJ, ele fecha o registro de SP e abre o do RJ sozinho.

# B. O Código do Airflow (A DAG Orquestradora)
# Este é o arquivo Python que vai ficar dentro do seu Cloud Composer (Airflow) para amarrar tudo e rodar todos os dias.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd

# Função Python para simular a extração da API e envio para o Google Cloud Storage
def extrair_api_para_gcs():
    # 1. Simula a busca de dados novos
    dados_novos = pd.DataFrame({
        'id_cliente': [1, 2],
        'nome': ['MARIA', 'JOAO'],
        'cidade': ['RIO DE JANEIRO', 'SAO PAULO'] # Maria mudou para o RJ!
    })
    # 2. Salva direto no Cloud Storage (usando o padrão gs://)
    dados_novos.to_csv('gs://meu-bucket-data-lake/clientes_hoje.csv', index=False)
    print("Dados salvos no Cloud Storage com sucesso!")

# Definindo as configurações da Pipeline
default_args = {
    'owner': 'engenharia_de_dados',
    'start_date': datetime(2026, 2, 21),
}

with DAG(
    'pipeline_clientes_gcp',
    default_args=default_args,
    schedule_interval='@daily', # Roda todo dia
    catchup=False
) as dag:

    # TAREFA 1: Extrair dados e jogar no Data Lake (GCS)
    task_extracao = PythonOperator(
        task_id='extrair_api_para_gcs',
        python_callable=extrair_api_para_gcs
    )

    # TAREFA 2: Pegar o CSV do GCS e colocar na tabela Staging do BigQuery
    # Olha que incrível: não precisamos escrever Python para isso, o Airflow já tem um operador pronto!
    task_carga_bigquery = GCSToBigQueryOperator(
        task_id='gcs_para_bigquery_staging',
        bucket='meu-bucket-data-lake',
        source_objects=['clientes_hoje.csv'],
        destination_project_dataset_table='meu_projeto.minha_area_staging.stg_clientes',
        write_disposition='WRITE_TRUNCATE', # Apaga a staging anterior e insere os novos
        source_format='CSV',
        skip_leading_rows=1 # Pula o cabeçalho do CSV
    )

    # TAREFA 3: Rodar o dbt para aplicar o SCD Tipo 2 no BigQuery
    # Dispara o comando do dbt através do terminal (Bash)
    task_transformacao_dbt = BashOperator(
        task_id='rodar_dbt_snapshot',
        bash_command='cd /caminho/do/meu/projeto_dbt && dbt snapshot'
    )

    # ORDEM DE EXECUÇÃO DA PIPELINE
    task_extracao >> task_carga_bigquery >> task_transformacao_dbt

# Resumo do que construímos:
# O Airflow acorda e manda o Python buscar os dados e jogar num Bucket do Google (GCS).

# O Airflow pega os dados desse Bucket e joga no BigQuery (na tabela stg_clientes).

# O Airflow manda o dbt agir. O dbt lê a stg_clientes, compara com a tabela final, vê que a Maria mudou de cidade, fecha o histórico dela no passado e insere a nova linha do Rio de Janeiro.