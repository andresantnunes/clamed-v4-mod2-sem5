"""
ETL SCD Tipo 2 — Versão Google BigQuery.

Preserva o histórico de alterações usando MERGE no BigQuery.
Quando um cliente muda de Endereco ou Preco_Score, o registro antigo é
fechado (status_ativo = FALSE, data_fim = CURRENT_DATE) e uma nova linha
é inserida com os dados atualizados.

Utiliza o gerador_dados.py (do ecommerce-etl) para gerar e atualizar clientes.

Pré-requisitos:
  - google-cloud-bigquery instalado (`pip install google-cloud-bigquery pandas-gbq`)
  - Variável de ambiente GOOGLE_APPLICATION_CREDENTIALS apontando para o JSON
    da Service Account, OU autenticação via `gcloud auth application-default login`.
  - Ajustar as constantes PROJECT_ID e DATASET abaixo.

Fluxo:
  1. Gera dados iniciais com gerar_dados_iniciais()
  2. Carrega na staging (stg_clientes) no BigQuery
  3. Aplica SCD Tipo 2 na dimensão (dim_clientes) via MERGE
  4. Gera atualizações com gerar_updates()
  5. Recarrega staging e reaplicar SCD Tipo 2
  6. Consulta e imprime resultado
"""

import hashlib
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Importar gerador_dados do ecommerce-etl/src
# ---------------------------------------------------------------------------
GERADOR_DIR = (
    Path(__file__).resolve().parent.parent
    / "exemplo-aula"
    / "ecommerce-etl"
    / "src"
)
sys.path.insert(0, str(GERADOR_DIR))

from gerador_dados import gerar_dados_iniciais, gerar_updates  # noqa: E402

# ---------------------------------------------------------------------------
# Configuração do BigQuery — AJUSTE AQUI
# ---------------------------------------------------------------------------
PROJECT_ID = "meu-projeto-gcp"          # ID do projeto no Google Cloud
DATASET = "meu_data_warehouse"           # Nome do dataset no BigQuery
TABELA_STAGING = f"{DATASET}.stg_clientes"
TABELA_DIM = f"{DATASET}.dim_clientes"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def gerar_surrogate_key(valor: str) -> int:
    """Gera uma Surrogate Key numérica via hash MD5."""
    return int(hashlib.md5(str(valor).encode()).hexdigest(), 16) % (10 ** 10)


def obter_client():
    """
    Retorna um cliente BigQuery autenticado.
    Requer google-cloud-bigquery instalado e credenciais configuradas.
    """
    try:
        from google.cloud import bigquery
        return bigquery.Client(project=PROJECT_ID)
    except ImportError:
        print(
            "ERRO: biblioteca google-cloud-bigquery não encontrada.\n"
            "Instale com: pip install google-cloud-bigquery pandas-gbq"
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# DDL — Criação das tabelas
# ---------------------------------------------------------------------------
def criar_tabelas(client) -> None:
    """Cria as tabelas de staging e dimensão no BigQuery (se não existirem)."""

    ddl_staging = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{TABELA_STAGING}` (
        ID          INT64       NOT NULL,
        Nome        STRING      NOT NULL,
        Endereco    STRING      NOT NULL,
        Preco_Score FLOAT64     NOT NULL
    )
    """

    ddl_dim = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{TABELA_DIM}` (
        sk_cliente   INT64     NOT NULL,
        ID           INT64     NOT NULL,
        Nome         STRING    NOT NULL,
        Endereco     STRING    NOT NULL,
        Preco_Score  FLOAT64   NOT NULL,
        status_ativo BOOL      NOT NULL,
        data_inicio  DATE      NOT NULL,
        data_fim     DATE      NOT NULL
    )
    """

    client.query(ddl_staging).result()
    client.query(ddl_dim).result()
    print("  Tabelas criadas/verificadas no BigQuery.")


# ---------------------------------------------------------------------------
# Staging
# ---------------------------------------------------------------------------
def carregar_staging(client, df: pd.DataFrame) -> None:
    """Carrega o DataFrame na tabela de staging (WRITE_TRUNCATE)."""
    from google.cloud.bigquery import LoadJobConfig, WriteDisposition

    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE)
    tabela_ref = f"{PROJECT_ID}.{TABELA_STAGING}"

    job = client.load_table_from_dataframe(df, tabela_ref, job_config=job_config)
    job.result()
    print(f"  Staging carregada: {job.output_rows} linhas em {tabela_ref}")


# ---------------------------------------------------------------------------
# SCD Tipo 2 via MERGE
# ---------------------------------------------------------------------------
def aplicar_scd_tipo2(client) -> None:
    """
    Aplica SCD Tipo 2 usando MERGE no BigQuery.

    O MERGE do BigQuery não suporta múltiplas ações WHEN MATCHED com a mesma
    condição, então dividimos em duas queries:
      1. UPDATE — fecha registros ativos cujo Endereco ou Preco_Score mudou.
      2. INSERT via MERGE — insere novas versões (alterados + totalmente novos).
    """

    # ---- PASSO 1: Fechar registros alterados ----
    query_fechar = f"""
    UPDATE `{PROJECT_ID}.{TABELA_DIM}` dim
       SET dim.status_ativo = FALSE,
           dim.data_fim     = CURRENT_DATE()
      FROM `{PROJECT_ID}.{TABELA_STAGING}` stg
     WHERE dim.ID = stg.ID
       AND dim.status_ativo = TRUE
       AND (
           dim.Endereco    <> stg.Endereco
        OR dim.Preco_Score <> stg.Preco_Score
       )
    """
    resultado_fechar = client.query(query_fechar).result()
    print(f"  MERGE Passo 1 (fechar antigos): OK")

    # ---- PASSO 2: Inserir novas versões ----
    query_inserir = f"""
    MERGE `{PROJECT_ID}.{TABELA_DIM}` AS dim
    USING (
        SELECT stg.*
          FROM `{PROJECT_ID}.{TABELA_STAGING}` stg
          LEFT JOIN `{PROJECT_ID}.{TABELA_DIM}` dim_ativo
            ON dim_ativo.ID = stg.ID
           AND dim_ativo.status_ativo = TRUE
         WHERE dim_ativo.ID IS NULL
            OR dim_ativo.Endereco    <> stg.Endereco
            OR dim_ativo.Preco_Score <> stg.Preco_Score
    ) AS novos
    ON FALSE  -- Forçar INSERT para todos os registros do subquery
    WHEN NOT MATCHED THEN
        INSERT (sk_cliente, ID, Nome, Endereco, Preco_Score, status_ativo, data_inicio, data_fim)
        VALUES (
            FARM_FINGERPRINT(CONCAT(CAST(novos.ID AS STRING), '_', CAST(CURRENT_TIMESTAMP() AS STRING))),
            novos.ID,
            novos.Nome,
            novos.Endereco,
            novos.Preco_Score,
            TRUE,
            CURRENT_DATE(),
            DATE '9999-12-31'
        )
    """
    resultado_inserir = client.query(query_inserir).result()
    print(f"  MERGE Passo 2 (inserir novos): OK")


# ---------------------------------------------------------------------------
# Exibição
# ---------------------------------------------------------------------------
def imprimir_dimensao(client, titulo: str = "") -> None:
    query = f"""
    SELECT sk_cliente, ID, Nome, Endereco, Preco_Score,
           status_ativo, data_inicio, data_fim
      FROM `{PROJECT_ID}.{TABELA_DIM}`
     ORDER BY ID, data_inicio
    """
    df = client.query(query).to_dataframe()
    print(f"\n{'=' * 90}")
    print(f"  {titulo}" if titulo else "  dim_clientes (BigQuery)")
    print(f"{'=' * 90}")
    if df.empty:
        print("  (vazia)")
    else:
        print(df.to_string(index=False))
    print()


def imprimir_historico_cliente(client, id_cliente: int) -> None:
    """Mostra todas as versões de um cliente específico no BigQuery."""
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{TABELA_DIM}`
     WHERE ID = {id_cliente}
     ORDER BY data_inicio
    """
    df = client.query(query).to_dataframe()
    print(f"\n--- Histórico do cliente ID={id_cliente} ---")
    if df.empty:
        print("  Nenhum registro encontrado.")
    else:
        print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# Execução completa
# ---------------------------------------------------------------------------
def executar() -> None:
    print("Conectando ao BigQuery...")
    client = obter_client()

    print("[1/5] Criando tabelas no BigQuery...")
    criar_tabelas(client)

    # 2. Gerar dados iniciais
    print("[2/5] Gerando dados iniciais (100 clientes)...")
    df_inicial = gerar_dados_iniciais(num_registros=100)

    # 3. Carga inicial
    print("[3/5] Carga inicial — Staging + SCD Tipo 2 (MERGE)...")
    carregar_staging(client, df_inicial)
    aplicar_scd_tipo2(client)
    imprimir_dimensao(client, "APÓS CARGA INICIAL")

    # 4. Gerar atualizações
    print("[4/5] Gerando 15 atualizações aleatórias...")
    df_atualizado = gerar_updates(df_inicial.copy(), num_updates=15)

    # 5. Carga incremental SCD2
    print("[5/5] Carga incremental — Staging + SCD Tipo 2 (MERGE)...")
    carregar_staging(client, df_atualizado)
    aplicar_scd_tipo2(client)
    imprimir_dimensao(client, "APÓS ATUALIZAÇÃO SCD TIPO 2")

    # Mostrar histórico de clientes com múltiplas versões
    query_multi = f"""
    SELECT ID, COUNT(*) as versoes
      FROM `{PROJECT_ID}.{TABELA_DIM}`
     GROUP BY ID
    HAVING COUNT(*) > 1
     LIMIT 5
    """
    df_multi = client.query(query_multi).to_dataframe()
    if not df_multi.empty:
        print("=" * 90)
        print("  EXEMPLOS DE HISTÓRICO (clientes com mais de uma versão)")
        print("=" * 90)
        for _, row in df_multi.iterrows():
            imprimir_historico_cliente(client, int(row["ID"]))

    print("Concluído! Dados persistidos no BigQuery.")


if __name__ == "__main__":
    executar()
