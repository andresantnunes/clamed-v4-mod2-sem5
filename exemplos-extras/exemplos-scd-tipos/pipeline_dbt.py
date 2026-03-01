"""
Pipeline ELT com dbt — Exemplo completo de orquestração.

Este script demonstra o fluxo ELT onde:
  1. O Python (gerador_dados.py) gera os dados de clientes
  2. Os dados são carregados na área de staging (simulada localmente)
  3. O dbt é invocado para aplicar as transformações (SCD Tipo 2 via Snapshot)

O script gera a estrutura completa de um projeto dbt na pasta 'dbt_projeto/'
e executa os comandos necessários.

Utiliza o gerador_dados.py (do ecommerce-etl) para gerar os dados de clientes.

Pré-requisitos (para execução real com BigQuery):
  - dbt-core e dbt-bigquery instalados (`pip install dbt-core dbt-bigquery`)
  - Credenciais do GCP configuradas
  - Ajustar profiles.yml com seu projeto/dataset

Para execução local de demonstração (sem BigQuery):
  - O script gera toda a estrutura do projeto dbt e os CSVs
  - Você pode inspecionar os arquivos gerados e entender o fluxo
"""

import os
import subprocess
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
# Configuração
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DBT_PROJECT_DIR = BASE_DIR / "dbt_projeto"

# BigQuery — AJUSTE AQUI para execução real
GCP_PROJECT = "meu-projeto-gcp"
BQ_DATASET = "meu_data_warehouse"
BQ_STAGING_DATASET = "staging"
KEYFILE_PATH = "/caminho/para/service-account.json"


# =====================================================================
# 1. GERAÇÃO E PREPARAÇÃO DE DADOS
# =====================================================================
def preparar_dados_csv() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Usa o gerador_dados.py para criar dois CSVs simulando dois dias:
      - clientes_dia1.csv: Carga inicial
      - clientes_dia2.csv: Com atualizações (endereço mudou, novos clientes)
    """
    data_dir = DBT_PROJECT_DIR / "seeds"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Dia 1 — Carga inicial
    print("  Gerando dados do Dia 1 (carga inicial)...")
    df_dia1 = gerar_dados_iniciais(num_registros=10)
    df_dia1.to_csv(data_dir / "clientes_dia1.csv", index=False)

    # Dia 2 — Com atualizações
    print("  Gerando dados do Dia 2 (com atualizações)...")
    df_dia2 = gerar_updates(df_dia1.copy(), num_updates=5)
    df_dia2.to_csv(data_dir / "clientes_dia2.csv", index=False)

    print(f"  CSVs salvos em: {data_dir}")
    return df_dia1, df_dia2


# =====================================================================
# 2. CRIAÇÃO DA ESTRUTURA DO PROJETO dbt
# =====================================================================
def criar_projeto_dbt() -> None:
    """Gera toda a estrutura de pastas e arquivos do projeto dbt."""

    # Pastas principais
    pastas = [
        DBT_PROJECT_DIR / "models" / "staging",
        DBT_PROJECT_DIR / "models" / "marts",
        DBT_PROJECT_DIR / "snapshots",
        DBT_PROJECT_DIR / "tests",
        DBT_PROJECT_DIR / "macros",
        DBT_PROJECT_DIR / "seeds",
    ]
    for pasta in pastas:
        pasta.mkdir(parents=True, exist_ok=True)

    # ---- dbt_project.yml ----
    dbt_project_yml = f"""\
name: 'ecommerce_dw'
version: '1.0.0'
config-version: 2

profile: 'ecommerce_dw'

model-paths: ["models"]
snapshot-paths: ["snapshots"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

clean-targets:
  - "target"
  - "dbt_packages"

seeds:
  ecommerce_dw:
    +schema: {BQ_STAGING_DATASET}
"""
    (DBT_PROJECT_DIR / "dbt_project.yml").write_text(dbt_project_yml, encoding="utf-8")

    # ---- profiles.yml ----
    profiles_yml = f"""\
ecommerce_dw:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: {GCP_PROJECT}
      dataset: {BQ_DATASET}
      threads: 4
      keyfile: {KEYFILE_PATH}
      timeout_seconds: 300
      location: US
"""
    (DBT_PROJECT_DIR / "profiles.yml").write_text(profiles_yml, encoding="utf-8")

    # ---- models/staging/stg_clientes.sql ----
    stg_clientes_sql = """\
-- models/staging/stg_clientes.sql
-- View de staging: padroniza colunas vindas do CSV (seed)

{{ config(materialized='view') }}

SELECT
    CAST(ID AS INT64)           AS id_cliente,
    UPPER(TRIM(Nome))           AS nome,
    UPPER(TRIM(Endereco))       AS endereco,
    ROUND(Preco_Score, 2)       AS preco_score
FROM {{ ref('clientes_dia1') }}
"""
    (DBT_PROJECT_DIR / "models" / "staging" / "stg_clientes.sql").write_text(
        stg_clientes_sql, encoding="utf-8"
    )

    # ---- models/staging/schema.yml ----
    stg_schema_yml = """\
version: 2

models:
  - name: stg_clientes
    description: "View de staging que padroniza dados brutos de clientes."
    columns:
      - name: id_cliente
        description: "Chave natural do cliente"
        tests:
          - not_null
          - unique
      - name: nome
        description: "Nome do cliente (maiúsculo)"
        tests:
          - not_null
      - name: endereco
        description: "Endereço do cliente"
      - name: preco_score
        description: "Score de preço do cliente"
"""
    (DBT_PROJECT_DIR / "models" / "staging" / "schema.yml").write_text(
        stg_schema_yml, encoding="utf-8"
    )

    # ---- snapshots/clientes_snapshot.sql (SCD Tipo 2 automático!) ----
    snapshot_sql = """\
-- snapshots/clientes_snapshot.sql
-- O dbt Snapshot aplica SCD Tipo 2 automaticamente!
-- Ele cria as colunas dbt_valid_from, dbt_valid_to e dbt_scd_id.

{% snapshot dim_clientes %}

{{
    config(
      target_schema='""" + BQ_DATASET + """',
      unique_key='id_cliente',
      strategy='check',
      check_cols=['endereco', 'preco_score']
    )
}}

-- Lê da view de staging
SELECT
    id_cliente,
    nome,
    endereco,
    preco_score
FROM {{ ref('stg_clientes') }}

{% endsnapshot %}
"""
    (DBT_PROJECT_DIR / "snapshots" / "clientes_snapshot.sql").write_text(
        snapshot_sql, encoding="utf-8"
    )

    # ---- models/marts/dim_clientes_atual.sql ----
    dim_atual_sql = """\
-- models/marts/dim_clientes_atual.sql
-- Tabela final para o BI: mostra apenas os registros ativos (versão mais recente)

{{ config(materialized='table') }}

SELECT
    id_cliente,
    nome,
    endereco,
    preco_score,
    dbt_valid_from AS data_inicio,
    dbt_valid_to   AS data_fim,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END AS is_ativo
FROM {{ ref('dim_clientes') }}
"""
    (DBT_PROJECT_DIR / "models" / "marts" / "dim_clientes_atual.sql").write_text(
        dim_atual_sql, encoding="utf-8"
    )

    # ---- models/marts/dim_clientes_historico.sql ----
    dim_hist_sql = """\
-- models/marts/dim_clientes_historico.sql
-- Tabela completa com todo o histórico de alterações dos clientes

{{ config(materialized='table') }}

SELECT
    id_cliente,
    nome,
    endereco,
    preco_score,
    dbt_valid_from AS data_inicio,
    dbt_valid_to   AS data_fim,
    CASE
        WHEN dbt_valid_to IS NULL THEN 'ATIVO'
        ELSE 'HISTORICO'
    END AS status
FROM {{ ref('dim_clientes') }}
ORDER BY id_cliente, dbt_valid_from
"""
    (DBT_PROJECT_DIR / "models" / "marts" / "dim_clientes_historico.sql").write_text(
        dim_hist_sql, encoding="utf-8"
    )

    # ---- tests/assert_clientes_ativos_unicos.sql ----
    test_sql = """\
-- tests/assert_clientes_ativos_unicos.sql
-- Garante que cada cliente tenha no máximo 1 registro ativo (dbt_valid_to IS NULL)

SELECT
    id_cliente,
    COUNT(*) AS qtd_ativos
FROM {{ ref('dim_clientes') }}
WHERE dbt_valid_to IS NULL
GROUP BY id_cliente
HAVING COUNT(*) > 1
"""
    (DBT_PROJECT_DIR / "tests" / "assert_clientes_ativos_unicos.sql").write_text(
        test_sql, encoding="utf-8"
    )

    print(f"  Projeto dbt criado em: {DBT_PROJECT_DIR}")


# =====================================================================
# 3. EXECUÇÃO DO dbt
# =====================================================================
def executar_dbt_seed() -> None:
    """Carrega os CSVs (seeds) para o BigQuery via dbt seed."""
    print("  Executando: dbt seed ...")
    _rodar_dbt("seed")


def executar_dbt_snapshot() -> None:
    """Executa o dbt snapshot (aplica SCD Tipo 2)."""
    print("  Executando: dbt snapshot ...")
    _rodar_dbt("snapshot")


def executar_dbt_run() -> None:
    """Executa os models (staging views + marts tables)."""
    print("  Executando: dbt run ...")
    _rodar_dbt("run")


def executar_dbt_test() -> None:
    """Executa os testes do dbt."""
    print("  Executando: dbt test ...")
    _rodar_dbt("test")


def _rodar_dbt(comando: str) -> None:
    """Executa um comando dbt dentro do diretório do projeto."""
    try:
        resultado = subprocess.run(
            ["dbt", comando, "--profiles-dir", str(DBT_PROJECT_DIR)],
            cwd=str(DBT_PROJECT_DIR),
            capture_output=True,
            text=True,
        )
        print(resultado.stdout)
        if resultado.returncode != 0:
            print(f"  [AVISO] dbt {comando} retornou código {resultado.returncode}")
            if resultado.stderr:
                print(resultado.stderr)
    except FileNotFoundError:
        print(
            f"  [AVISO] Comando 'dbt' não encontrado no PATH.\n"
            f"  Instale com: pip install dbt-core dbt-bigquery\n"
            f"  O projeto dbt foi gerado em {DBT_PROJECT_DIR} — "
            f"você pode executá-lo manualmente."
        )


# =====================================================================
# 4. FLUXO PRINCIPAL
# =====================================================================
def executar() -> None:
    """
    Fluxo ELT completo:
      1. Gera a estrutura do projeto dbt
      2. Gera os CSVs com o gerador_dados.py
      3. Carrega seeds no BigQuery (dbt seed)
      4. Executa snapshot para SCD Tipo 2 (dbt snapshot)
      5. Cria views e tabelas finais (dbt run)
      6. Roda testes de qualidade (dbt test)
    """
    print("=" * 70)
    print("  PIPELINE ELT COM dbt — Exemplo de aula")
    print("=" * 70)

    # 1. Criar projeto dbt
    print("\n[1/6] Criando estrutura do projeto dbt...")
    criar_projeto_dbt()

    # 2. Gerar dados
    print("\n[2/6] Gerando dados com gerador_dados.py...")
    df_dia1, df_dia2 = preparar_dados_csv()

    print("\n--- Dados Dia 1 (Carga Inicial) ---")
    print(df_dia1.head(10).to_string(index=False))
    print("\n--- Dados Dia 2 (Com Atualizações) ---")
    print(df_dia2.head(10).to_string(index=False))

    # 3-6. Executar dbt
    print("\n[3/6] Carregando seeds no BigQuery...")
    executar_dbt_seed()

    print("\n[4/6] Aplicando SCD Tipo 2 via dbt snapshot (Dia 1)...")
    executar_dbt_snapshot()

    # Simular Dia 2: trocar o seed para clientes_dia2
    seed_dir = DBT_PROJECT_DIR / "seeds"
    stg_model = DBT_PROJECT_DIR / "models" / "staging" / "stg_clientes.sql"
    conteudo_stg = stg_model.read_text(encoding="utf-8")
    conteudo_stg_dia2 = conteudo_stg.replace("clientes_dia1", "clientes_dia2")
    stg_model.write_text(conteudo_stg_dia2, encoding="utf-8")

    print("\n  [Simulando Dia 2] Trocando source para clientes_dia2...")
    executar_dbt_seed()

    print("\n[5/6] Aplicando SCD Tipo 2 via dbt snapshot (Dia 2)...")
    executar_dbt_snapshot()

    print("\n[6/6] Criando tabelas finais e rodando testes...")
    executar_dbt_run()
    executar_dbt_test()

    # Restaurar modelo para dia1 (estado original)
    stg_model.write_text(conteudo_stg, encoding="utf-8")

    print("\n" + "=" * 70)
    print("  PIPELINE CONCLUÍDA!")
    print("=" * 70)
    print(f"\n  Projeto dbt gerado em: {DBT_PROJECT_DIR}")
    print(f"  Seeds (CSVs):          {seed_dir}")
    print(f"\n  Estrutura do projeto:")
    _listar_arvore(DBT_PROJECT_DIR, prefixo="    ")


def _listar_arvore(caminho: Path, prefixo: str = "") -> None:
    """Imprime a árvore de diretórios do projeto."""
    itens = sorted(caminho.iterdir(), key=lambda p: (p.is_file(), p.name))
    for i, item in enumerate(itens):
        conector = "└── " if i == len(itens) - 1 else "├── "
        print(f"{prefixo}{conector}{item.name}")
        if item.is_dir() and item.name not in ("target", "dbt_packages", "__pycache__"):
            extensao = "    " if i == len(itens) - 1 else "│   "
            _listar_arvore(item, prefixo + extensao)


if __name__ == "__main__":
    executar()
