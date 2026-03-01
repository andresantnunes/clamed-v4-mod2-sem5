"""
ETL SCD Tipo 2 — Versão SQL puro + Projeto dbt (Snapshot).

Este arquivo demonstra DUAS abordagens para SCD Tipo 2 usando SQL:

  A) SQL PURO (SQLite):
     - MERGE manual em 2 passos (UPDATE para fechar + INSERT para novas versões)
     - Usa CTEs para identificar registros alterados/novos
     - Executa tudo localmente com SQLite

  B) dbt SNAPSHOT (referência):
     - Gera os arquivos de um mini-projeto dbt pronto para usar
     - Usa a estratégia 'check' do dbt para detectar mudanças
     - Compatível com BigQuery, Postgres, Snowflake, etc.

Utiliza o gerador_dados.py (do ecommerce-etl) para gerar e atualizar clientes.

Fluxo SQL puro:
  1. Gera dados iniciais
  2. Carrega staging via Pandas → SQLite
  3. Aplica SCD Tipo 2 com SQL (CTE + UPDATE + INSERT)
  4. Gera atualizações e reaplicar
  5. Imprime histórico
"""

import sqlite3
import sys
from datetime import datetime
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
ARQ_DB = BASE_DIR / "scd2_sql_puro.db"
DBT_DIR = BASE_DIR / "dbt_scd2_exemplo"


def conectar() -> sqlite3.Connection:
    return sqlite3.connect(ARQ_DB)


# ═══════════════════════════════════════════════════════════════════════════
# PARTE A — SQL PURO (SQLite)
# ═══════════════════════════════════════════════════════════════════════════


def criar_dimensao(conexao: sqlite3.Connection) -> None:
    """Cria a tabela dim_clientes com colunas de controle SCD Tipo 2."""
    conexao.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_clientes (
            sk_cliente  INTEGER PRIMARY KEY AUTOINCREMENT,
            ID          INTEGER NOT NULL,
            Nome        TEXT    NOT NULL,
            Endereco    TEXT    NOT NULL,
            Preco_Score REAL    NOT NULL,
            is_current  INTEGER NOT NULL DEFAULT 1,
            data_inicio TEXT    NOT NULL,
            data_fim    TEXT    NOT NULL DEFAULT '9999-12-31'
        )
        """
    )
    conexao.commit()


def carregar_staging(conexao: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Carrega o DataFrame na tabela temporária stg_clientes."""
    df.to_sql("stg_clientes", conexao, if_exists="replace", index=False)


def aplicar_scd_tipo2_sql(conexao: sqlite3.Connection) -> None:
    """
    SCD Tipo 2 usando SQL com CTEs (Common Table Expressions).

    A lógica é dividida em dois comandos:
      1. UPDATE — fecha (is_current=0) registros ativos que mudaram.
      2. INSERT — insere novas versões (alterados + novos) usando uma CTE
         que identifica exatamente quais registros precisam ser criados.
    """
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor = conexao.cursor()

    # ---- PASSO 1: Fechar registros alterados (SQL com subquery) ----
    sql_fechar = """
        UPDATE dim_clientes
           SET is_current = 0,
               data_fim   = :agora
         WHERE is_current = 1
           AND ID IN (
               -- Subquery: IDs que existem na staging com dados diferentes
               SELECT stg.ID
                 FROM stg_clientes stg
                INNER JOIN dim_clientes dim
                   ON dim.ID = stg.ID
                  AND dim.is_current = 1
                WHERE stg.Endereco    <> dim.Endereco
                   OR stg.Preco_Score <> dim.Preco_Score
           )
    """
    cursor.execute(sql_fechar, {"agora": agora})
    fechados = cursor.rowcount

    # ---- PASSO 2: Inserir novas versões (SQL com CTE) ----
    # A CTE "registros_novos" identifica:
    # CTE = Comum Table Expression, uma "tabela temporária" que pode ser referenciada
    #   - Clientes alterados (endereço OU preco_score difere do ativo)
    #   - Clientes totalmente novos (não existem na dimensão)
    sql_inserir = """
        WITH registros_novos AS (
            SELECT
                stg.ID,
                stg.Nome,
                stg.Endereco,
                stg.Preco_Score
            FROM stg_clientes stg
            LEFT JOIN dim_clientes dim_ativo
              ON dim_ativo.ID = stg.ID
             AND dim_ativo.is_current = 1
            WHERE
                -- Cliente novo (não existe na dimensão)
                dim_ativo.ID IS NULL
                -- OU cliente existente com dados diferentes
                OR dim_ativo.Endereco    <> stg.Endereco
                OR dim_ativo.Preco_Score <> stg.Preco_Score
        )
        INSERT INTO dim_clientes (ID, Nome, Endereco, Preco_Score,
                                  is_current, data_inicio, data_fim)
        SELECT ID, Nome, Endereco, Preco_Score,
               1, :agora, '9999-12-31'
          FROM registros_novos
    """
    cursor.execute(sql_inserir, {"agora": agora})
    inseridos = cursor.rowcount

    conexao.commit()
    print(f"  SCD Tipo 2 (SQL) — Fechados: {fechados} | Inseridos: {inseridos}")


# ---------------------------------------------------------------------------
# Exibição
# ---------------------------------------------------------------------------
def imprimir_dimensao(conexao: sqlite3.Connection, titulo: str = "") -> None:
    df = pd.read_sql_query(
        "SELECT * FROM dim_clientes ORDER BY ID, data_inicio", conexao
    )
    print(f"\n{'=' * 90}")
    print(f"  {titulo}" if titulo else "  dim_clientes (SQL puro)")
    print(f"{'=' * 90}")
    if df.empty:
        print("  (vazia)")
    else:
        print(df.to_string(index=False))
    print()


def imprimir_historico_cliente(conexao: sqlite3.Connection, id_cliente: int) -> None:
    df = pd.read_sql_query(
        "SELECT * FROM dim_clientes WHERE ID = ? ORDER BY data_inicio",
        conexao,
        params=(id_cliente,),
    )
    print(f"\n--- Histórico do cliente ID={id_cliente} ---")
    if df.empty:
        print("  Nenhum registro encontrado.")
    else:
        print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# Execução da parte SQL pura
# ---------------------------------------------------------------------------
def executar_sql_puro() -> None:
    if ARQ_DB.exists():
        ARQ_DB.unlink()

    print("=" * 90)
    print("  PARTE A — SCD TIPO 2 COM SQL PURO (SQLite)")
    print("=" * 90)

    print("\n[1/4] Gerando dados iniciais (50 clientes)...")
    df_inicial = gerar_dados_iniciais(num_registros=50)

    with conectar() as conexao:
        criar_dimensao(conexao)

        print("[2/4] Carga inicial — Staging + SCD Tipo 2 (SQL)...")
        carregar_staging(conexao, df_inicial)
        aplicar_scd_tipo2_sql(conexao)
        imprimir_dimensao(conexao, "APÓS CARGA INICIAL (SQL)")

        print("[3/4] Gerando 15 atualizações aleatórias...")
        df_atualizado = gerar_updates(df_inicial.copy(), num_updates=15)

        print("[4/4] Carga incremental — Staging + SCD Tipo 2 (SQL)...")
        carregar_staging(conexao, df_atualizado)
        aplicar_scd_tipo2_sql(conexao)
        imprimir_dimensao(conexao, "APÓS ATUALIZAÇÃO SCD TIPO 2 (SQL)")

        # Mostrar histórico de clientes com múltiplas versões
        df_multi = pd.read_sql_query(
            """
            SELECT ID, COUNT(*) as versoes
              FROM dim_clientes
             GROUP BY ID
            HAVING COUNT(*) > 1
             LIMIT 5
            """,
            conexao,
        )
        if not df_multi.empty:
            print("=" * 90)
            print("  EXEMPLOS DE HISTÓRICO")
            print("=" * 90)
            for _, row in df_multi.iterrows():
                imprimir_historico_cliente(conexao, int(row["ID"]))

    print("Banco salvo em:", ARQ_DB)


# ═══════════════════════════════════════════════════════════════════════════
# PARTE B — GERADOR DE PROJETO dbt (Snapshot SCD Tipo 2)
# ═══════════════════════════════════════════════════════════════════════════


def gerar_projeto_dbt() -> None:
    """
    Gera uma estrutura completa de mini-projeto dbt com:
      - dbt_project.yml
      - snapshots/dim_clientes_snapshot.sql  (SCD Tipo 2 via dbt snapshot)
      - models/staging/stg_clientes.sql      (view de staging)
      - models/marts/dim_clientes.sql        (dimensão final)
      - seeds/clientes_dia1.csv              (dados iniciais)
      - seeds/clientes_dia2.csv              (dados com mudanças)

    Para executar:
      cd dbt_scd2_exemplo
      dbt seed          # carrega os CSVs
      dbt snapshot      # aplica SCD Tipo 2
      dbt run           # materializa os modelos
    """
    print("\n" + "=" * 90)
    print("  PARTE B — GERANDO PROJETO dbt (Snapshot SCD Tipo 2)")
    print("=" * 90)

    # ---- Estrutura de pastas ----
    pastas = [
        DBT_DIR,
        DBT_DIR / "snapshots",
        DBT_DIR / "models" / "staging",
        DBT_DIR / "models" / "marts",
        DBT_DIR / "seeds",
        DBT_DIR / "tests",
    ]
    for pasta in pastas:
        pasta.mkdir(parents=True, exist_ok=True)

    # ---- dbt_project.yml ----
    dbt_project = """\
# dbt_project.yml — Projeto de exemplo SCD Tipo 2
name: 'scd2_exemplo'
version: '1.0.0'

profile: 'scd2_exemplo'

model-paths: ["models"]
seed-paths: ["seeds"]
snapshot-paths: ["snapshots"]
test-paths: ["tests"]

seeds:
  scd2_exemplo:
    +schema: raw  # Os CSVs vão para o schema "raw"
"""
    (DBT_DIR / "dbt_project.yml").write_text(dbt_project, encoding="utf-8")

    # ---- profiles.yml (exemplo para BigQuery e Postgres) ----
    profiles = """\
# profiles.yml — Configuração de conexão com o banco
# Coloque este arquivo em ~/.dbt/profiles.yml
#
# Exemplo BigQuery:
scd2_exemplo:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: meu-projeto-gcp
      dataset: meu_data_warehouse
      threads: 4
      timeout_seconds: 300
#
# Exemplo Postgres (descomente e ajuste):
#    dev:
#      type: postgres
#      host: localhost
#      port: 5432
#      user: meu_usuario
#      password: minha_senha
#      dbname: meu_dw
#      schema: public
#      threads: 4
"""
    (DBT_DIR / "profiles.yml").write_text(profiles, encoding="utf-8")

    # ---- seeds/clientes_dia1.csv ----
    csv_dia1 = """\
id_cliente,nome,endereco,preco_score
1,Ana Silva,Rua A 100,85.50
2,Bruno Costa,Rua B 200,72.30
3,Carla Souza,Rua C 300,91.00
4,Diego Lima,Rua D 400,65.80
5,Eva Santos,Rua E 500,78.20
"""
    (DBT_DIR / "seeds" / "clientes_dia1.csv").write_text(csv_dia1, encoding="utf-8")

    # ---- seeds/clientes_dia2.csv ----
    csv_dia2 = """\
id_cliente,nome,endereco,preco_score
1,Ana Silva,Avenida Nova 999,85.50
2,Bruno Costa,Rua B 200,72.30
3,Carla Souza,Rua C 300,95.00
4,Diego Lima,Rua D 400,65.80
5,Eva Santos,Rua E 500,78.20
6,Fabio Rocha,Rua F 600,88.10
"""
    (DBT_DIR / "seeds" / "clientes_dia2.csv").write_text(csv_dia2, encoding="utf-8")

    # ---- models/staging/stg_clientes.sql ----
    stg_clientes = """\
-- models/staging/stg_clientes.sql
-- View que padroniza os dados brutos dos clientes.
-- Troque o ref() pelo seed ou source correto do seu projeto.

{{ config(materialized='view') }}

SELECT
    id_cliente,
    UPPER(TRIM(nome))       AS nome,
    TRIM(endereco)           AS endereco,
    ROUND(preco_score, 2)    AS preco_score
FROM {{ ref('clientes_dia1') }}
-- Na segunda execução, troque para clientes_dia2:
-- FROM {{ ref('clientes_dia2') }}
"""
    (DBT_DIR / "models" / "staging" / "stg_clientes.sql").write_text(
        stg_clientes, encoding="utf-8"
    )

    # ---- snapshots/dim_clientes_snapshot.sql ----
    snapshot = """\
-- snapshots/dim_clientes_snapshot.sql
-- SCD Tipo 2 automático via dbt Snapshot.
--
-- O dbt compara as colunas listadas em check_cols a cada execução.
-- Se alguma mudou, ele:
--   1. Fecha o registro antigo (dbt_valid_to = data atual)
--   2. Insere um novo registro com os dados atualizados
--
-- Colunas geradas automaticamente pelo dbt:
--   dbt_scd_id     → Surrogate Key (hash)
--   dbt_updated_at → Timestamp da última atualização
--   dbt_valid_from → Início da validade do registro (= data_inicio)
--   dbt_valid_to   → Fim da validade (NULL = registro ativo, = data_fim)

{% snapshot dim_clientes_snapshot %}

{{
    config(
        target_schema='analytics',
        unique_key='id_cliente',

        -- Estratégia 'check': compara colunas específicas para detectar mudança
        strategy='check',
        check_cols=['endereco', 'preco_score'],

        -- Alternativa: estratégia 'timestamp' (usa uma coluna updated_at da fonte)
        -- strategy='timestamp',
        -- updated_at='updated_at',
    )
}}

-- Lê os dados da staging (view padronizada)
SELECT
    id_cliente,
    nome,
    endereco,
    preco_score
FROM {{ ref('stg_clientes') }}

{% endsnapshot %}
"""
    (DBT_DIR / "snapshots" / "dim_clientes_snapshot.sql").write_text(
        snapshot, encoding="utf-8"
    )

    # ---- models/marts/dim_clientes.sql ----
    dim_clientes = """\
-- models/marts/dim_clientes.sql
-- Dimensão final de clientes com SCD Tipo 2.
-- Consome o snapshot e adiciona colunas de negócio amigáveis.

{{ config(materialized='table') }}

SELECT
    dbt_scd_id                          AS sk_cliente,
    id_cliente,
    nome,
    endereco,
    preco_score,

    -- Colunas de controle do SCD Tipo 2
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END                                 AS is_current,

    dbt_valid_from                      AS data_inicio,
    COALESCE(dbt_valid_to, '9999-12-31') AS data_fim

FROM {{ ref('dim_clientes_snapshot') }}
ORDER BY id_cliente, dbt_valid_from
"""
    (DBT_DIR / "models" / "marts" / "dim_clientes.sql").write_text(
        dim_clientes, encoding="utf-8"
    )

    # ---- tests/test_scd2_um_ativo_por_cliente.sql ----
    test_sql = """\
-- tests/test_scd2_um_ativo_por_cliente.sql
-- Garante que cada cliente tem exatamente 1 registro ativo (is_current = TRUE).
-- Se retornar alguma linha, o teste FALHA.

SELECT
    id_cliente,
    COUNT(*) AS qtd_ativos
FROM {{ ref('dim_clientes') }}
WHERE is_current = TRUE
GROUP BY id_cliente
HAVING COUNT(*) <> 1
"""
    (DBT_DIR / "tests" / "test_scd2_um_ativo_por_cliente.sql").write_text(
        test_sql, encoding="utf-8"
    )

    # ---- README.md ----
    readme = """\
# Projeto dbt — SCD Tipo 2 (Exemplo)

## O que é?
Mini-projeto dbt que demonstra **SCD Tipo 2** usando `dbt snapshot`.

## Como funciona?

### 1. Carregar dados do Dia 1
```bash
dbt seed          # carrega clientes_dia1.csv
dbt snapshot      # cria o snapshot (carga inicial)
dbt run           # materializa dim_clientes
dbt test          # valida regras
```

### 2. Simular mudanças (Dia 2)
Edite `models/staging/stg_clientes.sql` e troque:
```sql
FROM {{ ref('clientes_dia1') }}
```
por:
```sql
FROM {{ ref('clientes_dia2') }}
```

Depois rode novamente:
```bash
dbt seed          # carrega clientes_dia2.csv
dbt snapshot      # detecta mudanças e aplica SCD Tipo 2
dbt run           # atualiza dim_clientes
dbt test          # valida
```

### 3. Resultado esperado
A tabela `dim_clientes` terá:
- **Ana Silva**: 2 linhas (endereço antigo inativo + novo ativo)
- **Carla Souza**: 2 linhas (preco_score antigo inativo + novo ativo)
- **Fabio Rocha**: 1 linha (cliente novo, ativo)
- **Bruno, Diego, Eva**: 1 linha cada (sem mudanças)

## Estrutura
```
dbt_scd2_exemplo/
├── dbt_project.yml
├── profiles.yml
├── seeds/
│   ├── clientes_dia1.csv
│   └── clientes_dia2.csv
├── models/
│   ├── staging/
│   │   └── stg_clientes.sql
│   └── marts/
│       └── dim_clientes.sql
├── snapshots/
│   └── dim_clientes_snapshot.sql
└── tests/
    └── test_scd2_um_ativo_por_cliente.sql
```
"""
    (DBT_DIR / "README.md").write_text(readme, encoding="utf-8")

    print(f"\n  Projeto dbt gerado em: {DBT_DIR}")
    print("  Arquivos criados:")
    for pasta in pastas:
        for arq in sorted(pasta.iterdir()):
            if arq.is_file():
                print(f"    {arq.relative_to(DBT_DIR)}")

    print("\n  Para executar:")
    print(f"    cd {DBT_DIR}")
    print("    dbt seed && dbt snapshot && dbt run && dbt test")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN — Executa ambas as partes
# ═══════════════════════════════════════════════════════════════════════════

def executar() -> None:
    executar_sql_puro()
    gerar_projeto_dbt()
    print("\n" + "=" * 90)
    print("  CONCLUÍDO — Ambas as abordagens demonstradas!")
    print("=" * 90)


if __name__ == "__main__":
    executar()
