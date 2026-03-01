"""
ETL SCD Tipo 2 — Versão SQLite (local).

Preserva o histórico de alterações criando novas linhas na dimensão.
Quando um cliente muda de Endereço ou Preco_Score, o registro antigo é
"fechado" (is_current = 0, data_fim = hoje) e um novo registro é inserido
com os dados atualizados (is_current = 1, data_fim = 9999-12-31).

Utiliza o gerador_dados.py (do ecommerce-etl) para gerar e atualizar clientes.

Fluxo:
  1. Gera dados iniciais com gerar_dados_iniciais()
  2. Carrega na staging (stg_clientes)
  3. Aplica SCD Tipo 2 na dimensão (dim_clientes)
  4. Gera atualizações com gerar_updates()
  5. Recarrega staging e reaplicar SCD Tipo 2
  6. Imprime resultado mostrando registros históricos
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
ARQ_DB = BASE_DIR / "scd2_sqlite.db"


def conectar() -> sqlite3.Connection:
    return sqlite3.connect(ARQ_DB)


# ---------------------------------------------------------------------------
# Criação da dimensão
# ---------------------------------------------------------------------------
def criar_dimensao(conexao: sqlite3.Connection) -> None:
    """
    Cria a tabela dim_clientes com colunas de controle SCD Tipo 2:
      - sk_cliente: Surrogate Key (auto-incremento)
      - ID: Chave natural vinda do sistema origem
      - is_current: 1 = registro ativo, 0 = registro histórico
      - data_inicio / data_fim: janela de validade do registro
    """
    conexao.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_clientes (
            sk_cliente  INTEGER PRIMARY KEY AUTOINCREMENT,
            ID          INTEGER NOT NULL,
            Nome        TEXT NOT NULL,
            Endereco    TEXT NOT NULL,
            Preco_Score REAL NOT NULL,
            is_current  INTEGER NOT NULL DEFAULT 1,
            data_inicio TEXT NOT NULL,
            data_fim    TEXT NOT NULL DEFAULT '9999-12-31'
        )
        """
    )
    conexao.commit()


# ---------------------------------------------------------------------------
# Staging
# ---------------------------------------------------------------------------
def carregar_staging(conexao: sqlite3.Connection, df: pd.DataFrame) -> None:
    """Carrega o DataFrame na tabela temporária stg_clientes."""
    df.to_sql("stg_clientes", conexao, if_exists="replace", index=False)


# ---------------------------------------------------------------------------
# SCD Tipo 2
# ---------------------------------------------------------------------------
def aplicar_scd_tipo2(conexao: sqlite3.Connection) -> None:
    """
    SCD Tipo 2:
      1. Fecha (is_current=0) registros ativos cujo Endereco ou Preco_Score
         divergem dos dados na staging.
      2. Insere novas versões para clientes alterados (nova linha ativa).
      3. Insere clientes totalmente novos (ID que não existia).
    """
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor = conexao.cursor()

    # ---- PASSO 1: Fechar registros que sofreram alteração ----
    cursor.execute(
        """
        UPDATE dim_clientes
           SET is_current = 0,
               data_fim   = ?
         WHERE is_current = 1
           AND EXISTS (
               SELECT 1
                 FROM stg_clientes stg
                WHERE stg.ID = dim_clientes.ID
                  AND (
                      stg.Endereco    <> dim_clientes.Endereco
                   OR stg.Preco_Score <> dim_clientes.Preco_Score
                  )
           )
        """,
        (agora,),
    )
    fechados = cursor.rowcount

    # ---- PASSO 2: Inserir novas versões (alterados + novos) ----
    cursor.execute(
        """
        INSERT INTO dim_clientes (ID, Nome, Endereco, Preco_Score, is_current, data_inicio, data_fim)
        SELECT stg.ID, stg.Nome, stg.Endereco, stg.Preco_Score,
               1, ?, '9999-12-31'
          FROM stg_clientes stg
          LEFT JOIN dim_clientes dim_ativo
            ON dim_ativo.ID = stg.ID
           AND dim_ativo.is_current = 1
         WHERE dim_ativo.ID IS NULL
            OR dim_ativo.Endereco    <> stg.Endereco
            OR dim_ativo.Preco_Score <> stg.Preco_Score
        """,
        (agora,),
    )
    inseridos = cursor.rowcount

    conexao.commit()
    print(f"  SCD Tipo 2 — Fechados: {fechados} | Inseridos: {inseridos}")


# ---------------------------------------------------------------------------
# Exibição
# ---------------------------------------------------------------------------
def imprimir_dimensao(conexao: sqlite3.Connection, titulo: str = "") -> None:
    df = pd.read_sql_query(
        "SELECT * FROM dim_clientes ORDER BY ID, data_inicio", conexao
    )
    print(f"\n{'=' * 80}")
    print(f"  {titulo}" if titulo else "  dim_clientes")
    print(f"{'=' * 80}")
    if df.empty:
        print("  (vazia)")
    else:
        print(df.to_string(index=False))
    print()


def imprimir_historico_cliente(conexao: sqlite3.Connection, id_cliente: int) -> None:
    """Mostra todas as versões de um cliente específico."""
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
# Execução completa
# ---------------------------------------------------------------------------
def executar() -> None:
    # Limpar banco anterior
    if ARQ_DB.exists():
        ARQ_DB.unlink()

    # 1. Gerar dados iniciais
    print("[1/4] Gerando dados iniciais (50 clientes)...")
    df_inicial = gerar_dados_iniciais(num_registros=50)

    with conectar() as conexao:
        criar_dimensao(conexao)

        # 2. Carga inicial
        print("[2/4] Carga inicial — Staging + SCD Tipo 2...")
        carregar_staging(conexao, df_inicial)
        aplicar_scd_tipo2(conexao)
        imprimir_dimensao(conexao, "APÓS CARGA INICIAL")

        # 3. Gerar atualizações
        print("[3/4] Gerando 15 atualizações aleatórias...")
        df_atualizado = gerar_updates(df_inicial.copy(), num_updates=15)

        # 4. Carga incremental SCD2
        print("[4/4] Carga incremental — Staging + SCD Tipo 2...")
        carregar_staging(conexao, df_atualizado)
        aplicar_scd_tipo2(conexao)
        imprimir_dimensao(conexao, "APÓS ATUALIZAÇÃO SCD TIPO 2")

        # Mostrar histórico de alguns clientes que provavelmente mudaram
        print("=" * 80)
        print("  EXEMPLOS DE HISTÓRICO (clientes com mais de uma versão)")
        print("=" * 80)
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
        for _, row in df_multi.iterrows():
            imprimir_historico_cliente(conexao, int(row["ID"]))

    print("Concluído! Banco salvo em:", ARQ_DB)


if __name__ == "__main__":
    executar()
