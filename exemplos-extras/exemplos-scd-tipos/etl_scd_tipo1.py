"""
ETL SCD Tipo 1 — Sobrescreve dados antigos sem manter histórico.

Utiliza o gerador_dados.py (do ecommerce-etl) para gerar e atualizar clientes.
Armazena os dados em um banco SQLite local (scd1.db).

Fluxo:
  1. Gera dados iniciais com gerar_dados_iniciais()
  2. Carrega na staging (stg_clientes)
  3. Aplica SCD Tipo 1 na dimensão (dim_clientes): UPDATE + INSERT
  4. Gera atualizações com gerar_updates()
  5. Recarrega staging e reaplicar SCD Tipo 1
  6. Imprime resultado final
"""

import sqlite3
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
ARQ_DB = BASE_DIR / "scd1.db"


def conectar() -> sqlite3.Connection:
    return sqlite3.connect(ARQ_DB)


# ---------------------------------------------------------------------------
# Criação da dimensão
# ---------------------------------------------------------------------------
def criar_dimensao(conexao: sqlite3.Connection) -> None:
    """Cria a tabela dim_clientes para SCD Tipo 1 (sem colunas de histórico)."""
    conexao.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_clientes (
            ID INTEGER PRIMARY KEY,
            Nome TEXT NOT NULL,
            Endereco TEXT NOT NULL,
            Preco_Score REAL NOT NULL
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
# SCD Tipo 1
# ---------------------------------------------------------------------------
def aplicar_scd_tipo1(conexao: sqlite3.Connection) -> None:
    """
    SCD Tipo 1: sobrescreve registros existentes e insere novos.
    - UPDATE quando Nome, Endereco ou Preco_Score mudaram.
    - INSERT quando o ID não existe na dimensão.
    """
    cursor = conexao.cursor()

    # 1. Atualizar registros existentes que sofreram alteração
    cursor.execute(
        """
        UPDATE dim_clientes
           SET Nome        = (SELECT stg.Nome FROM stg_clientes stg
                               WHERE stg.ID = dim_clientes.ID),
               Endereco    = (SELECT stg.Endereco FROM stg_clientes stg
                               WHERE stg.ID = dim_clientes.ID),
               Preco_Score = (SELECT stg.Preco_Score FROM stg_clientes stg
                               WHERE stg.ID = dim_clientes.ID)
         WHERE ID IN (SELECT ID FROM stg_clientes)
           AND (
               Nome        <> (SELECT stg.Nome FROM stg_clientes stg
                                WHERE stg.ID = dim_clientes.ID)
            OR Endereco    <> (SELECT stg.Endereco FROM stg_clientes stg
                                WHERE stg.ID = dim_clientes.ID)
            OR Preco_Score <> (SELECT stg.Preco_Score FROM stg_clientes stg
                                WHERE stg.ID = dim_clientes.ID)
           )
        """
    )
    atualizados = cursor.rowcount

    # 2. Inserir registros novos (IDs que não existem na dimensão)
    cursor.execute(
        """
        INSERT INTO dim_clientes (ID, Nome, Endereco, Preco_Score)
        SELECT stg.ID, stg.Nome, stg.Endereco, stg.Preco_Score
          FROM stg_clientes stg
          LEFT JOIN dim_clientes dim ON dim.ID = stg.ID
         WHERE dim.ID IS NULL
        """
    )
    inseridos = cursor.rowcount

    conexao.commit()
    print(f"  SCD Tipo 1 — Atualizados: {atualizados} | Inseridos: {inseridos}")


# ---------------------------------------------------------------------------
# Exibição
# ---------------------------------------------------------------------------
def imprimir_dimensao(conexao: sqlite3.Connection, titulo: str = "") -> None:
    df = pd.read_sql_query(
        "SELECT * FROM dim_clientes ORDER BY ID", conexao
    )
    print(f"\n{'=' * 60}")
    print(f"  {titulo}" if titulo else "  dim_clientes")
    print(f"{'=' * 60}")
    if df.empty:
        print("  (vazia)")
    else:
        print(df.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# Execução completa
# ---------------------------------------------------------------------------
def executar() -> None:
    # Limpar banco anterior para demonstração limpa
    if ARQ_DB.exists():
        ARQ_DB.unlink()

    # 1. Gerar dados iniciais (CSV criado pelo gerador)
    print("[1/4] Gerando dados iniciais (100 clientes)...")
    df_inicial = gerar_dados_iniciais(num_registros=100)

    with conectar() as conexao:
        criar_dimensao(conexao)

        # 2. Carga inicial
        print("[2/4] Carga inicial — Staging + SCD Tipo 1...")
        carregar_staging(conexao, df_inicial)
        aplicar_scd_tipo1(conexao)
        imprimir_dimensao(conexao, "APÓS CARGA INICIAL (primeiros registros)")

        # 3. Gerar atualizações
        print("[3/4] Gerando 20 atualizações aleatórias...")
        df_atualizado = gerar_updates(df_inicial.copy(), num_updates=20)

        # 4. Carga incremental SCD1
        print("[4/4] Carga incremental — Staging + SCD Tipo 1...")
        carregar_staging(conexao, df_atualizado)
        aplicar_scd_tipo1(conexao)
        imprimir_dimensao(conexao, "APÓS ATUALIZAÇÃO SCD TIPO 1")

    print("Concluído! Banco salvo em:", ARQ_DB)


if __name__ == "__main__":
    executar()
