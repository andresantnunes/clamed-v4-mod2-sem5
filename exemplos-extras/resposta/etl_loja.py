from datetime import datetime
from pathlib import Path
import sqlite3

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ARQ_PRODUTOS = DATA_DIR / "produtos.csv"
ARQ_CLIENTES = DATA_DIR / "clientes.csv"
ARQ_DB = BASE_DIR / "meu_dw.db"


def conectar_dw() -> sqlite3.Connection:
    return sqlite3.connect(ARQ_DB)


def criar_dimensoes(conexao: sqlite3.Connection) -> None:
    """Parte 2 - cria tabelas de dimensão no DW local."""
    cursor = conexao.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_produtos (
            id_produto INTEGER PRIMARY KEY,
            nome TEXT NOT NULL,
            preco REAL NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_clientes (
            id_cliente INTEGER NOT NULL,
            nome TEXT NOT NULL,
            endereco TEXT NOT NULL,
            is_current INTEGER NOT NULL,
            dt_inicio TEXT NOT NULL
        )
        """
    )

    conexao.commit()


def carregar_staging(conexao: sqlite3.Connection) -> None:
    """Parte 3 - carrega CSV em tabelas stg_* via Pandas."""
    if not ARQ_PRODUTOS.exists() or not ARQ_CLIENTES.exists():
        raise FileNotFoundError(
            "CSVs não encontrados. Rode primeiro: python gerador.py"
        )

    df_produtos = pd.read_csv(ARQ_PRODUTOS)
    df_clientes = pd.read_csv(ARQ_CLIENTES)

    df_produtos.to_sql("stg_produtos", conexao, if_exists="replace", index=False)
    df_clientes.to_sql("stg_clientes", conexao, if_exists="replace", index=False)


def aplicar_scd_tipo_1_produtos(conexao: sqlite3.Connection) -> None:
    """Parte 4 - SCD Tipo 1: atualiza preço e insere novos produtos."""
    cursor = conexao.cursor()

    cursor.execute(
        """
        UPDATE dim_produtos
           SET nome = (SELECT stg.nome
                         FROM stg_produtos stg
                        WHERE stg.id_produto = dim_produtos.id_produto),
               preco = (SELECT stg.preco
                          FROM stg_produtos stg
                         WHERE stg.id_produto = dim_produtos.id_produto)
         WHERE id_produto IN (SELECT id_produto FROM stg_produtos)
           AND (
               preco <> (SELECT stg.preco
                           FROM stg_produtos stg
                          WHERE stg.id_produto = dim_produtos.id_produto)
            OR nome <> (SELECT stg.nome
                          FROM stg_produtos stg
                         WHERE stg.id_produto = dim_produtos.id_produto)
           )
        """
    )

    cursor.execute(
        """
        INSERT INTO dim_produtos (id_produto, nome, preco)
        SELECT stg.id_produto, stg.nome, stg.preco
          FROM stg_produtos stg
          LEFT JOIN dim_produtos dim
            ON dim.id_produto = stg.id_produto
         WHERE dim.id_produto IS NULL
        """
    )

    conexao.commit()


def aplicar_scd_tipo_2_clientes(conexao: sqlite3.Connection) -> None:
    """Parte 5 - SCD Tipo 2: preserva histórico de endereço."""
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor = conexao.cursor()

    cursor.execute(
        """
        UPDATE dim_clientes
           SET is_current = 0
         WHERE is_current = 1
           AND EXISTS (
               SELECT 1
                 FROM stg_clientes stg
                WHERE stg.id_cliente = dim_clientes.id_cliente
                  AND stg.endereco <> dim_clientes.endereco
           )
        """
    )

    cursor.execute(
        """
        INSERT INTO dim_clientes (id_cliente, nome, endereco, is_current, dt_inicio)
        SELECT stg.id_cliente, stg.nome, stg.endereco, 1, ?
          FROM stg_clientes stg
          LEFT JOIN dim_clientes dim_atual
            ON dim_atual.id_cliente = stg.id_cliente
           AND dim_atual.is_current = 1
         WHERE dim_atual.id_cliente IS NULL
            OR dim_atual.endereco <> stg.endereco
        """,
        (agora,),
    )

    conexao.commit()


def imprimir_dim_clientes(conexao: sqlite3.Connection) -> None:
    """Parte 6 - validação: imprime dim_clientes completa."""
    consulta = """
        SELECT id_cliente, nome, endereco, is_current, dt_inicio
          FROM dim_clientes
         ORDER BY id_cliente, dt_inicio
    """
    df = pd.read_sql_query(consulta, conexao)
    print("\n===== DIM_CLIENTES (VALIDAÇÃO) =====")
    if df.empty:
        print("Tabela dim_clientes está vazia.")
    else:
        print(df.to_string(index=False))


def executar_etl() -> None:
    with conectar_dw() as conexao:
        criar_dimensoes(conexao)
        carregar_staging(conexao)
        aplicar_scd_tipo_1_produtos(conexao)
        aplicar_scd_tipo_2_clientes(conexao)
        imprimir_dim_clientes(conexao)


if __name__ == "__main__":
    executar_etl()
