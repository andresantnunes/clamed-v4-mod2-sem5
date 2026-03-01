import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from logging import Logger

import pandas as pd
from gerador_dados_exercicio_exemplo import criar_dados_iniciais, gerar_mudancas 


log = Logger("etl_scd_t2_dataframe")

BASE_DIR = Path(__file__).resolve().parent
BD_DIR = BASE_DIR / "banco"
ARQ_DB = BD_DIR / "scd1.db"

DATA_DIR = BASE_DIR / "data"
ARQ_PRODUTOS = DATA_DIR / "produtos.csv"
ARQ_CLIENTES = DATA_DIR / "clientes.csv"

def conectar() -> sqlite3.Connection:
    # Para o SQLite só definimos o arquivo de destino
    return sqlite3.connect(ARQ_DB)

def criar_dimensao(conexao: sqlite3.Connection) -> None:
    """Cria a tabela de dimensão clientes."""
    cursor = conexao.cursor() # Cursos - executor de comandos sql
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_clientes (
            id_cliente INTEGER,
            nome TEXT,
            endereco TEXT,
            data_update DATE
        )
    """)
    conexao.commit()

def carregar_staging(conexao: sqlite3.Connection, df: pd.DataFrame) -> None:
    df.to_sql("stg_clientes", conexao, if_exists="replace", index=False)

def aplicar_scd_tipo1(conexao: sqlite3.Connection) -> None:
    cursor = conexao.cursor()
    data_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Atualizar registros existentes tanto no stage quanto na dimensão
    cursor.execute(
        """
        UPDATE dim_clientes
           SET 
               nome = (SELECT nome FROM stg_clientes WHERE stg_clientes.id_cliente = dim_clientes.id_cliente),
               endereco = (SELECT endereco FROM stg_clientes WHERE stg_clientes.id_cliente = dim_clientes.id_cliente),
               data_update = ?
         WHERE id_cliente IN (
            SELECT id_cliente FROM stg_clientes
         )
           AND (
               nome        <> (SELECT stg.nome FROM stg_clientes stg
                                WHERE stg.id_cliente = dim_clientes.id_cliente)
            OR endereco    <> (SELECT stg.endereco FROM stg_clientes stg
                                WHERE stg.id_cliente = dim_clientes.id_cliente)
        )
        """,
        (data_atual,)
    )
    conexao.commit()

    atualizados = cursor.rowcount
    print(f"{atualizados} registros atualizados na dimensão clientes.")

    # Inserir novos registros
    cursor.execute(
        """
        INSERT INTO dim_clientes (id_cliente, nome, endereco, data_update)
        SELECT id_cliente, nome, endereco, ?
          FROM stg_clientes
         WHERE id_cliente NOT IN (SELECT id_cliente FROM dim_clientes)
        """,
        (data_atual,)
    )

    insersoes = cursor.rowcount
    conexao.commit()
    print(f"{insersoes} novos registros inseridos na dimensão clientes.")

def imprimir_dimensao(conexao: sqlite3.Connection, titulo: str = "") -> None:
    df = pd.read_sql_query(
        "SELECT * FROM dim_clientes ORDER BY id_cliente", conexao
    )
    print(f"\n{'=' * 60}")
    print(f"  {titulo}" if titulo else "  dim_clientes")
    print(f"{'=' * 60}")
    if df.empty:
        print("  (vazia)")
    else:
        print(df.to_string(index=False))
    print()

def excutar_etl() -> None:
    if ARQ_DB.exists():
        ARQ_DB.unlink() # limpa o banco de dados

    with conectar() as conexao:
        criar_dimensao(conexao)

        # Gerar dados iniciais e mudanças
        criar_dados_iniciais()

        # Carregar dados do staging
        df_staging = pd.read_csv(ARQ_CLIENTES)
        carregar_staging(conexao, df_staging)
        aplicar_scd_tipo1(conexao)
        imprimir_dimensao(conexao, "APÓS CARGA INICIAL (primeiros registros)")

        # Timer de espera para simular o tempo passando
        input("\nPressione Enter para gerar mudanças e aplicar SCD Tipo 1...")

        gerar_mudancas()
        df_staging = pd.read_csv(ARQ_CLIENTES)
        carregar_staging(conexao, df_staging)

        # Aplicar SCD Tipo 1
        aplicar_scd_tipo1(conexao)

        # Imprimir resultado final
        imprimir_dimensao(conexao, "Dimensão Clientes - SCD Tipo 1")



if __name__ == "__main__":
    excutar_etl()
