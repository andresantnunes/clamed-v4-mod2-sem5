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
ARQ_DB = BD_DIR / "scd2_sqlite.db"

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
            sk_cli INTEGER PRIMARY KEY AUTOINCREMENT,
            id_cliente INTEGER,
            nome TEXT,
            endereco TEXT,
            is_current BOOLEAN,
            data_inicio DATE,
            data_fim DATE
        )
    """)
    conexao.commit() # commit - confirma as alterações feitas no banco de dados

# espero criar a tabelas de staging com os dados já recebidos 
def carregar_staging(conexao: sqlite3.Connection, df: pd.DataFrame) -> None:
    # create e replace/substitui a tabela de staging
    df.to_sql("stg_clientes", conexao, if_exists="replace", index=False)

# applicar alterações do tipo 2
def aplicar_scd_tipo2(conexao: sqlite3.Connection) -> None:
    data_agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor = conexao.cursor()

    # onde o Staging é igual a Dimensão, realizamos o desativamento da Dimensão

    cursor.execute(
        """
        UPDATE dim_clientes
           SET is_current = 0,
               data_fim   = ?
         WHERE is_current = 1
           AND EXISTS (
               SELECT 1
                 FROM stg_clientes stg
                WHERE stg.id_cliente = dim_clientes.id_cliente
                  AND (
                      stg.endereco    <> dim_clientes.endereco
                  )
           )
        """,
        (data_agora,)
    )# Jogar para dentro do comando SQL a data atual, onde tem o ?

    desativados = cursor.rowcount # rowcount - número de linhas afetadas pela última operação

    # onde o Staging é igual a Dimensão, 
    # realizamos a inserção de um novo registro na Dimensão
    cursor.execute(
        """
        INSERT INTO 
          dim_clientes (id_cliente, nome, endereco, is_current, data_inicio, data_fim)
        SELECT stg.id_cliente, stg.nome, stg.endereco,
               1, ?, '9999-12-31'
          FROM stg_clientes stg
          LEFT JOIN dim_clientes dim_ativo
            ON dim_ativo.id_cliente = stg.id_cliente
           AND dim_ativo.is_current = 1
         WHERE dim_ativo.id_cliente IS NULL
            OR dim_ativo.endereco    <> stg.endereco
        """,
        (data_agora,)
    )

    inserido = cursor.rowcount
    conexao.commit()
    # print de informações que não geraram problema
    print(f" Registros desativados: {desativados} | Registros inseridos: {inserido}")
    
def imprimir_dimensao(conexao: sqlite3.Connection, titulo: str = "") -> None:
    # Select dos dados da dimensão para mostrar o resultado
    df = pd.read_sql_query(
        "SELECT * FROM dim_clientes ORDER BY id_cliente, data_inicio", conexao
    )
    print(f"\n{'=' * 80}")
    print(f"  {titulo}" if titulo else "  dim_clientes")
    print(f"{'=' * 80}")
    if df.empty:
        print("  (vazia)")
    else:
        print(df.to_string(index=False))
    print("")


def imprimir_historico_cliente(conexao: sqlite3.Connection, id_cliente: int) -> None:
    df = pd.read_sql_query(
        "SELECT * FROM dim_clientes WHERE id_cliente = ? ORDER BY data_inicio",
        conexao,
        params=(id_cliente,),
    )
    print(f"\n--- Histórico do cliente ID={id_cliente} ---")
    if df.empty:
        print("  Nenhum registro encontrado.")
    else:
        print(df.to_string(index=False))

def executar() -> None:
    # Limpar banco anterior
    if ARQ_DB.exists():
        ARQ_DB.unlink()

    # 1. Gerar dados iniciais
    print("[1/4] Gerando dados iniciais (50 clientes)...")
    criar_dados_iniciais()
    df_inicial = pd.read_csv(ARQ_CLIENTES)

    with conectar() as conexao: # gerenciador de contexto
            # Não é necessário criar ou fechar a conexão manualmente, ela fecha automaticamente
            #  ao sair do bloco with

        # Criar a tabela de dimensão    
        criar_dimensao(conexao) 

        # 2. Carga inicial
        print("[2/4] Carga inicial — Staging + SCD Tipo 2...")
        carregar_staging(conexao, df_inicial)
        aplicar_scd_tipo2(conexao)
        imprimir_dimensao(conexao, "APÓS CARGA INICIAL")

        # 3. Gerar atualizações
        print("[3/4] Gerando 15 atualizações aleatórias...")
        gerar_mudancas()
        df_atualizado = pd.read_csv(ARQ_CLIENTES)

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
            SELECT id_cliente, COUNT(*) as versoes
              FROM dim_clientes
             GROUP BY id_cliente
            HAVING COUNT(*) > 1
             LIMIT 5
            """,
            conexao,
        )
        for _, row in df_multi.iterrows():
            imprimir_historico_cliente(conexao, int(row["id_cliente"]))

    print(f"Concluído! Banco salvo em: {ARQ_DB}")


if __name__ == "__main__":
    executar()