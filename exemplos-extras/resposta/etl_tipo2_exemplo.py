from datetime import datetime
from pathlib import Path
import sqlite3

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
ARQ_DB = BASE_DIR / "meu_dw_tipo2.db"


def conectar_dw_tipo2() -> sqlite3.Connection:
    return sqlite3.connect(ARQ_DB)


def criar_tabelas_tipo2(conexao: sqlite3.Connection) -> None:
    cursor = conexao.cursor()

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


def carregar_staging_clientes(conexao: sqlite3.Connection, df_clientes: pd.DataFrame) -> None:
    df_clientes.to_sql("stg_clientes", conexao, if_exists="replace", index=False)


def aplicar_scd_tipo_2_clientes(conexao: sqlite3.Connection) -> None:
    """ETL SCD Tipo 2 isolado: preserva histórico de endereço."""
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


def consultar_dim_clientes(conexao: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT id_cliente, nome, endereco, is_current, dt_inicio
          FROM dim_clientes
         ORDER BY id_cliente, dt_inicio
        """,
        conexao,
    )


def executar_exemplo_tipo2() -> pd.DataFrame:
    if ARQ_DB.exists():
        ARQ_DB.unlink()

    clientes_carga_inicial = pd.DataFrame(
        [
            {"id_cliente": 1, "nome": "Ana Silva", "endereco": "Rua A, 100"},
            {"id_cliente": 2, "nome": "Bruno Costa", "endereco": "Rua B, 200"},
            {"id_cliente": 3, "nome": "Carla Souza", "endereco": "Rua C, 300"},
        ]
    )

    clientes_carga_alterada = pd.DataFrame(
        [
            {"id_cliente": 1, "nome": "Ana Silva", "endereco": "Rua A, 100"},
            {"id_cliente": 2, "nome": "Bruno Costa", "endereco": "Avenida Nova, 500"},
            {"id_cliente": 3, "nome": "Carla Souza", "endereco": "Rua C, 300"},
            {"id_cliente": 4, "nome": "Diego Lima", "endereco": "Rua D, 400"},
        ]
    )

    with conectar_dw_tipo2() as conexao:
        criar_tabelas_tipo2(conexao)

        carregar_staging_clientes(conexao, clientes_carga_inicial)
        aplicar_scd_tipo_2_clientes(conexao)

        carregar_staging_clientes(conexao, clientes_carga_alterada)
        aplicar_scd_tipo_2_clientes(conexao)

        resultado = consultar_dim_clientes(conexao)

    print("===== DIM_CLIENTES - EXEMPLO SCD TIPO 2 =====")
    print(resultado.to_string(index=False))

    return resultado


if __name__ == "__main__":
    executar_exemplo_tipo2()
