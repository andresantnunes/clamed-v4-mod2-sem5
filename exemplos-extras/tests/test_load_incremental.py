import pandas as pd
from sqlalchemy import create_engine

from load_incremental import carga_incremental_scd1, carga_incremental_scd2
from gerador_dados import gerar_clientes


def test_carga_incremental_scd2_cria_e_atualiza():
    engine = create_engine('sqlite:///:memory:')

    # Carga inicial
    df_inicial = gerar_clientes(n=3, seed=2)
    carga_incremental_scd2(df_inicial, engine)
    resultado = pd.read_sql_query('SELECT * FROM dim_cliente', engine)
    assert len(resultado) == 3

    # Simular atualização: alterar cidade do primeiro cliente
    df_atualizado = df_inicial.copy()
    df_atualizado.loc[0, 'cidade'] = 'CIDADE_ATUALIZADA'
    carga_incremental_scd2(df_atualizado, engine)

    resultado2 = pd.read_sql_query('SELECT * FROM dim_cliente', engine)
    # Deve haver uma linha adicional para a nova versão do cliente alterado
    assert len(resultado2) == 4


def test_carga_incremental_scd1_sobrescreve():
    engine = create_engine('sqlite:///:memory:')

    df_inicial = gerar_clientes(n=2, seed=3)
    carga_incremental_scd1(df_inicial, engine)
    resultado = pd.read_sql_query('SELECT * FROM dim_cliente_scd1', engine)
    assert len(resultado) == 2

    # Atualizar nome do primeiro cliente
    df_update = df_inicial.copy()
    df_update.loc[0, 'nome'] = 'NOME ATUALIZADO'
    carga_incremental_scd1(df_update, engine)

    resultado2 = pd.read_sql_query('SELECT * FROM dim_cliente_scd1', engine)
    assert len(resultado2) == 2
    # Conferir que o nome atualizado está presente (lembrando que limpeza padroniza para maiúsculas)
    nomes = [n.upper() for n in resultado2['nome'].astype(str).tolist()]
    assert 'NOME ATUALIZADO' in nomes
