import pandas as pd
from sqlalchemy import create_engine

from full_load import full_load_dim_cliente, full_load_fato_vendas
from gerador_dados import gerar_clientes, gerar_vendas


def test_full_load_dim_e_fato():
    # Engine em memória (SQLite) para testes rápidos e isolados
    engine = create_engine('sqlite:///:memory:')

    # Gerar dados de exemplo
    df_clientes = gerar_clientes(n=5, seed=1)
    df_vendas = gerar_vendas(n_vendas=5, n_clientes=5, sementes=1)

    # Executar full load da dimensão
    full_load_dim_cliente(df_clientes, engine)
    resultado_dim = pd.read_sql_query('SELECT COUNT(*) as c FROM dimensao_cliente', engine)
    assert int(resultado_dim['c'][0]) == len(df_clientes)

    # Executar full load da fato
    full_load_fato_vendas(df_vendas, df_clientes, engine)
    resultado_fato = pd.read_sql_query('SELECT COUNT(*) as c FROM fato_vendas', engine)
    assert int(resultado_fato['c'][0]) == len(df_vendas)
