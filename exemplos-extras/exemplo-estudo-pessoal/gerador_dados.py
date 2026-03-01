import argparse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# =====================================================================
# 0. CONFIGURAÇÃO DO BANCO DE DADOS (POSTGRESQL)
# =====================================================================
# Substitua pelos dados reais do seu PostgreSQL
DB_USER = 'usuario'
DB_PASS = 'senha'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'datawarehouse'

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# =====================================================================
# 1. GERAÇÃO E EXTRAÇÃO DE DADOS (MOCK, ESCALÁVEL)
# =====================================================================

def gerar_clientes(n=500, seed: int | None = None) -> pd.DataFrame:
    """Gera `n` clientes mock.

    - `id_cliente_origem`: 1..n
    - `nome`: combinação simples de nomes
    - `cidade`, `email`, `data_cadastro`: valores aleatórios plausíveis
    """
    if seed is not None:
        np.random.seed(seed)

    primeiros = [
        'João', 'Maria', 'Carlos', 'Ana', 'Pedro', 'Mariana', 'Lucas', 'Marcos', 'Fernanda', 'Patrícia'
    ]
    sobrenomes = [
        'Silva', 'Souza', 'Lima', 'Oliveira', 'Costa', 'Santos', 'Pereira', 'Almeida', 'Ribeiro', 'Moreira'
    ]
    cidades = ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Curitiba', 'Porto Alegre', 'Salvador', 'Fortaleza']

    ids = np.arange(1, n + 1)
    nomes = [f"{np.random.choice(primeiros)} {np.random.choice(sobrenomes)}" for _ in range(n)]
    cidades_choice = list(np.random.choice(cidades, size=n))
    emails = [f"{nome.lower().replace(' ', '.')}.{i}@exemplo.com" for i, nome in enumerate(nomes, start=1)]

    data_inicial = datetime(2020, 1, 1)
    dias = np.random.randint(0, 2000, size=n)
    datas = [ (data_inicial + timedelta(int(int_d))).strftime('%Y-%m-%d') for int_d in dias ]

    df = pd.DataFrame({
        'id_cliente_origem': ids,
        'nome': nomes,
        'cidade': cidades_choice,
        'email': emails,
        'data_cadastro': datas
    })
    return df


def gerar_vendas(n_vendas=500, n_clientes=500, sementes: int | None = None) -> pd.DataFrame:
    """Gera `n_vendas` registros de vendas associando-os aos clientes gerados."""
    if sementes is not None:
        np.random.seed(sementes + 1)

    id_venda_inicio = 1000
    ids = np.arange(id_venda_inicio, id_venda_inicio + n_vendas)
    gerado_random = np.random.Generator(np.random.PCG64(sementes))
    id_clientes = gerado_random.integers(1, n_clientes + 1, size=n_vendas)
    valores = np.round(gerado_random.uniform(5.0, 5000.0, size=n_vendas), 2)

    data_inicial = datetime(2020, 1, 1)
    dias = gerado_random.integers(0, 2000, size=n_vendas)
    datas = [ (data_inicial + timedelta(int(int_d))).strftime('%Y-%m-%d') for int_d in dias ]

    df = pd.DataFrame({
        'id_venda': ids,
        'id_cliente_origem': id_clientes,
        'valor': valores,
        'data_venda': datas
    })
    return df


def main(total: int = 500, save: bool = False, seed: int | None = None):
    df_clientes = gerar_clientes(n=total, seed=seed)
    df_vendas = gerar_vendas(n_vendas=total, n_clientes=total, sementes=seed)

    print(f"Gerados {len(df_clientes)} clientes e {len(df_vendas)} vendas")
    print(df_clientes.head(3).to_string(index=False))
    print(df_vendas.head(3).to_string(index=False))

    if save:
        df_clientes.to_csv('clientes_gerados.csv', index=False)
        df_vendas.to_csv('vendas_geradas.csv', index=False)
        print('Arquivos salvos: clientes_gerados.csv, vendas_geradas.csv')

    return df_clientes, df_vendas


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Gerador mock de dados (clientes e vendas)')
    parser.add_argument('-n', '--total', type=int, default=500, help='Número de registros a gerar (padrão: 500)')
    parser.add_argument('--save', action='store_true', help='Salvar CSVs gerados no diretório atual')
    parser.add_argument('--seed', type=int, default=None, help='Semente para RNG (opcional)')
    args = parser.parse_args()

    main(total=args.total, save=args.save, seed=args.seed)