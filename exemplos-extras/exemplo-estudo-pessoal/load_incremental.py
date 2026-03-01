"""
LOAD INCREMENTAL (SCD Tipo 1 e SCD Tipo 2)

Este módulo implementa duas estratégias de carga incremental:
- `carga_incremental_scd1`: sobrescreve registros (SCD Tipo 1)
- `carga_incremental_scd2`: expira registros antigos e cria novas linhas (SCD Tipo 2)

Mantemos nomes em português e preferimos nomes descritivos para variáveis e funções.
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import argparse

# Importar funções utilitárias do módulo de full load
from full_load import limpar_e_enriquecer_clientes
from gerador_dados import gerar_clientes


def carga_incremental_scd1(df_novos_dados, engine):
    """SCD Tipo 1: atualiza registros existentes e insere novos (sem histórico)."""
    df_novos = limpar_e_enriquecer_clientes(df_novos_dados)

    try:
        df_atual = pd.read_sql_table('dimensao_cliente_scd1', engine)
    except ValueError:
        # Se a tabela não existir, criamos a tabela inicial
        df_novos.to_sql('dimensao_cliente_scd1', engine, if_exists='replace', index=False)
        print('Carga inicial SCD1 concluída (tabela criada).')
        return

    # Identificar novos e existentes
    df_merged = pd.merge(df_novos, df_atual[['id_cliente_origem']], on='id_cliente_origem', how='left', indicator=True, validate="many_to_many")

    novos_registros = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    registros_existentes = df_merged[df_merged['_merge'] == 'both'].drop(columns=['_merge'])

    # Atualizamos: removemos os existentes antigos e re-anexamos as versões novas
    ids_para_atualizar = registros_existentes['id_cliente_origem'].tolist()
    df_mantidos = df_atual[~df_atual['id_cliente_origem'].isin(ids_para_atualizar)]

    df_final = pd.concat([df_mantidos, registros_existentes, novos_registros], ignore_index=True)
    df_final.to_sql('dimensao_cliente_scd1', engine, if_exists='replace', index=False)
    print('Carga incremental SCD1 concluída: registros atualizados e novos inseridos.')


def carga_incremental_scd2(df_novos_dados, engine):
    """SCD Tipo 2: expira registros antigos e insere novas versões, mantendo histórico."""
    df_novos = limpar_e_enriquecer_clientes(df_novos_dados)
    
    hoje = pd.Timestamp.now().normalize()
    data_fim_padrao = pd.Timestamp('2099-12-31')

    try:
        df_atual = pd.read_sql_table('dimensao_cliente', engine)
    except ValueError:
        # Se não existir a tabela de dimensão, criamos uma carga inicial com chaves surrogadas
        df_novos = df_novos.reset_index(drop=True)
        df_novos['sk_cliente'] = range(1, len(df_novos) + 1)
        df_novos['data_inicio'] = hoje
        df_novos['data_fim'] = data_fim_padrao
        df_novos['is_ativo'] = True
        df_novos.to_sql('dimensao_cliente', engine, if_exists='replace', index=False)
        print('Carga inicial SCD2 concluída (tabela dimensao_cliente criada).')
        return

    df_atual['data_inicio'] = pd.to_datetime(df_atual['data_inicio'], errors='coerce')
    df_atual['data_fim'] = pd.to_datetime(df_atual['data_fim'], errors='coerce')


    # Filtrar registros ativos
    df_ativos = df_atual[df_atual.get('is_ativo', True) == True]

    # Preparar comparação (merge) entre novos dados e os ativos
    df_compare = pd.merge(df_novos, df_ativos, on='id_cliente_origem', suffixes=('_novo', '_atual'), how='left', validate='many_to_one')

    # Identificar novos clientes (sem sk_cliente ativo) e possíveis atualizacoes
    novos = df_compare[df_compare['sk_cliente'].isna()].copy()
    potencialmente_atualizados = df_compare[df_compare['sk_cliente'].notna()].copy()

    condicao_mudanca = (
        (potencialmente_atualizados['nome_novo'] != potencialmente_atualizados['nome_atual']) |
        (potencialmente_atualizados['cidade_novo'] != potencialmente_atualizados['cidade_atual']) |
        (potencialmente_atualizados.get('email_novo') != potencialmente_atualizados.get('email_atual'))
    )

    registros_alterados = potencialmente_atualizados[condicao_mudanca].copy()

    max_sk = int(df_atual['sk_cliente'].max()) if not df_atual.empty else 0
    linhas_para_inserir = []

    # Expirar registros alterados e preparar novas linhas
    for _, row in registros_alterados.iterrows():
        idx_atual = df_atual[(df_atual['sk_cliente'] == row['sk_cliente'])].index
        df_atual.loc[idx_atual, 'is_ativo'] = False
        df_atual.loc[idx_atual, 'data_fim'] = hoje
        
        max_sk += 1
        nova_linha = {
            'sk_cliente': max_sk,
            'id_cliente_origem': row['id_cliente_origem'],
            'nome': row['nome_novo'],
            'cidade': row['cidade_novo'],
            'email': row.get('email_novo', None),
            'dominio_email': row.get('dominio_email_novo', 'desconhecido'),
            'data_cadastro': row.get('data_cadastro_novo', pd.NaT),
            'data_inicio': hoje,
            'data_fim': pd.to_datetime('2099-12-31').date(),
            'is_ativo': True
        }
        linhas_para_inserir.append(nova_linha)

    # Preparar novas linhas para clientes totalmente novos
    for _, row in novos.iterrows():
        max_sk += 1
        nova_linha = {
            'sk_cliente': max_sk,
            'id_cliente_origem': row['id_cliente_origem'],
            'nome': row['nome_novo'] if 'nome_novo' in row else row.get('nome'),
            'cidade': row['cidade_novo'] if 'cidade_novo' in row else row.get('cidade'),
            'email': row.get('email_novo', None),
            'dominio_email': row.get('dominio_email_novo', 'desconhecido'),
            'data_cadastro': row.get('data_cadastro_novo', pd.NaT),
            'data_inicio': hoje,
            'data_fim': pd.to_datetime('2099-12-31').date(),
            'is_ativo': True
        }
        linhas_para_inserir.append(nova_linha)

    if linhas_para_inserir:
        df_novas_linhas = pd.DataFrame(linhas_para_inserir)
        df_atual = pd.concat([df_atual, df_novas_linhas], ignore_index=True)

    # Gravar resultado final na tabela dimensao_cliente
    df_atual.to_sql('dimensao_cliente', engine, if_exists='replace', index=False)
    print('Carga incremental SCD2 concluída: histórico mantido e novas linhas inseridas.')


def main(total: int = 500, seed: int | None = None,
         db_user: str = 'usuario', db_pass: str = 'senha', db_host: str = 'localhost', db_port: str = '5432', db_name: str = 'datawarehouse', tipo: str = 'ambos'):
    """Gera dados mock e executa a carga incremental (SCD1, SCD2 ou ambos)."""
    df_clientes = gerar_clientes(n=total, seed=seed)

    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    if tipo in ('scd1', 'ambos'):
        carga_incremental_scd1(df_clientes, engine)
    if tipo in ('scd2', 'ambos'):
        carga_incremental_scd2(df_clientes, engine)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Executa carga incremental (SCD1 / SCD2) usando dados gerados')
    parser.add_argument('-n', '--total', type=int, default=100, help='Número de registros a gerar (padrão: 100)')
    parser.add_argument('--seed', type=int, default=None, help='Semente RNG (opcional)')
    parser.add_argument('--db-user', type=str, default='postgres')
    parser.add_argument('--db-pass', type=str, default='postgres')
    parser.add_argument('--db-host', type=str, default='localhost')
    parser.add_argument('--db-port', type=str, default='5432')
    parser.add_argument('--db-name', type=str, default='postgres')
    parser.add_argument('--tipo', type=str, choices=['scd1', 'scd2', 'ambos'], default='ambos', help='Tipo de carga incremental a executar')
    args = parser.parse_args()

    main(total=args.total, seed=args.seed,
         db_user=args.db_user, db_pass=args.db_pass, db_host=args.db_host, db_port=args.db_port, db_name=args.db_name, tipo=args.tipo)