# =====================================================================
# 2. TRANSFORMAÇÃO E FULL LOAD
# =====================================================================
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import argparse
from gerador_dados import gerar_clientes, gerar_vendas


def limpar_e_enriquecer_clientes(df):
    """Limpeza e enriquecimento usando Pandas."""
    df_clean = df.copy()
    # Padronizar nomes para maiúsculo
    if 'nome' in df_clean.columns:
        df_clean['nome'] = df_clean['nome'].str.upper()
    # Enriquecimento: Criar flag de domínio de email
    if 'email' in df_clean.columns:
        df_clean['dominio_email'] = df_clean['email'].apply(lambda x: x.split('@')[1] if '@' in x else 'desconhecido')
    else:
        df_clean['dominio_email'] = 'desconhecido'
    # Converter datas
    if 'data_cadastro' in df_clean.columns:
        df_clean['data_cadastro'] = pd.to_datetime(df_clean['data_cadastro'])
    else:
        df_clean['data_cadastro'] = pd.NaT
    return df_clean


def full_load_dim_cliente(df_clientes, engine):
    """Cria a Dimensão Cliente para o SCD Tipo 2 no Full Load."""
    df_dim = limpar_e_enriquecer_clientes(df_clientes)
    
    # Adicionando colunas de controle para SCD
    df_dim = df_dim.reset_index(drop=True)
    df_dim['sk_cliente'] = range(1, len(df_dim) + 1)  # Surrogate Key
    df_dim['data_inicio'] = datetime.now().date()
    df_dim['data_fim'] = pd.to_datetime('2099-12-31').date()
    df_dim['is_ativo'] = True
    
    # Reordenando para deixar a Surrogate Key na frente
    # Garantir que a coluna id_cliente_origem exista
    if 'id_cliente_origem' not in df_dim.columns and 'id_cliente' in df_dim.columns:
        df_dim.rename(columns={'id_cliente': 'id_cliente_origem'}, inplace=True)

    cols = ['sk_cliente', 'id_cliente_origem', 'nome', 'cidade', 'email', 'dominio_email', 'data_cadastro', 'data_inicio', 'data_fim', 'is_ativo']
    # Filtrar colunas existentes e manter ordem desejada
    cols_existentes = [c for c in cols if c in df_dim.columns]
    df_dim = df_dim[cols_existentes]
    
    # Carga no banco de dados (Replace por ser Full Load)
    df_dim.to_sql('dimensao_cliente', engine, if_exists='replace', index=False)
    print("Full Load: Tabela dimensao_cliente criada com sucesso.")


def full_load_fato_vendas(df_vendas, df_clientes, engine):
    """Cria e carrega a Fato Vendas."""
    # Buscar a Surrogate Key da dimensão atual
    df_dim = pd.read_sql_table('dimensao_cliente', engine)
    
    # Merge para pegar a sk_cliente
    if 'id_cliente_origem' not in df_vendas.columns and 'id_cliente' in df_vendas.columns:
        df_vendas = df_vendas.rename(columns={'id_cliente': 'id_cliente_origem'})

    df_fato = pd.merge(
        df_vendas, 
        df_dim[['id_cliente_origem', 'sk_cliente']], 
        on='id_cliente_origem', 
        how='left', 
        validate='many_to_one'
        )
    
    # Selecionando colunas finais da fato
    desired_cols = ['id_venda', 'sk_cliente', 'valor', 'data_venda']
    cols_final = [c for c in desired_cols if c in df_fato.columns]
    df_fato = df_fato[cols_final]
    if 'data_venda' in df_fato.columns:
        df_fato['data_venda'] = pd.to_datetime(df_fato['data_venda'])
    
    df_fato.to_sql('fato_vendas', engine, if_exists='replace', index=False)
    print("Full Load: Tabela fato_vendas criada com sucesso.")


def main(total: int = 500, seed: int | None = None,
         db_user: str = 'postgres', db_pass: str = 'postgres', db_host: str = 'localhost', db_port: str = '5432', db_name: str = 'postgres'):
    """Gera dados usando `gerador_dados` e executa o Full Load no Postgres local."""
    # Gerar dados (em memória)
    df_clientes = gerar_clientes(n=total, seed=seed)
    df_vendas = gerar_vendas(n_vendas=total, n_clientes=total, sementes=seed)

    # Conectar no Postgres local (Data Warehouse)
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    # Executar full loads
    full_load_dim_cliente(df_clientes, engine)
    full_load_fato_vendas(df_vendas, df_clientes, engine)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Executa Full Load usando dados gerados localmente')
    parser.add_argument('-n', '--total', type=int, default=500, help='Número de registros a gerar (padrão: 500)')
    parser.add_argument('--seed', type=int, default=None, help='Semente RNG (opcional)')
    parser.add_argument('--db-user', type=str, default='postgres')
    parser.add_argument('--db-pass', type=str, default='postgres')
    parser.add_argument('--db-host', type=str, default='localhost')
    parser.add_argument('--db-port', type=str, default='5432')
    parser.add_argument('--db-name', type=str, default='postgres')
    args = parser.parse_args()

    main(total=args.total, seed=args.seed,
         db_user=args.db_user, db_pass=args.db_pass, db_host=args.db_host, db_port=args.db_port, db_name=args.db_name)