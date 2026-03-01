import pandas as pd
from sqlalchemy import create_engine

# 1. Extração
df_vendas = pd.read_csv('vendas_diarias.csv')

# 2. Transformação (Limpeza simples)
df_vendas['nome'] = df_vendas['nome'].str.upper() # Padroniza nomes para maiúsculo
df_vendas = df_vendas.drop_duplicates(subset=['id_cliente']) # Remove duplicatas

# 3. Carga (Full Load: substitui a tabela inteira)
engine = create_engine('postgresql://usuario:senha@localhost:5432/meu_dw')
df_vendas.to_sql('dim_cliente', engine, if_exists='replace', index=False)