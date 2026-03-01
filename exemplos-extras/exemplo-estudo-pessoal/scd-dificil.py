import sqlite3
import pandas as pd
from datetime import datetime

# --- CONFIGURAÇÃO DO AMBIENTE (SIMULANDO O BIGQUERY) ---
# Criamos um banco de dados SQL em memória para este exemplo.
conn = sqlite3.connect(':memory:')
cursor = conn.cursor()

# 1. Criação da Tabela Dimensão Final (O nosso DW)
# Note as colunas de controle de SCD Tipo 2: sk_cliente, data_inicio, data_fim, status_ativo
cursor.execute('''
CREATE TABLE dim_cliente (
    sk_cliente INTEGER PRIMARY KEY AUTOINCREMENT, -- Surrogate Key (gerada pelo banco)
    id_cliente INTEGER,       -- Chave Natural (do sistema origem)
    nome TEXT,
    cidade TEXT,
    data_inicio DATE,
    data_fim DATE,
    status_ativo BOOLEAN
)
''')

# 2. Carga Inicial (Dia 1) - Povoando o DW
# No Dia 1, a Maria mora em São Paulo.
dia1_inserts = """
INSERT INTO dim_cliente (id_cliente, nome, cidade, data_inicio, data_fim, status_ativo) VALUES
(1, 'MARIA', 'SAO PAULO', '2023-01-01', '9999-12-31', 1),
(2, 'JOAO', 'BELO HORIZONTE', '2023-01-01', '9999-12-31', 1);
"""
cursor.executescript(dia1_inserts)
conn.commit()

print("--- ESTADO INICIAL DO DW (Dia 1) ---")
df_dw_dia1 = pd.read_sql("SELECT * FROM dim_cliente", conn)
display(df_dw_dia1)


# --- CHEGADA DE NOVOS DADOS (Dia 2) ---
# A Maria mudou para o Rio de Janeiro.
# Criamos uma tabela de STAGING para receber esses dados brutos.
cursor.execute('''
CREATE TABLE stg_cliente_dia2 (
    id_cliente INTEGER,
    nome TEXT,
    cidade TEXT
)
''')
dia2_staging_inserts = """
INSERT INTO stg_cliente_dia2 VALUES
(1, 'MARIA', 'RIO DE JANEIRO'), -- Mudança!
(2, 'JOAO', 'BELO HORIZONTE');   -- Sem mudança
"""
cursor.executescript(dia2_staging_inserts)
conn.commit()
print("\n--- DADOS QUE CHEGARAM NA STAGING (Dia 2) ---")
display(pd.read_sql("SELECT * FROM stg_cliente_dia2", conn))


# --- A MÁGICA DO SCD TIPO 2 COM SQL PURO (O jeito difícil!) ---
# Esta é a lógica complexa que o dbt automatiza para você com "Snapshots".
# Precisamos de uma transação para garantir que tudo ocorra ou nada ocorra.

hoje = datetime.now().strftime('%Y-%m-%d')

sql_scd2_complexo = f"""
BEGIN TRANSACTION;

-- PASSO 1: UPDATE (Fechar os registros antigos que mudaram)
-- Define status_ativo = 0 e data_fim = hoje para quem mudou de cidade.
UPDATE dim_cliente
SET status_ativo = 0,
    data_fim = '{hoje}'
WHERE id_cliente IN (
    -- Subquery para encontrar os IDs que mudaram comparando Staging vs DW Ativo
    SELECT stg.id_cliente
    FROM stg_cliente_dia2 stg
    JOIN dim_cliente dw ON stg.id_cliente = dw.id_cliente
    WHERE dw.status_ativo = 1 
      AND (stg.cidade <> dw.cidade OR stg.nome <> dw.nome)
);

-- PASSO 2: INSERT (Inserir as novas versões e novos clientes)
-- Seleciona da Staging e insere no DW como registros novos e ativos.
INSERT INTO dim_cliente (id_cliente, nome, cidade, data_inicio, data_fim, status_ativo)
SELECT 
    stg.id_cliente, 
    stg.nome, 
    stg.cidade, 
    '{hoje}' as data_inicio,
    '9999-12-31' as data_fim,
    1 as status_ativo
FROM stg_cliente_dia2 stg
LEFT JOIN dim_cliente dw 
    ON stg.id_cliente = dw.id_cliente AND dw.status_ativo = 1
-- Insere apenas se não existir um registro ativo idêntico no DW
WHERE dw.id_cliente IS NULL;

COMMIT;
"""

# Executando a lógica complexa
cursor.executescript(sql_scd2_complexo)
conn.commit()

# --- RESULTADO FINAL ---
print(f"\n--- ESTADO FINAL DO DW APÓS PROCESSAMENTO SCD TIPO 2 (Dia 2: {hoje}) ---")
print("Note que a Maria agora tem duas linhas: o histórico (SP) e o atual (RJ).")
df_dw_final = pd.read_sql("SELECT * FROM dim_cliente ORDER BY id_cliente, data_inicio", conn)
display(df_dw_final)

# Fechando a conexão
conn.close()