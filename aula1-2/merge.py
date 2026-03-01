import pandas as pd


# tem apena o id_cliente_origem - ID natural
# tem valor de negócio
df_novos = pd.DataFrame([
    {
        'id_cliente_origem': 1, 
        'nome': 'Ana',   
        'cidade': 'SP', 
        'email': 'ana@example.com'
    },
    {'id_cliente_origem': 2, 'nome': 'Bruno', 'cidade': 'RJ', 'email': 'bruno@ex.com'},
    {'id_cliente_origem': 3, 'nome': 'Carlos','cidade': 'BH', 'email': 'carlos@ex.com'}
])

# tem o sk_cliente - ID Surrogate
# id artificial
df_ativos = pd.DataFrame([
    {
        'sk_cliente': 10, 
        'id_cliente_origem': 1, 
        'nome': 'Ana',   
        'cidade': 'SP', 
        'email': 'ana@old.com'
    },
    {'sk_cliente': 11, 'id_cliente_origem': 2, 'nome': 'Bruno', 'cidade': 'Niteroi', 'email': 'bruno@ex.com'}
])


df_comparado = pd.merge(
    df_novos, 
    df_ativos, 
    on='id_cliente_origem', # a chave de junção 
    how='left', 
    suffixes=('_novo', '_ativo'),
    validate='one_to_many'
)

df_comparado_outer = pd.merge(
    df_novos, 
    df_ativos, 
    on='id_cliente_origem', # a chave de junção 
    how='outer', 
    suffixes=('_novo', '_ativo')
)


print("df_novos:\n", df_novos, "\n")
print("df_ativos:\n", df_ativos, "\n")
print("df_comparado (resultado do merge):\n", df_comparado, "\n")
print("df_comparado_outer (resultado do merge outer):\n", df_comparado_outer)


# 2                  3    Carlos          BH    carlos@ex.com         NaN        NaN          NaN           NaN
# tira a linha que tem NA na SK_CLIENTE, ou seja, os itens que não tem correspondencia no df_ativos
itens_novo = df_comparado[df_comparado['sk_cliente'].isna()].copy()

# itens que existem tanto no df_novos quanto no df_ativos, ou seja, os itens que tem correspondencia
pontencialmente_atualizados = df_comparado[df_comparado['sk_cliente'].notna()].copy()

mudancas = (
    ( pontencialmente_atualizados["nome_novo"] != pontencialmente_atualizados["nome_ativo"]) |
    ( pontencialmente_atualizados["cidade_novo"] != pontencialmente_atualizados["cidade_ativo"]) |
    ( pontencialmente_atualizados["email_novo"] != pontencialmente_atualizados["email_ativo"])
)

if mudancas.any():
    itens_atualizados = pontencialmente_atualizados[mudancas].copy()

ids_alterados = itens_atualizados['id_cliente_origem'].tolist()

# 11
max_sk = int(df_ativos["sk_cliente"].max()) if not df_ativos.empty else 0

if empty := itens_atualizados.empty:
    print("Não há itens atualizados.")
# daqui para frente teremos itens atualizados

for _, row in itens_atualizados.iterrows():
    # para cada item em itens_atualizados, 
    # vamos expirar o item antigo 
    # criar um novo item
    # salvar ambos 
    idx_atual = df_ativos[df_ativos["sk_cliente"] == row["sk_cliente"]].index

    # loc - acessar o dataframe por rótulo (label) ou por uma condição booleana
    # busca no dataframe 
    df_ativos.loc[idx_atual, "is_ativo"] = False
    df_ativos.loc[idx_atual, "data_fim"] = pd.Timestamp.today()

    # Criar um novo campo
    max_sk += 1 # 12
    linha_nova = {
        "sk_cliente": max_sk,
        "id_cliente_origem": row["id_cliente_origem"],
        "nome": row.get("nome_novo"),
        "cidade": row.get("cidade_novo"),
        "email": row.get("email_novo")
    }

    df_ativos = pd.concat([df_ativos, pd.DataFrame([linha_nova])], ignore_index=True)
    
for _, row in itens_novo.iterrows():
    max_sk += 1
    linha_nova = {
        "sk_cliente": max_sk,
        "id_cliente_origem": row["id_cliente_origem"],
        "nome": row.get("nome_novo"),
        "cidade": row.get("cidade_novo"),
        "email": row.get("email_novo")
    }
    df_ativos = pd.concat([df_ativos, pd.DataFrame([linha_nova])], ignore_index=True)

print("itens_novo:\n", itens_novo, "\n")
print("itens_atualizados:\n", itens_atualizados, "\n")  
print("df_ativos atualizado:\n", df_ativos, "\n")