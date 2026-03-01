# Exemplo de demonstração (execute no seu ambiente)
import pandas as pd

df_novos = pd.DataFrame([
    {'id_cliente_origem': 1, 'nome': 'Ana',   'cidade': 'SP', 'email': 'ana@example.com'},
    {'id_cliente_origem': 2, 'nome': 'Bruno', 'cidade': 'RJ', 'email': 'bruno@ex.com'},
    {'id_cliente_origem': 3, 'nome': 'Carlos','cidade': 'BH', 'email': 'carlos@ex.com'}
])

df_ativos = pd.DataFrame([
    {'sk_cliente': 10, 'id_cliente_origem': 1, 'nome': 'Ana',   'cidade': 'SP', 'email': 'ana@old.com'},
    {'sk_cliente': 11, 'id_cliente_origem': 2, 'nome': 'Bruno', 'cidade': 'Niteroi', 'email': 'bruno@ex.com'}
])

df_compare = pd.merge(
    df_novos, df_ativos,
    on='id_cliente_origem',
    suffixes=('_novo', '_atual'),
    how='left',
    validate='many_to_one'
)

print("df_novos:\n", df_novos, "\n")

print("df_novos:\n", df_novos, "\n")
print("df_ativos:\n", df_ativos, "\n")
print("df_compare (resultado do merge):\n", df_compare)