# gerador_dados.py

import pandas as pd
import random
import faker

# Initialize the Faker library to generate fake data
fake = faker.Faker()

def gerar_dados_iniciais(num_registros=100):
    """
    Gera dados iniciais fictícios para clientes em um e-commerce.
    
    Args:
        num_registros (int): Número de registros a serem gerados.
    
    Returns:
        DataFrame: Um DataFrame contendo os dados dos clientes.
    """
    dados = {
        'ID': [i for i in range(1, num_registros + 1)],
        'Nome': [fake.name() for _ in range(num_registros)],
        'Endereco': [fake.address().replace('\n', ', ') for _ in range(num_registros)],
        'Preco_Score': [random.uniform(10.0, 100.0) for _ in range(num_registros)]
    }
    
    df_clientes = pd.DataFrame(dados)
    df_clientes.to_csv('clientes.csv', index=False)
    print("Arquivo 'clientes.csv' gerado com sucesso!")
    return df_clientes

def gerar_updates(df_clientes, num_updates=10):
    """
    Gera atualizações nos dados existentes para simular o comportamento de um sistema OLTP.
    
    Args:
        df_clientes (DataFrame): DataFrame contendo os dados dos clientes.
        num_updates (int): Número de atualizações a serem geradas.
    
    Returns:
        DataFrame: Um DataFrame atualizado com as modificações.
    """
    for _ in range(num_updates):
        # Seleciona um cliente aleatório para atualizar
        index = random.randint(0, len(df_clientes) - 1)
        df_clientes.at[index, 'Endereco'] = fake.address().replace('\n', ', ')
        df_clientes.at[index, 'Preco_Score'] = random.uniform(10.0, 100.0)
    
    df_clientes.to_csv('clientes_atualizados.csv', index=False)
    print("Arquivo 'clientes_atualizados.csv' gerado com atualizações!")
    return df_clientes

if __name__ == "__main__":
    # Gera dados iniciais
    df_clientes = gerar_dados_iniciais(100)
    # Gera atualizações nos dados
    df_clientes_atualizados = gerar_updates(df_clientes, 10)