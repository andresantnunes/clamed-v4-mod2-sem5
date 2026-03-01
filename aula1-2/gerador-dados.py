import pandas as pd
import random
import faker

# gerador_dados - sneaky_case -> variaveis e funções
# gerador-dados - kebab-case -> arquivos, mas em geral não é recomendado para variáveis e funções
# GeradorDados - PascalCase -> classes
# geradorDados - camelCase -> variáveis e funções, mas não é tão comum em Python
# GERADOR_DADOS - SCREAMING_SNAKE_CASE -> constantes

# Permite criar dado falsos
# Faker é classe da  biblioteca faker
# o fake do lado esquerdo do igual é um  objeto
fake = faker.Faker()

NUMERO_REGISTROS_PADRAO = 1000

# Gerar um numero de registros que definimos
# Valor padrão de uma função
def gerador_dados(num_registros: int = NUMERO_REGISTROS_PADRAO) -> list:
    dados = [] # gerando os dados para uma lista em python nativo
    for _ in range(num_registros):

        registro = {
            'nome': fake.name(),
            'email': fake.email(),
            'endereco': fake.address(),
            'telefone': fake.phone_number(),
            'data_nascimento': fake.date_of_birth().strftime('%Y-%m-%d'),
            'idade': random.randint(18, 80)
        }

        dados.append(registro)
    return dados


def gerador_dados_pandas(num_registros: int = NUMERO_REGISTROS_PADRAO) -> pd.DataFrame:
    dados = { # Gera os dados direto no DataFrame
        'nome': [fake.name() for _ in range(num_registros)],
        'email': [fake.email() for _ in range(num_registros)],
        'endereco': [fake.address() for _ in range(num_registros)],
        'telefone': [fake.phone_number() for _ in range(num_registros)],
        'data_nascimento': [fake.date_of_birth().strftime('%Y-%m-%d') for _ in range(num_registros)],
        'idade': [random.randint(18, 80) for _ in range(num_registros)]
    }
    return pd.DataFrame(dados)


# Aula 3 - gerador de updated
def gerador_updates(num_registros: int = NUMERO_REGISTROS_PADRAO) -> pd.DataFrame:
    dados = {
        'id_cliente_origem': [random.randint(1, 1000) for _ in range(num_registros)],
        'nome': [fake.name() for _ in range(num_registros)],
        'cidade': [fake.city() for _ in range(num_registros)],
        'email': [fake.email() for _ in range(num_registros)]
    }
    return pd.DataFrame(dados)

lista_gerada = gerador_dados(100)

df_gerado = gerador_dados_pandas(100)

df_gerado.to_csv('dados_gerados.csv', index=False)

# Não precisamos definir as colunar porque estamos usando dictionary/dicionario
# O pandas infere o DF
# Pouco eficiente, 1-Python nativo, 2-Tradução para DF
pd.DataFrame(lista_gerada).to_csv('dados_gerados_lista.csv', index=False)
