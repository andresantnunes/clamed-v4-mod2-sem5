from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
ARQ_PRODUTOS = DATA_DIR / "produtos.csv"
ARQ_CLIENTES = DATA_DIR / "clientes.csv"


def garantir_pasta_dados() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def criar_dados_iniciais() -> None:
    """Parte 1 - cria os CSVs iniciais com 3 produtos e 3 clientes."""
    garantir_pasta_dados()

    produtos = pd.DataFrame(
        [
            {"id_produto": 1, "nome": "Mouse Gamer", "preco": 89.90},
            {"id_produto": 2, "nome": "Teclado Mecanico", "preco": 249.90},
            {"id_produto": 3, "nome": "Monitor 24", "preco": 799.90},
        ]
    )

    clientes = pd.DataFrame(
        [
            {"id_cliente": 1, "nome": "Ana Silva", "endereco": "Rua A, 100"},
            {"id_cliente": 2, "nome": "Bruno Costa", "endereco": "Rua B, 200"},
            {"id_cliente": 3, "nome": "Carla Souza", "endereco": "Rua C, 300"},
        ]
    )

    produtos.to_csv(ARQ_PRODUTOS, index=False)
    clientes.to_csv(ARQ_CLIENTES, index=False)

    print(f"CSV criado: {ARQ_PRODUTOS}")
    print(f"CSV criado: {ARQ_CLIENTES}")


def gerar_mudancas() -> None:
    """Parte 1 - altera preço de produto, muda endereço e adiciona cliente."""
    garantir_pasta_dados()

    if not ARQ_PRODUTOS.exists() or not ARQ_CLIENTES.exists():
        raise FileNotFoundError(
            "Arquivos CSV não encontrados. Rode primeiro: python gerador.py"
        )

    produtos = pd.read_csv(ARQ_PRODUTOS)
    clientes = pd.read_csv(ARQ_CLIENTES)

    produtos.loc[produtos["id_produto"] == 2, "preco"] = 269.90

    clientes.loc[clientes["id_cliente"] == 2, "endereco"] = "Avenida Nova, 500"

    novo_cliente = pd.DataFrame(
        [{"id_cliente": 4, "nome": "Diego Lima", "endereco": "Rua D, 400"}]
    )
    clientes = pd.concat([clientes, novo_cliente], ignore_index=True)
    clientes = clientes.drop_duplicates(subset=["id_cliente"], keep="last")

    produtos.to_csv(ARQ_PRODUTOS, index=False)
    clientes.to_csv(ARQ_CLIENTES, index=False)

    print("Mudanças aplicadas com sucesso.")


if __name__ == "__main__":
    gerar_mudancas()
