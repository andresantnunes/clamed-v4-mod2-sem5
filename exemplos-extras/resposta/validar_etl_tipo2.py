from pathlib import Path
import sqlite3

import pandas as pd

import etl_tipo2_exemplo

BASE_DIR = Path(__file__).resolve().parent
ARQ_DB = BASE_DIR / "meu_dw_tipo2.db"


def carregar_dim_clientes() -> pd.DataFrame:
    with sqlite3.connect(ARQ_DB) as conexao:
        return pd.read_sql_query(
            """
            SELECT id_cliente, nome, endereco, is_current, dt_inicio
              FROM dim_clientes
            """,
            conexao,
        )


def validar_regras_scd2(df: pd.DataFrame) -> tuple[bool, list[str]]:
    erros: list[str] = []

    if df.empty:
        erros.append("A dimensão de clientes está vazia.")
        return False, erros

    current_por_cliente = (
        df.groupby("id_cliente")["is_current"].sum().reset_index(name="qtd_current")
    )
    invalidos = current_por_cliente[current_por_cliente["qtd_current"] != 1]
    if not invalidos.empty:
        erros.append("Existe cliente sem exatamente 1 registro atual (is_current=1).")

    cliente_2 = df[df["id_cliente"] == 2]
    if len(cliente_2) != 2:
        erros.append("Cliente 2 deveria ter exatamente 2 versões históricas.")
    else:
        enderecos_cliente_2 = set(cliente_2["endereco"].tolist())
        if len(enderecos_cliente_2) != 2:
            erros.append("Cliente 2 deveria possuir dois endereços diferentes.")

        if not ((cliente_2["is_current"] == 0).any() and (cliente_2["is_current"] == 1).any()):
            erros.append("Cliente 2 deveria ter uma versão antiga e uma atual.")

    cliente_4 = df[df["id_cliente"] == 4]
    if len(cliente_4) != 1 or int(cliente_4.iloc[0]["is_current"]) != 1:
        erros.append("Cliente 4 deveria existir com um único registro atual.")

    ok = len(erros) == 0
    return ok, erros


def main() -> None:
    etl_tipo2_exemplo.executar_exemplo_tipo2()
    df = carregar_dim_clientes()

    ok, erros = validar_regras_scd2(df)

    print("\n===== VALIDADOR SCD TIPO 2 =====")
    if ok:
        print("✅ Validação aprovada: regras de SCD Tipo 2 atendidas.")
        return

    print("❌ Validação reprovada:")
    for erro in erros:
        print(f"- {erro}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
