"""
ETL SCD Tipo 2 — Versão 100% DataFrame (Pandas).

Preserva o histórico de alterações SEM usar SQL.
Toda a lógica de fechar registros antigos e criar novas versões
é feita usando operações do Pandas (merge, loc, concat).

Utiliza o gerador_dados.py (do ecommerce-etl) para gerar e atualizar clientes.

Fluxo:
  1. Gera dados iniciais com gerar_dados_iniciais()
  2. Aplica SCD Tipo 2 apenas com DataFrames
  3. Gera atualizações com gerar_updates()
  4. Reaplicar SCD Tipo 2 e imprimir resultado mostrando histórico
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Importar gerador_dados do ecommerce-etl/src
# ---------------------------------------------------------------------------
GERADOR_DIR = (
    Path(__file__).resolve().parent.parent
    / "exemplo-aula"
    / "ecommerce-etl"
    / "src"
)
sys.path.insert(0, str(GERADOR_DIR))

from gerador_dados import gerar_dados_iniciais, gerar_updates  # noqa: E402

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
ARQ_PARQUET = BASE_DIR / "dim_clientes_scd2.parquet"

# Colunas que, se mudarem, geram nova versão (SCD Tipo 2)
COLUNAS_RASTREADAS = ["Endereco", "Preco_Score"]


# ---------------------------------------------------------------------------
# SCD Tipo 2 com DataFrames
# ---------------------------------------------------------------------------
def aplicar_scd_tipo2_dataframe(
    dim_atual: pd.DataFrame,
    staging: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aplica SCD Tipo 2 comparando a dimensão atual com os dados de staging.

    Parâmetros:
        dim_atual: DataFrame com a dimensão (pode estar vazio na 1ª carga).
        staging:   DataFrame com os dados novos vindos da fonte.

    Retorna:
        DataFrame atualizado com registros históricos e novos.
    """
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # -------------------------------------------------------------------
    # Caso 1: Carga inicial (dimensão vazia)
    # -------------------------------------------------------------------
    if dim_atual.empty:
        staging = staging.copy()
        staging["sk_cliente"] = range(1, len(staging) + 1)
        staging["is_current"] = 1
        staging["data_inicio"] = agora
        staging["data_fim"] = "9999-12-31"
        return staging[
            ["sk_cliente", "ID", "Nome", "Endereco", "Preco_Score",
             "is_current", "data_inicio", "data_fim"]
        ]

    # -------------------------------------------------------------------
    # Caso 2: Carga incremental
    # -------------------------------------------------------------------
    # Pegar apenas os registros ativos da dimensão
    ativos = dim_atual[dim_atual["is_current"] == 1].copy()

    # Fazer merge entre staging e registros ativos pela chave natural (ID)
    comparacao = staging.merge(
        ativos[["ID"] + COLUNAS_RASTREADAS],
        on="ID",
        how="left",
        suffixes=("_stg", "_dim"),
    )

    # ---- Identificar registros ALTERADOS ----
    mascara_alterados = pd.Series(False, index=comparacao.index)
    for col in COLUNAS_RASTREADAS:
        col_stg = f"{col}_stg"
        col_dim = f"{col}_dim"
        # Mudou se existia na dimensão (não é NaN) e o valor é diferente
        mascara_alterados |= (
            comparacao[col_dim].notna() & (comparacao[col_stg] != comparacao[col_dim])
        )

    ids_alterados = set(comparacao.loc[mascara_alterados, "ID"])

    # ---- Identificar registros NOVOS (não existiam na dimensão) ----
    ids_existentes = set(ativos["ID"])
    ids_novos = set(staging["ID"]) - ids_existentes

    # ---- PASSO 1: Fechar registros antigos na dimensão ----
    dim_resultado = dim_atual.copy()
    dim_resultado.loc[
        (dim_resultado["ID"].isin(ids_alterados)) & (dim_resultado["is_current"] == 1),
        ["is_current", "data_fim"],
    ] = [0, agora]

    # ---- PASSO 2: Criar novas versões para alterados e novos ----
    ids_para_inserir = ids_alterados | ids_novos
    novos_registros = staging[staging["ID"].isin(ids_para_inserir)].copy()

    if not novos_registros.empty:
        # Colunas com sufixo _stg podem existir se vieram do merge; usar as originais
        for col in COLUNAS_RASTREADAS:
            col_stg = f"{col}_stg"
            if col_stg in novos_registros.columns:
                novos_registros = novos_registros.rename(columns={col_stg: col})
                novos_registros = novos_registros.drop(
                    columns=[f"{col}_dim"], errors="ignore"
                )

        # Gerar surrogate keys sequenciais a partir do máximo atual
        sk_max = dim_resultado["sk_cliente"].max() if not dim_resultado.empty else 0
        novos_registros["sk_cliente"] = range(
            sk_max + 1, sk_max + 1 + len(novos_registros)
        )
        novos_registros["is_current"] = 1
        novos_registros["data_inicio"] = agora
        novos_registros["data_fim"] = "9999-12-31"

        novos_registros = novos_registros[
            ["sk_cliente", "ID", "Nome", "Endereco", "Preco_Score",
             "is_current", "data_inicio", "data_fim"]
        ]

        dim_resultado = pd.concat(
            [dim_resultado, novos_registros], ignore_index=True
        )

    # Contadores para exibição
    fechados = len(ids_alterados)
    inseridos = len(ids_para_inserir)
    print(f"  SCD Tipo 2 (DataFrame) — Fechados: {fechados} | Inseridos: {inseridos}")

    return dim_resultado


# ---------------------------------------------------------------------------
# Persistência simples (Parquet)
# ---------------------------------------------------------------------------
def salvar_dimensao(df: pd.DataFrame) -> None:
    df.to_parquet(ARQ_PARQUET, index=False)


def carregar_dimensao() -> pd.DataFrame:
    if ARQ_PARQUET.exists():
        return pd.read_parquet(ARQ_PARQUET)
    return pd.DataFrame(
        columns=[
            "sk_cliente", "ID", "Nome", "Endereco", "Preco_Score",
            "is_current", "data_inicio", "data_fim",
        ]
    )


# ---------------------------------------------------------------------------
# Exibição
# ---------------------------------------------------------------------------
def imprimir_dimensao(df: pd.DataFrame, titulo: str = "") -> None:
    print(f"\n{'=' * 90}")
    print(f"  {titulo}" if titulo else "  dim_clientes (DataFrame)")
    print(f"{'=' * 90}")
    if df.empty:
        print("  (vazia)")
    else:
        exibir = df.sort_values(["ID", "data_inicio"]).reset_index(drop=True)
        print(exibir.to_string(index=False))
    print()


def imprimir_historico_cliente(df: pd.DataFrame, id_cliente: int) -> None:
    """Mostra todas as versões de um cliente específico."""
    historico = df[df["ID"] == id_cliente].sort_values("data_inicio")
    print(f"\n--- Histórico do cliente ID={id_cliente} ---")
    if historico.empty:
        print("  Nenhum registro encontrado.")
    else:
        print(historico.to_string(index=False))
    print()


# ---------------------------------------------------------------------------
# Execução completa
# ---------------------------------------------------------------------------
def executar() -> None:
    # Limpar arquivo anterior para demonstração limpa
    if ARQ_PARQUET.exists():
        ARQ_PARQUET.unlink()

    # 1. Gerar dados iniciais
    print("[1/4] Gerando dados iniciais (50 clientes)...")
    df_inicial = gerar_dados_iniciais(num_registros=50)

    # 2. Carga inicial
    print("[2/4] Carga inicial — SCD Tipo 2 com DataFrames...")
    dim = carregar_dimensao()
    dim = aplicar_scd_tipo2_dataframe(dim, df_inicial)
    salvar_dimensao(dim)
    imprimir_dimensao(dim, "APÓS CARGA INICIAL")

    # 3. Gerar atualizações
    print("[3/4] Gerando 15 atualizações aleatórias...")
    df_atualizado = gerar_updates(df_inicial.copy(), num_updates=15)

    # 4. Carga incremental SCD2
    print("[4/4] Carga incremental — SCD Tipo 2 com DataFrames...")
    dim = carregar_dimensao()
    dim = aplicar_scd_tipo2_dataframe(dim, df_atualizado)
    salvar_dimensao(dim)
    imprimir_dimensao(dim, "APÓS ATUALIZAÇÃO SCD TIPO 2 (DataFrame)")

    # Mostrar histórico de clientes com mais de uma versão
    multi = dim.groupby("ID").size()
    ids_multi = multi[multi > 1].index.tolist()[:5]
    if ids_multi:
        print("=" * 90)
        print("  EXEMPLOS DE HISTÓRICO (clientes com mais de uma versão)")
        print("=" * 90)
        for id_cli in ids_multi:
            imprimir_historico_cliente(dim, id_cli)

    print("Concluído! Dimensão salva em:", ARQ_PARQUET)


if __name__ == "__main__":
    executar()
