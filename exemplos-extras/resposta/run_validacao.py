from pathlib import Path

import gerador
import etl_loja


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    arquivo_db = base_dir / "meu_dw.db"

    if arquivo_db.exists():
        arquivo_db.unlink()

    print("===== INÍCIO DO FLUXO COMPLETO =====")
    print(f"Diretório de execução: {base_dir}")

    print("\n[1/4] Gerando dados iniciais...")
    gerador.criar_dados_iniciais()

    print("\n[2/4] Executando ETL (carga inicial)...")
    etl_loja.executar_etl()

    print("\n[3/4] Aplicando mudanças no simulador...")
    gerador.gerar_mudancas()

    print("\n[4/4] Executando ETL novamente (validação SCD)...")
    etl_loja.executar_etl()

    print("\n===== FIM DO FLUXO COMPLETO =====")


if __name__ == "__main__":
    main()
