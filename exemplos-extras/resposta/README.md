# Resposta do Exercício - Pipeline ETL com SCD 1 e SCD 2

Este diretório contém **toda a implementação** pedida no `Exercicio.md`, com arquitetura simples e nomes claros.

## Tecnologias usadas

- **Python 3**: linguagem principal
- **Pandas**: leitura/escrita de CSV e carga de staging (`to_sql`)
- **SQLite (`sqlite3`)**: Data Warehouse local (`meu_dw.db`)

## Estrutura

- `gerador.py`: gera os CSVs e aplica mudanças simuladas
- `etl_loja.py`: cria DW, carrega staging, aplica SCD 1 e SCD 2, imprime validação
- `data/produtos.csv` e `data/clientes.csv`: dados da loja
- `meu_dw.db`: banco SQLite criado automaticamente

## Delimitação clara por partes

### Parte 1 - Simulador de e-commerce
Arquivo: `gerador.py`

- Função `criar_dados_iniciais()` cria:
  - `produtos.csv` com 3 produtos
  - `clientes.csv` com 3 clientes
- Função `gerar_mudancas()`:
  - altera preço de um produto
  - altera endereço de um cliente
  - adiciona um novo cliente

### Parte 2 - Data Warehouse
Arquivo: `etl_loja.py`

- Conexão com `meu_dw.db`
- Criação das dimensões:
  - `dim_produtos(id_produto, nome, preco)`
  - `dim_clientes(id_cliente, nome, endereco, is_current, dt_inicio)`

### Parte 3 - Staging
Arquivo: `etl_loja.py`

- Leitura de CSV com Pandas
- Escrita em tabelas temporárias SQLite:
  - `stg_produtos`
  - `stg_clientes`

### Parte 4 - SCD Tipo 2 (clientes)
Arquivo: `etl_loja.py`

- Detecta mudança de endereço comparando `stg_clientes` com `dim_clientes` atual (`is_current = 1`)
- Fecha registro antigo (`is_current = 0`)
- Insere novo registro com endereço atualizado (`is_current = 1` + `dt_inicio`)

## Como executar

No terminal, dentro da pasta `resposta`:

1. Criar dados iniciais:
   - `python gerador.py`
2. Rodar ETL (carga inicial):
   - `python etl_loja.py`
3. Aplicar mudanças simuladas:
   - `python -c "import gerador; gerador.gerar_mudancas()"`
4. Rodar ETL novamente:
   - `python etl_loja.py`

Pronto: o resultado impresso em `dim_clientes` valida o SCD Tipo 2 e `dim_produtos` valida o SCD Tipo 1.

## Execução com 1 comando

- `python run_validacao.py`

Esse script executa automaticamente todo o passo a passo da validação (Partes 1 a 6).
