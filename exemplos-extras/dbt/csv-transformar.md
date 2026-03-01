
1. Estrutura de Pastas do Projeto dbt
Quando você digita o comando dbt init meu_projeto no seu terminal, ele cria uma estrutura básica. Para organizar o nosso Data Warehouse focado em Vendas e Clientes, nós ajustamos as pastas internas assim:

- meu_projeto_dbt/
- dbt_project.yml: O "cérebro" do projeto. Aqui você diz qual é o nome do projeto, qual banco de dados ele vai se conectar (BigQuery) e as configurações globais.
- snapshots/: Onde colocamos os arquivos de histórico (SCD Tipo 2). O nosso arquivo clientes_snapshot.sql que criamos na etapa anterior mora exclusivamente aqui.
- models/: A pasta mais importante. Onde ficam os seus scripts de transformação (SELECT). Nós dividimos ela em duas subpastas essenciais:
- models/staging/: A "cozinha" do restaurante. Aqui você cria views simples que apenas limpam os nomes das colunas e padronizam tipos de dados (ex: transformar string em date), puxando os dados da área bruta do BigQuery.
- models/marts/: O "salão principal" do restaurante. Aqui ficam os pratos prontos para o cliente (analista de BI). É onde você cria as tabelas finais, como a dim_clientes e fato_vendas.
- tests/: Onde você guarda scripts SQL personalizados para testar regras de negócio específicas (ex: "nenhuma venda pode ter valor negativo").
- macros/: Onde ficam funções reutilizáveis em Jinja (uma linguagem de template) para não repetir código SQL.

2. Os Arquivos CSV para Testar a Pipeline (SCD Tipo 2)
Para ver a mágica do SCD Tipo 2 acontecendo, você precisa rodar a pipeline duas vezes, simulando dois dias diferentes. O dbt precisa ver o estado "antigo" e depois o "novo" para poder fechar a data de validade de um e abrir o do outro.

Dia 1: A Carga Inicial (clientes_dia1.csv)
No primeiro dia da sua empresa, você tem três clientes. Salve isso como o seu primeiro CSV, suba para o Data Lake e rode a pipeline (Airflow + dbt). O dbt vai criar os três clientes com status_ativo = true e data de fim em 9999-12-31.

Snippet de código
id_cliente,nome,cidade
1,MARIA,SAO PAULO
2,JOAO,BELO HORIZONTE
3,CARLOS,CURITIBA
Dia 2: A Atualização (clientes_dia2.csv)
No mês seguinte, aconteceram duas coisas no sistema do dia a dia (OLTP): a Maria se mudou para o Rio de Janeiro e uma cliente nova, a Ana, se cadastrou. O João e o Carlos não fizeram nada.

Salve esse novo CSV e rode a pipeline de novo.

Snippet de código
id_cliente,nome,cidade
1,MARIA,RIO DE JANEIRO
2,JOAO,BELO HORIZONTE
3,CARLOS,CURITIBA
4,ANA,PORTO ALEGRE
O que vai acontecer na sua tabela final no BigQuery?
Quando o dbt Snapshot terminar de processar o arquivo do Dia 2, a sua tabela invisível de clientes passará a ter 5 linhas:

A linha do Carlos (Ativa, Curitiba).
A linha do João (Ativa, Belo Horizonte).
A linha da Ana (Ativa, Porto Alegre - Nova!).
A linha ANTIGA da Maria (Inativa, São Paulo, com data_fim preenchida com o dia da pipeline).
A linha NOVA da Maria (Ativa, Rio de Janeiro).

É assim que você prova que a sua pipeline está robusta e pronta para produção!