{% snapshot dim_clientes %}

{{
    config(
      target_schema='meu_data_warehouse', -- Onde a tabela final vai ficar
      unique_key='id_cliente',            -- Qual é a chave do cliente
      
      -- Como o dbt vai saber se o cliente mudou? 
      -- Resposta: Checando se a coluna 'cidade' ou 'nome' foi alterada.
      strategy='check',                   
      check_cols=['cidade', 'nome']       
    )
}}


-- O dbt vai ler os dados que acabaram de chegar na área de Staging
SELECT 
    id_cliente,
    nome,
    cidade
FROM {{ source('minha_area_staging', 'stg_clientes') }}

{% endsnapshot %}