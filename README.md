# Brazil Macro Pulse | Pipeline de inflação e Selic

Projeto de portfólio que coleta séries públicas do Banco Central do Brasil, valida e armazena os dados em SQLite, prepara uma tabela mensal com SQL e gera um painel interativo.

**Pergunta de negócio:** como a inflação mensal e a Meta Selic evoluíram no período? O projeto descreve tendências e relações no tempo; não afirma que uma variável causou a outra.

## O que o projeto demonstra

- Consumo de API REST pública com parâmetros de período e tratamento de falhas HTTP.
- Ingestão de séries com frequências diferentes: IPCA mensal e Meta Selic diária.
- Validação de datas, valores numéricos, duplicatas e registros vazios.
- Modelagem relacional e carga idempotente com chave composta e UPSERT.
- SQL com CTEs, funções de janela e agregação mensal.
- Preparação de indicadores com Pandas e NumPy e visualização interativa com Plotly.
- Documentação da fonte, regras de qualidade, limitações e execução.

## Dados

| Série | Código SGS | Frequência | Unidade | Uso |
|---|---:|---|---|---|
| IPCA — variação mensal | 433 | Mensal | % no mês | Inflação mensal e inflação composta em 12 meses |
| Meta Selic definida pelo Copom | 432 | Diária | % ao ano | Última taxa observada de cada mês |

Fontes oficiais: [IPCA no SGS](https://www3.bcb.gov.br/sgspub/consultarvalores/consultarValoresSeries.do?method=consultarSeries&series=433) e [dataset da Meta Selic](https://dadosabertos.bcb.gov.br/dataset/432-taxa-de-juros---meta-selic-definida-pelo-copom). O portal do Banco Central informa limite de dez anos por consulta para séries diárias em JSON/CSV; por isso, o código divide as chamadas em intervalos menores. Consulte os metadados e a licença indicados na fonte antes de redistribuir os dados.

## Fluxo

    API SGS do Banco Central → Python: extração por períodos → validação de qualidade
    → SQLite: séries e observações → SQL: visão mensal IPCA + Selic
    → Pandas / NumPy: inflação em 12 meses → Plotly: painel HTML e CSV mensal

## Como executar

Requer Python 3.10 ou superior e conexão com a internet para consultar o Banco Central.

    git clone https://github.com/DiegoFariaFuel/brazil-macro-pipeline.git
    cd brazil-macro-pipeline
    python -m venv .venv
    source .venv/bin/activate          # Windows: .venv/Scripts/activate
    python -m pip install -r requirements.txt
    python -m src.pipeline

Por padrão, o pipeline coleta desde 2015-01-01 até a data atual. Para escolher outro intervalo:

    python -m src.pipeline --start-date 2018-01-01 --end-date 2025-12-31

O comando pode ser repetido: as observações existentes são atualizadas pela chave composta series_code e reference_date.

## Arquivos gerados

- data/processed/macro.sqlite — banco SQLite com metadados, observações e visão mensal.
- outputs/macro_monthly.csv — tabela analítica mensal, incluindo IPCA acumulado composto em 12 meses.
- outputs/dashboard.html — painel interativo de inflação mensal, inflação em 12 meses e Meta Selic no fim do mês.
- outputs/analysis_summary.md — resumo dos valores mais recentes e das lacunas de publicação observadas na execução.

Abra outputs/dashboard.html em um navegador após executar o pipeline. O painel carrega a biblioteca Plotly por CDN e requer internet para exibição.

## Consultas SQL incluídas

sql/analysis.sql contém exemplos para conferir a tabela mensal, resumir médias anuais e listar os meses de maior inflação. sql/schema.sql define as tabelas, chaves e a visão v_macro_monthly.

## Regras e limitações

1. Cada observação precisa ter data válida e valor numérico finito.
2. Não pode haver duas observações para a mesma série e data.
3. Dias sem publicação da Meta Selic não são preenchidos artificialmente; o indicador mensal usa a última observação disponível no mês.
4. A inflação de 12 meses é composta a partir das 12 variações mensais, não é a soma simples.
5. A análise é descritiva. Associação temporal entre inflação e juros não comprova causalidade.
6. O projeto não inclui previsão financeira nem recomendação de investimento.

## English summary

This portfolio project ingests Brazil Central Bank time series (monthly IPCA inflation and daily Copom Selic target), validates the records, stores them in SQLite, builds a monthly SQL view, calculates compounded 12-month inflation, and creates an interactive Plotly dashboard. It demonstrates REST API ingestion, data quality checks, relational modeling, SQL window functions, Pandas, NumPy, and reproducible documentation. The analysis is descriptive and does not claim causality.

## Licenças

O código deste projeto está sob MIT (ver LICENSE). Os dados permanecem sujeitos à licença e às condições publicadas pelo Banco Central.
