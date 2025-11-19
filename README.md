📌 README – Pipeline de Processamento de Projetos + Dashboard Power BI (Indicadores GEP – HU-UFPI)
🏥 Sobre o Projeto

Este projeto foi desenvolvido como parte de um trabalho de extensão relacionado ao Hospital Universitário da Universidade Federal do Piauí (HU-UFPI).
Ele possui duas partes integradas, que juntas criam um fluxo completo de análise de dados dos projetos do GEP (Gestão Estratégica de Projetos):

Pipeline em Python que trata arquivos, filtra registros, organiza nomes, padroniza colunas e insere automaticamente os dados em um banco PostgreSQL na plataforma Neon.

Dashboard em Power BI, que consome esses dados e exibe indicadores estratégicos do GEP, como andamento, situação dos projetos, distribuição por áreas, tempo médio de tramitação, entre outros.

⚙️ 1. Pipeline de Processamento em Python
🗂️ Estrutura do Processo

O pipeline é dividido em três etapas principais:

1. Entrada

O usuário insere arquivos CSV brutos na pasta:

csvs/brutos/

Esses arquivos vêm do HU-UFPI e podem conter inconsistências, repetições, acentos, colunas mal formatadas etc.

2. Filtragem / Limpeza

Scripts Python processam esses arquivos, gerando arquivos “limpos” já padronizados:

csvs/limpos/
nome_original_filtrado_limpo.csv

A limpeza inclui:

Remoção de inativos

Normalização de acentuação

Padronização dos nomes das colunas

Remoção de caracteres especiais

Eliminação de duplicatas

Conversão de datas quando necessário

3. Envio para o Banco (Enviador.py)

O arquivo Enviador.py lê automaticamente todos os CSVs que terminam com \_limpo.csv e executa:

✔ Criação da tabela correspondente no PostgreSQL (com CREATE TABLE IF NOT EXISTS)
✔ TRUNCATE antes de inserir, para garantir dados atualizados
✔ Inserção linha por linha com tratamento de erros
✔ Normalização automática do nome das colunas
✔ Geração automática dos nomes das tabelas com base no arquivo

A conexão é feita através de uma CONN_STR, protegida via .env, seguindo boas práticas de segurança.

📁 Estrutura Recomendada de Pastas
projeto/
│── csvs/
│ ├── brutos/
│ └── limpos/
│── Enviador.py
│── limpeza_duplicatas.py
|── limpeza_projetos_inativos.py
│── Apipe.py
│── .env
│── .gitignore
│── README.md

🔐 Segurança (Uso do .env)

A string de conexão fica armazenada em:

.env

Exemplo:

CONN_STR="postgresql://usuario:senha@host/banco?sslmode=require"

E o .gitignore contém:

.env

para evitar exposição dos dados sensíveis.

📊 2. Dashboard em Power BI — Indicadores GEP (HU-UFPI)

Após o carregamento dos dados no PostgreSQL/Neon, o Power BI acessa essas tabelas e constrói um painel visual com os indicadores essenciais da gestão de projetos.

🎯 Objetivo do Dashboard

Fornecer aos gestores e analistas do HU-UFPI uma visão clara sobre:

(Ainda vou escrever)

O painel ajuda a tomada de decisão e facilita o acompanhamento contínuo da execução dos projetos institucionais.

📌 Indicadores Comuns no Painel (exemplos)

(Ainda vou escrever)
