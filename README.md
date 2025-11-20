📌 README – Pipeline de Processamento de Projetos + Dashboard Power BI (Indicadores GEP – HU-UFPI)
🏥 Sobre o Projeto

--link de vizualiação do overleaf : https://www.overleaf.com/read/drsyczzjpkbx#5538b2

Grupo :

Ygor Jivago 
Vinicius Azevedo
Augusto César
Mateus Faria
Talyson Machado
Théo Alencar da Silva

Este projeto foi desenvolvido como parte de um trabalho de extensão relacionado ao Hospital Universitário da Universidade Federal do Piauí (HU-UFPI).
Ele possui duas partes integradas, que juntas criam um fluxo completo de análise de dados dos projetos do GEP (Gestão Estratégica de Projetos):

Pipeline em Python que trata arquivos, filtra registros, organiza nomes, padroniza colunas e insere automaticamente os dados em um banco PostgreSQL na plataforma Neon.

Dashboard em Power BI, que consome esses dados e exibe indicadores estratégicos do GEP, como andamento, situação dos projetos, distribuição por áreas, tempo médio de tramitação, entre outros.

Pipeline de Processamento de Projetos + Dashboard Power BI (Indicadores GEP – HU-UFPI)
Sobre o Projeto

Este projeto reúne um pipeline de tratamento de dados em Python e um conjunto de dashboards desenvolvidos no Power BI. O objetivo é organizar, limpar, padronizar e analisar os dados relacionados aos projetos geridos pelo GEP (Gestão Estratégica de Projetos) do HU-UFPI, oferecendo uma visão clara e confiável para decisão estratégica.

O fluxo completo funciona assim:

Entrada

O usuário insere arquivos CSV brutos na pasta:

csvs/brutos/

Esses arquivos vêm do HU-UFPI e podem conter inconsistências, repetições, acentos, colunas mal formatadas etc. - > Sempre são esperados os arquivos relatorio_projetos_historico.csv e relatorio_projetos.csv

Os arquivos são tratados, organizados e filtrados via scripts em Python.

Os dados são enviados automaticamente para um banco PostgreSQL hospedado na Neon.

O Power BI consome esse banco e gera dashboards analíticos com indicadores essenciais.

1. Pipeline de Processamento em Python
Estrutura Geral

requirements :

pandas
psycopg2-binary (psycopg2)
python-dotenv
DATABASE_URL = variavel local 

O pipeline executa etapas de:

Entrada
Leitura dos arquivos brutos (.csv) contendo informações dos projetos e seus históricos.

Tratamento
• Limpeza de colunas
• Remoção de duplicatas
• Filtragem de registros inválidos
• Ajuste de colunas de tempo
• Criação de estruturas padronizadas
• Organização do output em múltiplas pastas (limpos, sem duplicatas etc.)

Envio para o Banco
Finalizando o processamento, os dados são enviados automaticamente para um banco PostgreSQL/Neon, que é a fonte principal do Power BI.

Scripts incluídos

script_limpeza_duplicatas.py – remove duplicidades nos registros.

script_limpeza_projetos_inativos.py – filtra projetos inativos ou inválidos.

Apipe.py – pipeline geral de processamento.

Enviador.py – integração com o banco de dados.

update_tempo_trigger.sql – trigger SQL para manter a coluna “tempo” sempre atualizada conforme alterações na coluna “duracao”. (caso um novo banco seja criado esse script deve ser carregado manualmnte, pois ão faz parte da pipeline)

Estrutura de Pastas
csvs/
 ├── brutos/
 ├── limpos/
 └── sem_duplicatas/

dashboards/
 ├── versões antigas/
 └── versões novas/

*.py
*.sql
README.md

2. Dashboard em Power BI

Após o carregamento no PostgreSQL, o Power BI lê a base atualizada e monta os painéis de indicadores do GEP.

Objetivo do Dashboard

Dar aos gestores do HU-UFPI uma visão rápida e precisa sobre o andamento dos projetos institucionais. O painel auxilia tanto o acompanhamento operacional quanto decisões estratégicas.

Indicadores disponíveis (ou previstos)

Quantidade total de projetos

Projetos ativos, concluídos e inativos

Tempo médio de tramitação

Distribuição por área / categoria

Evolução temporal dos registros

Histórico de movimentação dos projetos

Análises de produtividade

Comparações entre períodos

Banco de Dados

O sistema utiliza PostgreSQL (Neon), e parte da lógica do banco é automatizada com triggers
SQL.
O arquivo update_tempo_trigger.sql garante que qualquer alteração na coluna duracao reflita corretamente no campo tempo em segundos.

Como Executar

Coloque seus arquivos brutos em csvs/brutos.

Execute o pipeline (ex.: python Apipe.py).

Os arquivos tratados serão gerados nas respectivas pastas.

O Enviador.py cuidará do envio ao PostgreSQL.

Abra o Power BI e atualize o dashboard conectado ao banco.

Observações

Os dashboards possuem versões antiga e nova dentro da pasta dashboards/.

Os arquivos .pbix já estão configurados para ler do PostgreSQL.

O projeto é modular, permitindo expansão futura para novas regras, novos datasets ou automação contínua.

3. Artigo 

Este projeto faz parte do estudo “Integração de ETL em Python e Power BI para Gestão Estratégica de Projetos Institucionais no Hospital Universitário da UFPI”, no qual desenvolvemos uma solução completa de Business Intelligence para monitoramento dos projetos vinculados ao HU-UFPI.

O artigo apresenta o contexto de fragmentação informacional existente no hospital e descreve como a equipe desenvolveu um fluxo end-to-end de dados, composto por:

ETL automatizado em Python, responsável por coletar, limpar, padronizar e consolidar dados provenientes de planilhas institucionais;

Modelagem e armazenamento em um banco de dados relacional (PostgreSQL/Neon);

Dashboards interativos em Power BI, exibindo indicadores como classificação dos projetos, evolução temporal, participação multicêntrica, tipos de estudo, andamento processual e produtos previstos.

O estudo demonstra que a solução implementada melhora a governança da informação, reduz inconsistências presentes nas planilhas brutas, aumenta a transparência e fortalece a tomada de decisão baseada em evidências dentro do HU-UFPI. O pipeline proposto também se mostra escalável, replicável e adequado às demandas analíticas de hospitais universitários.