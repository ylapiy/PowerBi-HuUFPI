import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import csv 

load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL is None:
    print("Erro [LOAD]: Variável de ambiente DATABASE_URL não encontrada.", file=sys.stderr)
    sys.exit(1)


def salvar_csv(df: pd.DataFrame, caminho_csv: str):
    """
    Salva o DataFrame como um arquivo CSV, otimizado para o COPY do Postgres.
    """
    try:
        df.to_csv(
            caminho_csv,
            sep=';',           # Usa ponto e vírgula como delimitador
            index=False,       # Não salva o índice do pandas
            encoding='utf-8',  # Garante a codificação correta
            
            # Salva NaT/None (nulos) como uma string vazia
            na_rep='', 
            
            # Usa o modo de quoting padrão do pandas, que é compatível com 'FORMAT csv'
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL 
        )
    except Exception as e:
        print(f"Erro [LOAD] ao salvar CSV em {caminho_csv}: {e}", file=sys.stderr)


def load_csv_to_postgres(caminho_csv: str, nome_tabela: str, df_colunas: list):
    """
    Carrega um arquivo CSV para uma tabela no PostgreSQL usando o comando COPY.
    Esta versão é robusta e lida com CSVs complexos, nulos e parsing.
    """
    print(f"   Iniciando 'COPY' para a tabela '{nome_tabela}'...")
    
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False # Controlamos a transação
        cur = conn.cursor()
        
        print(f"   -> Limpando tabela de destino '{nome_tabela}' (TRUNCATE)...")
        cur.execute(f"TRUNCATE TABLE {nome_tabela} CASCADE;")
        print(f"   -> Tabela limpa.")

        colunas_sql_str = f"({', '.join([f'\"{col}\"' for col in df_colunas])})"

        sql_copy = f"""
            COPY {nome_tabela} {colunas_sql_str}
            FROM stdin
            WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '"', NULL '')
        """
        
        # Abre o arquivo e o passa para o copy_expert (stdin)
        with open(caminho_csv, 'r', encoding='utf-8') as f:
            cur.copy_expert(sql_copy, f)
        
        conn.commit() 
        print(f"   -> Carga para '{nome_tabela}' concluída com sucesso.")

    except (Exception, psycopg2.Error) as error:
        if conn:
            conn.rollback()
            
        print(f"\nErro durante a transação: {error}", file=sys.stderr)
        print(f"Erro [LOAD] ao carregar dados para '{nome_tabela}': {error}", file=sys.stderr)
        print("   A transação foi revertida (rollback).")
    
    finally:
        if conn:
            conn.close()