import pandas as pd
import psycopg2
import csv
import os

# Importa a conexão do arquivo 'connection.py' que está no diretório raiz
# (Um nível acima do diretório 'src')
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from .db.connection import get_db_connection

def salvar_csv(df: pd.DataFrame, caminho_saida: str):
    """
    Salva um DataFrame em um arquivo CSV.
    """
    try:
        # Usando 'utf-8-sig' para garantir compatibilidade com Excel
        df.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
    except Exception as e:
        print(f"Erro ao salvar o arquivo {caminho_saida}: {e}")

def load_csv_to_postgres(caminho_csv: str, nome_tabela: str):
    """
    Carrega um arquivo CSV para uma tabela do PostgreSQL usando o comando COPY.
    Pressupõe que as colunas do CSV batem exatamente com as colunas da tabela.
    """
    print(f"   Iniciando 'COPY' para a tabela '{nome_tabela}'...")
    
    try:
        # --- 1. Ler o cabeçalho do CSV ---
        with open(caminho_csv, mode='r', encoding='utf-8-sig') as f:
            reader = csv.reader(f, delimiter=';')
            colunas = tuple(next(reader)) # Pega a primeira linha (cabeçalho)
            
        # --- 2. Abrir o arquivo novamente para o COPY ---
        with open(caminho_csv, mode='r', encoding='utf-8-sig') as f_data:
            next(f_data) # Pular o cabeçalho
            
            # --- 3. Usar a conexão do 'connection.py' ---
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    
                    # --- 4. Executar o COPY ---
                    cur.copy_from(
                        file=f_data,
                        table=nome_tabela,
                        sep=';',
                        columns=colunas
                    )
                    
        print(f"   -> 'COPY' para '{nome_tabela}' concluído com sucesso.")

    except FileNotFoundError:
        print(f"Erro [LOAD]: O arquivo '{caminho_csv}' não foi encontrado.")
    except (Exception, psycopg2.Error) as error:
        print(f"Erro [LOAD] ao carregar dados para '{nome_tabela}': {error}")
        print("   A transação foi revertida (rollback).")
        