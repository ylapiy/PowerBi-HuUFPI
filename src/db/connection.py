import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL is None:
    print("Erro: Variável de ambiente DATABASE_URL não encontrada.")
    print("Verifique se você criou o arquivo .env e o nome da variável está correto.")
else:
    print("Tentando conectar ao banco de dados...")

    try:
        with psycopg2.connect(DATABASE_URL) as conn:

            with conn.cursor() as cur:

                cur.execute('SELECT now();')

                server_time = cur.fetchone()

                print(f"Conexão bem-sucedida!")
                print(f"Hora atual do servidor do banco: {server_time[0]}")
    
    except (Exception, psycopg2.Error) as error:
        print(f"Erro ao conectar ao banco de dados: {error}")
        