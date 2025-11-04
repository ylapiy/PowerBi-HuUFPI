import psycopg2
import os
import sys
from dotenv import load_dotenv
from contextlib import contextmanager

# 1. Carregar variáveis de ambiente
load_dotenv()
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL is None:
    print("Erro: Variável de ambiente DATABASE_URL não encontrada.")
    print("Verifique se você criou o arquivo .env e o nome da variável está correto.")
    sys.exit(1) # Sai do script se a configuração essencial faltar

@contextmanager
def get_db_connection():
    """
    Gerenciador de contexto para conexões de banco de dados PostgreSQL.
    Garante que a conexão seja aberta, "commitada" em caso de sucesso,
    "rollback" em caso de erro, e fechada no final.
    """
    conn = None # Inicia conn como None
    try:
        # 2. Conectar
        conn = psycopg2.connect(DATABASE_URL)
        print("Conexão estabelecida.")
        
        # 3. Disponibiliza a conexão para o bloco 'with'
        yield conn 
        
        # 4. Se o bloco 'with' terminar sem erros, faz commit
        conn.commit()
        print("Transação commitada.")
        
    except (Exception, psycopg2.Error) as error:
        print(f"Erro durante a transação: {error}")
        if conn:
            # 5. Se deu erro, faz rollback
            conn.rollback()
            print("Transação revertida (rollback).")
        raise # Propaga o erro para o script principal saber o que aconteceu
        
    finally:
        if conn:
            # 6. Em todos os casos (sucesso ou erro), fecha a conexão
            conn.close()
            print("Conexão fechada.")

# 7. Bloco de teste
#    Este código SÓ executa quando você roda: python connection.py
if __name__ == "__main__":
    print("--- Executando teste de conexão local ---")
    
    try:
        # Usamos nossa própria função para testar
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT now();')
                server_time = cur.fetchone()
                print(f"Conexão bem-sucedida!")
                print(f"Hora atual do servidor do banco: {server_time[0]}")
    
    except (Exception, psycopg2.Error) as error:
        # O erro já foi impresso pelo get_db_connection, 
        # mas podemos confirmar que o teste falhou.
        print(f"Falha no teste de conexão: {error}")
        