import pandas as pd
import psycopg2
import unicodedata
import os
import glob
from dotenv import load_dotenv

load_dotenv()

# ============================
# CONFIG
# ============================
PASTA_LIMPOS = "csvs/limpos"
CONN_STR = os.getevn("DATABASE_STRING ")


# ============================
# UTIL: NORMALIZAR NOMES
# ============================
def normalize_name(name):
    nfkd = unicodedata.normalize("NFKD", name)
    no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    clean = (
        no_accents.replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
    )
    return clean.lower()


# ============================
# LER TODOS OS CSVs LIMPOS
# ============================
def obter_caminhos_limpos():
    padrao = os.path.join(PASTA_LIMPOS, "*_limpo.csv")
    arquivos = glob.glob(padrao)

    if not arquivos:
        raise FileNotFoundError(
            "Nenhum arquivo *_limpo.csv encontrado na pasta limpos."
        )

    arquivos = sorted(arquivos)  # sempre na mesma ordem
    return arquivos


# ============================
# CRIAR TABELA
# ============================
def create_table(conn, table_name, df):
    cur = conn.cursor()

    column_defs = []
    for col in df.columns:
        safe_col = normalize_name(col)
        column_defs.append(f'"{safe_col}" TEXT')

    schema = ",\n    ".join(column_defs)
    sql = f'CREATE TABLE IF NOT EXISTS"{table_name}" (\n    {schema}\n);'

    print(f"\n📌 Criando tabela: {table_name}")
    cur.execute(sql)
    conn.commit()
    cur.close()


# ============================
# INSERIR DADOS
# ============================
def insert_data(conn, table_name, df):
    cursor = conn.cursor()

    # 🔥 Limpar a tabela antes de inserir novos dados
    print(f"🧹 Limpando tabela {table_name} antes de inserir dados...")
    cursor.execute(f'TRUNCATE TABLE "{table_name}"')

    safe_cols = [normalize_name(c) for c in df.columns]

    cols_sql = ", ".join([f'"{c}"' for c in safe_cols])
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f'INSERT INTO "{table_name}" ({cols_sql}) VALUES ({placeholders})'

    print(f"Inserindo {len(df)} linhas em {table_name}...")

    for idx, row in df.iterrows():
        try:
            cursor.execute(sql, list(row.values))
        except Exception as e:
            print("🚨 ERRO AO INSERIR LINHA", idx)
            print(row)
            print(e)
            raise

    conn.commit()
    cursor.close()


# ============================
# PROCESSAR UM CSV
# ============================
def process_csv(csv_path):
    nome = os.path.basename(csv_path)
    print(f"\n📥 Lendo arquivo: {nome}")

    # Tabela é o nome do arquivo sem extensão e sem "_limpo"
    nome_base = nome.replace("_limpo.csv", "")
    table_name = normalize_name(nome_base)

    df = pd.read_csv(csv_path)
    conn = psycopg2.connect(CONN_STR)

    create_table(conn, table_name, df)
    insert_data(conn, table_name, df)

    conn.close()
    print(f"✔ {table_name} atualizado!")


# ============================
# EXECUÇÃO PRINCIPAL
# ============================
def main():
    arquivos = obter_caminhos_limpos()

    print("\n=== 📂 ARQUIVOS DETECTADOS EM csvs/limpos ===")
    for arq in arquivos:
        print(" -", os.path.basename(arq))

    for csv in arquivos:
        process_csv(csv)

    print("\n🎉 Todos os arquivos foram importados com sucesso!")


if __name__ == "__main__":
    main()
