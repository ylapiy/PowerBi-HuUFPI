import pandas as pd
import os
import glob

# Pasta onde estão os CSVs
pasta = r"csvs\filtrados"

# Nome do arquivo onde as duplicatas serão salvas
pasta_saida = r"csvs\sem_duplicatas"
pasta_removidos = r"csvs\removidos"
os.makedirs(pasta_saida, exist_ok=True)
os.makedirs(pasta_removidos, exist_ok=True)

# Possíveis nomes da coluna de título
possiveis_colunas_titulo = ["Título do Projeto", "Título"]

# Encontra todos os arquivos CSV na pasta
arquivos = glob.glob(os.path.join(pasta, "*.csv"))

# DataFrame acumulador
duplicatas_resumo = []

if not arquivos:
    print("Nenhum arquivo CSV encontrado na pasta informada.")
else:
    for arquivo in arquivos:
        try:
            nome_arquivo = os.path.basename(arquivo)
            print(f"\nVerificando duplicatas em: {nome_arquivo}")

            # Detecta automaticamente o separador
            df = pd.read_csv(arquivo, sep=None, engine="python", encoding="utf-8")

            # Encontra qual coluna usar
            coluna_titulo = None
            for c in possiveis_colunas_titulo:
                if c in df.columns:
                    coluna_titulo = c
                    break

            if not coluna_titulo:
                print(f"Nenhuma coluna de título encontrada em {nome_arquivo}. Pulando arquivo.")
                print(f"   Colunas disponíveis: {list(df.columns)}")
                continue
            
            # Normaliza para evitar falsos positivos (maiúsculas, espaços, etc.)
            df[coluna_titulo] = df[coluna_titulo].astype(str).str.strip().str.lower()

            # Calcula a "completude" (número de campos preenchidos) para cada linha
            df["completude"] = df.notna().sum(axis=1)
            
            # Ordena por completude (maior primeiro)
            df_ordenado = df.sort_values(by ="completude", ascending=False)

            # Remove duplicatas, mantendo a mais completa
            df_sem_duplicatas = df_ordenado.drop_duplicates(subset=[coluna_titulo], keep="first")
            
            #Identifica as duplicatas removidas
            duplicatas = df.loc[~df.index.isin(df_sem_duplicatas.index)].copy()

            # Remove coluna auxiliar
            df_sem_duplicatas = df_sem_duplicatas.drop(columns=["completude"])
            duplicatas = duplicatas.drop(columns=["completude"], errors='ignore')
            
            # Caminhos de saída
            saida_sem_duplicatas = os.path.join(pasta_saida, nome_arquivo.replace(".csv", "_sem_duplicatas.csv"))
            saida_removidos = os.path.join(pasta_removidos, nome_arquivo.replace(".csv", "_duplicatas_removidas.csv"))
            
            # Salva os arquivos
            df_sem_duplicatas.to_csv(saida_sem_duplicatas, index=False, encoding="utf-8")
            duplicatas.to_csv(saida_removidos, index=False, encoding="utf-8")
            
            print(f"✅ {nome_arquivo}: {len(duplicatas)} duplicatas removidas.")
            print(f"   ↳ Arquivo limpo: {os.path.basename(saida_sem_duplicatas)}")
            print(f"   ↳ Backup dos removidos: {os.path.basename(saida_removidos)}")


        except Exception as e:
            print(f"Erro ao processar {nome_arquivo}: {e}")

print("\nVerificação concluída!")
