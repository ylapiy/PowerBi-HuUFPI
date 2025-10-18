import pandas as pd
import os
import glob

# 📂 Pasta onde estão os CSVs
pasta = r"csvs\filtrados"

# 📁 Nome do arquivo onde as duplicatas serão salvas
arquivo_saida = "duplicatas_por_titulo.csv"

# 🧩 Possíveis nomes da coluna de título
possiveis_colunas_titulo = ["Título do Projeto", "Título"]

# 🔍 Encontra todos os arquivos CSV na pasta
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


            # Identifica duplicatas com base no título
            duplicatas = df[df.duplicated(subset=[coluna_titulo], keep=False)]

            if not duplicatas.empty:
                # Extrai apenas os títulos únicos duplicados
                titulos_duplicados = sorted(duplicatas[coluna_titulo].unique().tolist())

                for titulo in titulos_duplicados:
                    duplicatas_resumo.append({
                        "Arquivo": nome_arquivo,
                        "Título Duplicado": titulo
                    })

                print(f"{len(titulos_duplicados)} títulos duplicados encontrados em {nome_arquivo}.")
            else:
                print("Nenhuma duplicata encontrada.")

        except Exception as e:
            print(f"Erro ao processar {nome_arquivo}: {e}")

# Salva todas as duplicatas encontradas em um único arquivo
if duplicatas_resumo:
    resumo_df = pd.DataFrame(duplicatas_resumo)
    resumo_df.to_csv(arquivo_saida, index=False, encoding="utf-8")
    print(f"\n📄 Resumo de duplicatas salvo em: {arquivo_saida}")
    print(f"Total de títulos duplicados: {len(resumo_df)}")
else:
    print("\nNenhuma duplicata encontrada em nenhum arquivo.")

print("\nVerificação concluída!")
