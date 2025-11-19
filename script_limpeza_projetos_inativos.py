import pandas as pd
import os
import glob
import shutil

pasta_entrada = r"csvs/brutos"
pasta_saida = r"csvs/limpos"

# Remove todos os arquivos da pasta limpos
if os.path.exists(pasta_saida):
    shutil.rmtree(pasta_saida)  # remove a pasta inteira
os.makedirs(pasta_saida, exist_ok=True)  # recria vazia

arquivos = glob.glob(os.path.join(pasta_entrada, "*.csv"))

possiveis_colunas = ["Estado do projeto", "Status O que Aconteceu"]

if not arquivos:
    print("Nenhum CSV encontrado em brutos/")
else:
    for arquivo in arquivos:
        try:
            nome = os.path.basename(arquivo)
            saida = os.path.join(pasta_saida, nome.replace(".csv", "_filtrado.csv"))

            print(f"\nProcessando: {nome}")

            df = pd.read_csv(arquivo, sep=None, engine="python", encoding="utf-8")

            coluna_status = None
            for c in possiveis_colunas:
                if c in df.columns:
                    coluna_status = c
                    break

            if not coluna_status:
                print(f"Coluna de status não encontrada. Pulando {nome}.")
                continue

            linhas_antes = len(df)
            df_filtrado = df[df[coluna_status] != "Projeto inativo"]
            removidas = linhas_antes - len(df_filtrado)

            df_filtrado.to_csv(saida, index=False, encoding="utf-8")

            print(f"{nome}: {removidas} linhas removidas → salvo em limpos/")

        except Exception as e:
            print(f"Erro em {nome}: {e}")

print("\nFiltro concluído!")
