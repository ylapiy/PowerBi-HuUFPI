import pandas as pd
import os
import glob

pasta_entrada = r"csvs/limpos"
pasta_saida = r"csvs/limpos"

possiveis_colunas_titulo = ["Título do Projeto", "Título"]

arquivos = glob.glob(os.path.join(pasta_entrada, "*_filtrado.csv"))

if not arquivos:
    print("Nenhum arquivo filtrado encontrado em limpos/")
else:
    for arquivo in arquivos:
        try:
            nome = os.path.basename(arquivo)
            print(f"\nRemovendo duplicatas em: {nome}")

            df = pd.read_csv(arquivo, sep=None, engine="python", encoding="utf-8")

            coluna_titulo = None
            for c in possiveis_colunas_titulo:
                if c in df.columns:
                    coluna_titulo = c
                    break

            if not coluna_titulo:
                print(f"Nenhuma coluna de título encontrada. Pulando {nome}.")
                continue

            df[coluna_titulo] = df[coluna_titulo].astype(str).str.strip().str.lower()

            df["completude"] = df.notna().sum(axis=1)
            df_ordenado = df.sort_values(by="completude", ascending=False)
            df_sem_dup = df_ordenado.drop_duplicates(
                subset=[coluna_titulo], keep="first"
            )

            df_sem_dup = df_sem_dup.drop(columns=["completude"], errors="ignore")

            saida = os.path.join(
                pasta_saida, nome.replace("_filtrado.csv", "_limpo.csv")
            )

            df_sem_dup.to_csv(saida, index=False, encoding="utf-8")

            print(f"OK → Arquivo final salvo como: {os.path.basename(saida)}")

            # 🔥 Agora remove o arquivo filtrado original
            os.remove(arquivo)
            print(f"🗑️ Arquivo filtrado removido: {nome}")

        except Exception as e:
            print(f"Erro em {nome}: {e}")

print("\nRemoção de duplicatas concluída!")
