import pandas as pd
import os
import glob

# Pasta onde estão os CSVs (coloca o caminho entre aspas)
pasta = r"csvs\brutos"

# Opção: sobrescrever os arquivos originais (modo destrutivo)
modo_destrutivo = False  # mude para True se quiser sobrescrever os arquivos originais

# Cria pasta de saída (apenas se não for destrutivo)
if not modo_destrutivo:
    pasta_saida = os.path.join(pasta, "filtrados")
    os.makedirs(pasta_saida, exist_ok=True)

# Encontra todos os arquivos CSV na pasta
arquivos = glob.glob(os.path.join(pasta, "*.csv"))

# Possíveis nomes de coluna
possiveis_colunas = ["Estado do projeto", "Status O que Aconteceu"]

if not arquivos:
    print("Nenhum arquivo CSV encontrado na pasta informada.")
else:
    for arquivo in arquivos:
        try:
            nome_arquivo = os.path.basename(arquivo)
            if modo_destrutivo:
                saida = arquivo  # sobrescreve o original
            else:
                saida = os.path.join(pasta_saida, nome_arquivo.replace(".csv", "_filtrado.csv"))

            print(f"\n Processando: {nome_arquivo}")

            # Detecta automaticamente o separador e lê o arquivo
            df = pd.read_csv(arquivo, sep=None, engine="python", encoding="utf-8")

            # Procura qual das colunas existe
            coluna_status = None
            
            for coluna in possiveis_colunas:
                if coluna in df.columns:
                    coluna_status = coluna
                    break
                
            if not coluna_status:
                print(f" Coluna de status não encontrada em {nome_arquivo}. Pulando arquivo.")
                continue
            
            # Filtra os projetos inativos

            linhas_antes = len(df)
            df_filtrado = df[df[coluna_status] != "Projeto inativo"]
            linhas_removidas = linhas_antes - len(df_filtrado)

            df_filtrado.to_csv(saida, index=False, encoding="utf-8")

            if modo_destrutivo:
                print(f" {nome_arquivo}: {linhas_removidas} linhas removidas (arquivo original sobrescrito).")
            else:
                print(f" {nome_arquivo}: {linhas_removidas} linhas removidas → salvo em 'filtrados/'")

        except Exception as e:
            print(f" Erro ao processar {nome_arquivo}: {e}")

print("\n Processo concluído!")
