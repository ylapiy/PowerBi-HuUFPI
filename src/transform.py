import pandas as pd
import os
import glob


class transform:
    def __init__(self):
        pass


    def limpar_duplicatas(self):
        # Pasta onde estão os CSVs
        pasta = r"PowerBi-HuUFPI/data/processed/filtrados"

        # Nome do arquivo onde as duplicatas serão salvas
        pasta_saida = r"PowerBi-HuUFPI/data/processed/sem_duplicatas"
        pasta_removidos = r"PowerBi-HuUFPI/data/processed/removidos"
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






    def limpar_projetos_inativos(self):
        pasta = r"PowerBi-HuUFPI/data/raw/brutos"

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
