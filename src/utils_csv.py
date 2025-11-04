import pandas as pd

def salvar_csv(df: pd.DataFrame, caminho_saida: str):
    """
    Salva um DataFrame em um arquivo CSV.
    """
    try:
        # Usando 'utf-8-sig' para garantir compatibilidade com Excel (abre acentos corretamente)
        df.to_csv(caminho_saida, index=False, sep=';', encoding='utf-8-sig')
        print(f"Arquivo CSV salvo em: {caminho_saida}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo {caminho_saida}: {e}")