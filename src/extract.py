import pandas as pd

def ler_csv_bruto(caminho: str) -> pd.DataFrame:
    """
    Lê um CSV da pasta raw, detectando o separador.
    """
    try:
        # Usa sep=None e engine='python' para detectar o separador (como no seu script)
        return pd.read_csv(caminho, sep=None, engine="python", encoding="utf-8")
    except Exception as e:
        print(f"Erro ao ler o arquivo {caminho}: {e}")
        return pd.DataFrame()
    