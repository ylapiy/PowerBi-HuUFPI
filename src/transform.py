import pandas as pd

def filtrar_projetos_inativos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas onde o status é 'Projeto inativo'.
    """
    possiveis_colunas = ["Estado do projeto", "Status O que Aconteceu"]
    
    coluna_status = None
    for coluna in possiveis_colunas:
        if coluna in df.columns:
            coluna_status = coluna
            break
            
    if not coluna_status:
        print(f"   -> Aviso: Coluna de status não encontrada. Pulando filtro de inativos.")
        return df  # Retorna o DataFrame original se a coluna não existir

    linhas_antes = len(df)
    df_filtrado = df[df[coluna_status] != "Projeto inativo"].copy()
    linhas_removidas = linhas_antes - len(df_filtrado)

    if linhas_removidas > 0:
        print(f"   -> {linhas_removidas} projetos inativos removidos.")
        
    return df_filtrado


def remover_duplicatas_por_completude(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame): # type: ignore
    """
    Remove duplicatas com base no título, mantendo a linha mais completa.
    
    Retorna dois DataFrames: (df_limpo, df_removidos)
    """
    possiveis_colunas_titulo = ["Título do Projeto", "Título"]

    coluna_titulo = None
    for c in possiveis_colunas_titulo:
        if c in df.columns:
            coluna_titulo = c
            break

    if not coluna_titulo:
        print(f"   -> Aviso: Coluna de título não encontrada. Pulando remoção de duplicatas.")
        # Retorna o DF original e um DF vazio para os removidos
        return df, pd.DataFrame(columns=df.columns)

    # 1. Normaliza para evitar falsos positivos
    df_copia = df.copy()
    df_copia[coluna_titulo] = df_copia[coluna_titulo].astype(str).str.strip().str.lower()

    # 2. Calcula a "completude" (número de campos preenchidos)
    df_copia["_completude"] = df_copia.notna().sum(axis=1)
    
    # 3. Ordena por completude (maior primeiro)
    df_ordenado = df_copia.sort_values(by="_completude", ascending=False)

    # 4. Remove duplicatas, mantendo a mais completa (a 'first' após ordenar)
    df_sem_duplicatas = df_ordenado.drop_duplicates(subset=[coluna_titulo], keep="first")
    
    # 5. Identifica as duplicatas que foram removidas
    df_removidos = df_copia.loc[~df_copia.index.isin(df_sem_duplicatas.index)]

    # 6. Remove coluna auxiliar de ambos os DataFrames
    df_sem_duplicatas = df_sem_duplicatas.drop(columns=["_completude"])
    df_removidos = df_removidos.drop(columns=["_completude"], errors='ignore')

    if not df_removidos.empty:
         print(f"   -> {len(df_removidos)} duplicatas (menos completas) removidas.")
    
    return df_sem_duplicatas, df_removidos
