import pandas as pd
import sys
# A importação deve trazer a variável 'TABELAS_CONFIG' (plural)
# Assumindo que o arquivo se chama 'tabelas_config.py'
from src.tabel_config import * 

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


def remover_duplicatas_por_completude(df: pd.DataFrame, nome_tabela: str) -> (pd.DataFrame, pd.DataFrame): # type: ignore
    """
    Limpa dados "lixo" (sem chave primária) e remove duplicatas com 
    base no título, mantendo a linha mais completa.
    
    Retorna dois DataFrames: (df_limpo, df_removidos)
    """
    
    # --- PASSO 1: FILTRAR LIXO (COM BASE NA CHAVE PRIMÁRIA) ---
    coluna_chave_bruta = None
    if nome_tabela == "projetos":
        coluna_chave_bruta = '\ufeffCódigo' # Chave bruta do CSV de projetos
    # --- CORREÇÃO: Usando o nome de tabela correto vindo do main.py ---
    elif nome_tabela == "projeto_historico": 
        coluna_chave_bruta = 'Número do Projeto' # Chave bruta do CSV de histórico
    # A tabela 'pesquisa' não tem chave primária, então pulamos este filtro para ela.

    df_filtrado_lixo = df.copy()
    
    if coluna_chave_bruta and coluna_chave_bruta in df_filtrado_lixo.columns:
        df_filtrado_lixo[coluna_chave_bruta] = df_filtrado_lixo[coluna_chave_bruta].astype(str).str.strip().str.lower()
        
        linhas_antes_filtro_nan = len(df_filtrado_lixo)
        
        df_filtrado_lixo = df_filtrado_lixo[
            (df_filtrado_lixo[coluna_chave_bruta] != "nan") & 
            (df_filtrado_lixo[coluna_chave_bruta] != "")
        ]
        
        linhas_removidas_nan = linhas_antes_filtro_nan - len(df_filtrado_lixo)
        if linhas_removidas_nan > 0:
            print(f"   -> {linhas_removidas_nan} linhas 'lixo' removidas (sem CHAVE PRIMÁRIA válida).")
    
    # --- PASSO 2: DEDUPLICAR (COM BASE NO TÍTULO) ---
    possiveis_colunas_titulo = ["Título do Projeto", "Título"]
    coluna_titulo = None
    for c in possiveis_colunas_titulo:
        if c in df_filtrado_lixo.columns:
            coluna_titulo = c
            break

    if not coluna_titulo:
        print(f"   -> Aviso: Coluna de título '{coluna_titulo}' não encontrada. Pulando deduplicação por título.")
        return df_filtrado_lixo, pd.DataFrame(columns=df_filtrado_lixo.columns)

    df_copia = df_filtrado_lixo.copy() 
    df_copia[coluna_titulo] = df_copia[coluna_titulo].astype(str).str.strip().str.lower()
    
    df_copia["_completude"] = df_copia.notna().sum(axis=1)
    df_ordenado = df_copia.sort_values(by="_completude", ascending=False)

    coluna_pk = None
    if nome_tabela == "projetos":
        coluna_pk = '\ufeffCódigo'
    # --- CORREÇÃO: Usando o nome de tabela correto vindo do main.py ---
    elif nome_tabela == "projeto_historico":
        coluna_pk = 'Número do Projeto'

    if coluna_pk and coluna_pk in df_ordenado.columns:
        df_sem_duplicatas = df_ordenado.drop_duplicates(subset=[coluna_pk], keep="first")
    else:
        df_sem_duplicatas = df_ordenado.drop_duplicates(subset=[coluna_titulo], keep="first")

    df_removidos = df_copia.loc[~df_copia.index.isin(df_sem_duplicatas.index)]

    df_sem_duplicatas = df_sem_duplicatas.drop(columns=["_completude"])
    df_removidos = df_removidos.drop(columns=["_completude"], errors='ignore')

    if not df_removidos.empty:
         print(f"   -> {len(df_removidos)} duplicatas (menos completas, por Chave/Título) removidas.")
    
    return df_sem_duplicatas, df_removidos

def renomear_colunas_para_sql(df: pd.DataFrame, mapa_colunas: dict, nome_tabela: str) -> pd.DataFrame:
    """
    Renomeia e filtra as colunas do DataFrame para bater exatamente
    com o mapa_colunas fornecido.
    """
    print(f"   Iniciando renomeação para a tabela: {nome_tabela}")
    
    if not mapa_colunas:
        print(f"Aviso [Transform]: Nenhum mapa de colunas definido para a tabela '{nome_tabela}'. O DataFrame não será alterado.")
        return df

    try:
        df_renomeado = df.rename(columns=mapa_colunas)
    except Exception as e:
        print(f"Erro [Transform] ao tentar renomear colunas: {e}", file=sys.stderr)
        return pd.DataFrame() 

    colunas_sql = list(mapa_colunas.values())
    
    colunas_faltantes = [col for col in colunas_sql if col not in df_renomeado.columns]
    
    if colunas_faltantes:
        print(f"Erro [Transform]: Após a renomeação, as seguintes colunas SQL estão FALTANDO no DataFrame: {colunas_faltantes}", file=sys.stderr)
        print("   Isso geralmente significa que a(s) coluna(s) de origem no CSV estão com o nome errado no 'mapa_colunas'.", file=sys.stderr)
        print(f"   Colunas disponíveis no DF (após renomear): {list(df_renomeado.columns)}", file=sys.stderr)
        return pd.DataFrame() 

    try:
        df_final = df_renomeado[colunas_sql]
        print(f"   -> Renomeação para '{nome_tabela}' concluída.")
        return df_final
    except KeyError as e:
        print(f"Erro [Transform] Inesperado ao filtrar colunas finais: A coluna {e} não foi encontrada.", file=sys.stderr)
        print(f"   Colunas esperadas: {colunas_sql}", file=sys.stderr)
        print(f"   Colunas disponíveis: {list(df_renomeado.columns)}", file=sys.stderr)
        return pd.DataFrame()
    

def corrigir_tipos_de_dados(df: pd.DataFrame, config: dict, nome_tabela: str) -> pd.DataFrame:
    """
    Converte colunas de data/hora, booleanos e numéricos para formatos compatíveis com o SQL.
    Lê a configuração de um dicionário 'config'.
    """
    print(f"   Iniciando correção de tipos para a tabela: {nome_tabela}")
    
    mapa_booleano = {
        'Sim': True, 'Não': False,
        ' Sim ': True, ' Não ': False,
        'SIM': True, 'NAO': False
    }

    colunas_data = config.get('colunas_data', [])
    colunas_timestamp = config.get('colunas_timestamp', [])
    colunas_booleano = config.get('colunas_booleano', [])
    colunas_numericas = config.get('colunas_numericas', [])

    try:
        # --- CORREÇÃO: Adicionado .dt.date para enviar YYYY-MM-DD ao invés de timestamp ---
        for col in colunas_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce').dt.date

        for col in colunas_timestamp:
             if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
        
        for col in colunas_booleano:
            if col in df.columns:
                df[col] = df[col].str.strip().map(mapa_booleano)
        
        for col in colunas_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',', '.'), 
                    errors='coerce'
                )

        print(f"   -> Correção de tipos para '{nome_tabela}' concluída.")
        
    except Exception as e:
        print(f"Erro [Transform] ao corrigir tipos de dados: {e}", file=sys.stderr)
        return pd.DataFrame() 

    return df


def aplicar_transformacoes_customizadas(df: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    """
    Aplica regras de negócio específicas que não são genéricas.
    """
    print(f"   Aplicando transformações customizadas para: {nome_tabela}")

    try:
        # --- CORREÇÃO 1: Adicionado o bloco para 'projetos' que corrige o BIGINT ---
        if nome_tabela == "projetos":
            col_name = "codigo"
            if col_name in df.columns:
                temp_numeric = pd.to_numeric(df[col_name], errors='coerce')
                temp_int = temp_numeric.astype(pd.Int64Dtype())
                df[col_name] = temp_int.astype(str)
                df[col_name] = df[col_name].replace('<NA>', None)
        
        # --- CORREÇÃO 2: Usando o nome de tabela correto vindo do main.py ---
        elif nome_tabela == "projeto_historico": 
            col_name = "numero do projeto"
            if col_name in df.columns:
                temp_numeric = pd.to_numeric(df[col_name], errors='coerce')
                temp_int = temp_numeric.astype(pd.Int64Dtype())
                df[col_name] = temp_int.astype(str)
                df[col_name] = df[col_name].replace('<NA>', None)
        
    except Exception as e:
        print(f"Erro [Transform] ao aplicar transformação customizada: {e}", file=sys.stderr)
        return pd.DataFrame()
        
    print(f"   -> Transformações customizadas concluídas.")
    return df


def processar_dataframe_para_sql(df_original: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    """
    Orquestra o processo de transformação completo para um DataFrame.
    """
    print(f"Iniciando processamento completo para a tabela: {nome_tabela}")
    
    # --- CORREÇÃO: Usando a variável 'TABELAS_CONFIG' (plural) que seu log de erro mostrou ---
    if nome_tabela not in TABEL_CONFIG:
        print(f"Erro [Processar]: Tabela '{nome_tabela}' não encontrada em TABELAS_CONFIG.", file=sys.stderr)
        return pd.DataFrame()
        
    config = TABEL_CONFIG[nome_tabela]
    # -----------------------------------------------------------------------------------
    
    mapa_colunas = config.get('mapa_colunas', {})
    
    df = df_original.copy()

    df = renomear_colunas_para_sql(df, mapa_colunas, nome_tabela)
    if df.empty:
        print(f"Erro [Processar]: Falha na etapa de renomeação para '{nome_tabela}'.", file=sys.stderr)
        return pd.DataFrame()

    df = corrigir_tipos_de_dados(df, config, nome_tabela)
    if df.empty:
        print(f"Erro [Processar]: Falha na etapa de correção de tipos para '{nome_tabela}'.", file=sys.stderr)
        return pd.DataFrame()
        
    df = aplicar_transformacoes_customizadas(df, nome_tabela)
    if df.empty:
        print(f"Erro [Processar]: Falha na etapa de transformações customizadas para '{nome_tabela}'.", file=sys.stderr)
        return pd.DataFrame()

    print(f"Processamento para '{nome_tabela}' concluído com sucesso.")
    return df