import pandas as pd
import sys

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
    # Este é o filtro que remove linhas como a "linha 733" do log anterior.
    
    coluna_chave_bruta = None
    if nome_tabela == "projetos":
        coluna_chave_bruta = '\ufeffCódigo' # Chave bruta do CSV de projetos
    elif nome_tabela == "historico_status_projetos":
        coluna_chave_bruta = 'Número do Projeto' # Chave bruta do CSV de histórico
    # A tabela 'pesquisa' não tem chave primária, então pulamos este filtro para ela.

    df_filtrado_lixo = df.copy()
    
    if coluna_chave_bruta and coluna_chave_bruta in df_filtrado_lixo.columns:
        # Normaliza a chave para verificação
        df_filtrado_lixo[coluna_chave_bruta] = df_filtrado_lixo[coluna_chave_bruta].astype(str).str.strip().str.lower()
        
        linhas_antes_filtro_nan = len(df_filtrado_lixo)
        
        # Filtra mantendo apenas linhas onde a CHAVE NÃO é 'nan' E NÃO é ''
        df_filtrado_lixo = df_filtrado_lixo[
            (df_filtrado_lixo[coluna_chave_bruta] != "nan") & 
            (df_filtrado_lixo[coluna_chave_bruta] != "")
        ]
        
        linhas_removidas_nan = linhas_antes_filtro_nan - len(df_filtrado_lixo)
        if linhas_removidas_nan > 0:
            print(f"   -> {linhas_removidas_nan} linhas 'lixo' removidas (sem CHAVE PRIMÁRIA válida).")
    
    # --- PASSO 2: DEDUPLICAR (COM BASE NO TÍTULO) ---
    # Esta é a sua lógica original, que agora roda no DataFrame já limpo.
    
    possiveis_colunas_titulo = ["Título do Projeto", "Título"]
    coluna_titulo = None
    for c in possiveis_colunas_titulo:
        if c in df_filtrado_lixo.columns:
            coluna_titulo = c
            break

    if not coluna_titulo:
        print(f"   -> Aviso: Coluna de título '{coluna_titulo}' não encontrada. Pulando deduplicação por título.")
        # Retorna o DF filtrado por lixo e um DF vazio para removidos
        return df_filtrado_lixo, pd.DataFrame(columns=df_filtrado_lixo.columns)

    # 1. Normaliza o Título
    df_copia = df_filtrado_lixo.copy() # Usa o DF que já passou pelo filtro de lixo
    df_copia[coluna_titulo] = df_copia[coluna_titulo].astype(str).str.strip().str.lower()
    
    # (O filtro de 'nan' no TÍTULO não é mais necessário aqui, 
    # pois o que importa é a CHAVE PRIMÁRIA)

    # 2. Calcula a "completude"
    df_copia["_completude"] = df_copia.notna().sum(axis=1)
    
    # 3. Ordena por completude
    df_ordenado = df_copia.sort_values(by="_completude", ascending=False)

    # 4. Remove duplicatas 
    # Preferência: deduplicar pela chave primária se existir
    coluna_pk = None
    if nome_tabela == "projetos":
        coluna_pk = '\ufeffCódigo'
    elif nome_tabela == "historico_status_projetos":
        coluna_pk = 'Número do Projeto'

    if coluna_pk and coluna_pk in df_ordenado.columns:
        # Deduplicação CORRETA: por chave primária, não por título
        df_sem_duplicatas = df_ordenado.drop_duplicates(subset=[coluna_pk], keep="first")
    else:
        # Caso realmente não tenha chave (ex: tabela 'pesquisa')
        df_sem_duplicatas = df_ordenado.drop_duplicates(subset=[coluna_titulo], keep="first")

    # 5. Identifica as duplicatas que foram removidas
    df_removidos = df_copia.loc[~df_copia.index.isin(df_sem_duplicatas.index)]

    # 6. Remove coluna auxiliar
    df_sem_duplicatas = df_sem_duplicatas.drop(columns=["_completude"])
    df_removidos = df_removidos.drop(columns=["_completude"], errors='ignore')

    if not df_removidos.empty:
         print(f"   -> {len(df_removidos)} duplicatas (menos completas, por Título) removidas.")
    
    return df_sem_duplicatas, df_removidos



def renomear_colunas_para_sql(df: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    """
    Renomeia as colunas do DataFrame para bater exatamente com as colunas
    da tabela SQL de destino.
    
    Os nomes das colunas de origem (à esquerda) foram CORRIGIDOS
    com base no seu log de erro de 04/11/2025.
    """
    print(f"   Iniciando renomeação para a tabela: {nome_tabela}")
    mapa_colunas = {}

    if nome_tabela == "projetos":
        # Mapeia de 'relatorio_projetos (870)(in).csv' para a tabela 'projetos'
        mapa_colunas = {
            # --- Nomes de Coluna de Origem (do seu CSV) --- : --- Nomes de Coluna do SQL ---
            
            # CORREÇÃO: Adicionado \ufeff (BOM) na primeira coluna
            '\ufeffCódigo': 'codigo',
            'Título': 'titulo', # Esta estava correta
            'Estado do projeto': 'estado_projeto',
            'Data de solicitação': 'data_solicitacao',
            'Última atualização': 'data_ultima_atualizacao',
            'HUF': 'huf', # Esta estava correta
            'Instituição proponente': 'instituicao_proponente',
            'Classificação institucional': 'classificacao_institucional',
            'Pesquisa Acadêmica': 'pesquisa_academica',
            'Envolve seres humanos?': 'envolve_seres_humanos',
            
            'Palavras-chave': 'palavras_chave', # Esta estava correta
            'População alvo': 'populacao_alvo',
            'Tamanho da amostra': 'tamanho_amostra',
            'Produtos esperados': 'produtos_esperados',
            'Formação Academica': 'formacao_academica',
            'Tipo de dados': 'tipo_dados',
            'Curso/Área de Conhecimento': 'curso_area_conhecimento', # Esta estava correta
            'Abordagem da pesquisa': 'abordagem_pesquisa',
            'Delineamento do estudo': 'delineamento_estudo',
            'Tipo de estudo': 'tipo_estudo',
            'CID': 'cid', # Esta estava correta
            
            'É estudo multicêntrico?': 'eh_estudo_multicentrico',
            'É coordenador?': 'eh_coordenador',
            'Instituição coordenadora de estudo': 'instituicao_coordenadora_estudo',
            'É projeto com co-participação?': 'eh_projeto_coparticipacao',
            'Outras Instituições participantes': 'outras_instituicoes_participantes',
            
            'Tipo de fomento': 'tipo_fomento',
            'Instituição de fomento': 'instituicao_fomento',
            'Bolsas (R$)': 'valor_bolsas',
            'Recurso de custeio (R$)': 'valor_custeio',
            'Recursos de capital (R$)': 'valor_capital',
            'Recursos total do projeto (R$)': 'valor_total_projeto',
            
            'Inicío do projeto (previsão)': 'data_inicio_previsao',
            'Fim do projeto (previsão)': 'data_fim_previsao',
            'Há previsão de coleta de dados no HUF?': 'previsao_coleta_dados_huf',
            'Inicío da coleta de dados (previsão)': 'data_inicio_coleta_previsao',
            'Fim da coleta de dados (previsão)': 'data_fim_coleta_previsao',
            'Local da coleta de dados': 'local_coleta_dados',
            'Possui aprovação do Comitê de Ética?': 'possui_aprovacao_comite_etica'
        }
        
    elif nome_tabela == "historico_status_projetos":
        # Mapeia de 'relatorio_projetos_historico(870)(in).csv' para a tabela 'historico_status_projetos'
        mapa_colunas = {
            # --- Nomes de Coluna de Origem (do seu CSV) --- : --- Nomes de Coluna do SQL ---
            
            # CORREÇÃO: Adicionado \ufeff (BOM)
            '\ufeffHospital Universitário (HU)': 'hospital_universitario_hu',
            'Número do Projeto': 'numero_projeto',
            'Título do Projeto': 'titulo_projeto',
            'Status Onde Estava': 'status_onde_estava',
            
            # CORREÇÃO: Removidos parênteses
            'Status O que Aconteceu': 'status_o_que_aconteceu',
            'Status do Projeto': 'status_projeto_atual',
            'Quem Fez': 'quem_fez',
            
            # CORREÇÃO: Alterado "data e hora" para "Data/Hora"
            'Quando Fez (Data/Hora)': 'quando_fez_data_hora',
            'Duração': 'duracao'
        }
        
    elif nome_tabela == "pesquisa": # Mapeia para a tabela 'projetos_pesquisa_simplificado'
        # Mapeia de 'relatorio_acompanhamento_projetos (870)(in) (1).csv'
        mapa_colunas = {
            # --- Nomes de Coluna de Origem (do seu CSV) --- : --- Nomes de Coluna do SQL ---
            
            # CORREÇÃO: Adicionado \ufeff (BOM)
            '\ufeffHU': 'hu',
            'Título do Projeto': 'titulo_projeto',
            'Classificação Institucional da Pesquisa': 'classificacao_institucional_pesquisa',
            
            # CORREÇÃO: Capitalização
            'Projeto envolve seres humanos': 'envolve_seres_humanos',
            'Delineamento do Estudo': 'delineamento_estudo',
            'Tipo de Estudo': 'tipo_estudo',
            
            # CORREÇÃO: Capitalização
            'É estudo multicêntrico': 'eh_estudo_multicentrico',
            'É projeto com co-participação': 'eh_projeto_com_coparticipacao',
            'Estado do projeto': 'estado_projeto',
            
            # Estas estavam corretas
            'Tempo decorrido entre status': 'tempo_decorrido_entre_status_1',
            'Tempo decorrido entre status.1': 'tempo_decorrido_entre_status_2'
        }
        
    else:
        print(f"Aviso [Transform]: Nenhum mapa de colunas definido para a tabela '{nome_tabela}'. O DataFrame não será alterado.")
        return df

    # Aplica a renomeação
    try:
        df_renomeado = df.rename(columns=mapa_colunas)
    except Exception as e:
        print(f"Erro [Transform] ao tentar renomear colunas: {e}")
        return pd.DataFrame() # Retorna DF vazio

    # Filtra o DataFrame para conter APENAS as colunas que serão carregadas no SQL
    colunas_sql = list(mapa_colunas.values())
    
    # Verifica se todas as colunas SQL esperadas existem no DataFrame renomeado
    # Esta verificação é crucial e foi ela que nos avisou do erro
    colunas_faltantes = [col for col in colunas_sql if col not in df_renomeado.columns]
    
    if colunas_faltantes:
        print(f"Erro [Transform]: Após a renomeação, as seguintes colunas SQL estão FALTANDO no DataFrame: {colunas_faltantes}")
        print("   Isso geralmente significa que a(s) coluna(s) de origem no CSV estão com o nome errado no 'mapa_colunas'.")
        print(f"   Colunas disponíveis no DF (após renomear): {list(df_renomeado.columns)}")
        return pd.DataFrame() # Retorna DF vazio para parar o processo

    try:
        df_final = df_renomeado[colunas_sql]
        print(f"   -> Renomeação para '{nome_tabela}' concluída. Colunas prontas: {list(df_final.columns)}")
        return df_final
    except KeyError as e:
        # Esta verificação dupla é um 'cinto de segurança'
        print(f"Erro [Transform] Inesperado ao filtrar colunas finais: A coluna {e} não foi encontrada.")
        print(f"   Colunas esperadas: {colunas_sql}")
        print(f"   Colunas disponíveis: {list(df_renomeado.columns)}")
        return pd.DataFrame()
    

def corrigir_tipos_de_dados(df: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    """
    Converte colunas de data/hora, booleanos e numéricos para formatos compatíveis com o SQL.
    - Datas (DD/MM/YYYY) -> Formato ISO (YYYY-MM-DD)
    - Booleanos (Sim/Não) -> (True/False)
    - Numéricos (0,00) -> (0.00)
    """
    print(f"   Iniciando correção de tipos para a tabela: {nome_tabela}")
    
    # Mapeamento universal para 'Sim'/'Não'
    mapa_booleano = {
        'Sim': True,
        'Não': False,
        ' Sim ': True,
        ' Não ': False,
        'SIM': True,
        'NAO': False
    }

    colunas_data = []
    colunas_timestamp = []
    colunas_booleano = []
    colunas_numericas = [] # <-- NOVA LISTA

    if nome_tabela == "projetos":
        colunas_data = [
            'data_solicitacao', 'data_inicio_previsao', 'data_fim_previsao',
            'data_inicio_coleta_previsao', 'data_fim_coleta_previsao'
        ]
        colunas_timestamp = [
            'data_ultima_atualizacao' 
        ]
        colunas_booleano = [
            'envolve_seres_humanos', 'eh_estudo_multicentrico', 'eh_coordenador',
            'eh_projeto_coparticipacao', 'previsao_coleta_dados_huf',
            'possui_aprovacao_comite_etica'
        ]
        # <-- NOVA SEÇÃO PARA CORRIGIR VALORES NUMÉRICOS (R$) -->
        colunas_numericas = [
            'valor_bolsas', 'valor_custeio', 'valor_capital', 'valor_total_projeto', 'tamanho_amostra'
        ]
    
    elif nome_tabela == "historico_status_projetos":
        colunas_timestamp = [
            'quando_fez_data_hora' 
        ]

        try:
            if 'numero_projeto' in df.columns:
                # 1. Converte para numérico (float). errors='coerce' transforma lixo em Nulo (NaT)
                temp_numeric = pd.to_numeric(df['numero_projeto'], errors='coerce')
                
                # 2. Converte para Inteiro Nulável (ex: 2194.0 -> 2194, NaT -> <NA>)
                temp_int = temp_numeric.astype(pd.Int64Dtype())
                
                # 3. Converte de volta para string (ex: 2194 -> "2194")
                df['numero_projeto'] = temp_int.astype(str)
                
                # 4. (Cinto de segurança) Substitui o string '<NA>' por None
                #    para que o 'salvar_csv' o transforme em '' (que o COPY entende como NULL)
                df['numero_projeto'] = df['numero_projeto'].replace('<NA>', None)
                
        except Exception as e:
            print(f"Erro [Transform] ao normalizar 'numero_projeto': {e}", file=sys.stderr)
            return pd.DataFrame()

    elif nome_tabela == "pesquisa":
        colunas_booleano = [
            'envolve_seres_humanos', 
            'eh_estudo_multicentrico',
            'eh_projeto_com_coparticipacao'
        ]

    try:
        # 1. Corrige colunas de DATA (sem hora)
        for col in colunas_data:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

        # 2. Corrige colunas de TIMESTAMP (data E hora)
        for col in colunas_timestamp:
             if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
        
        # 3. Corrige colunas BOOLEANAS
        for col in colunas_booleano:
            if col in df.columns:
                df[col] = df[col].str.strip().map(mapa_booleano)
        
        # 4. <-- NOVA ETAPA: Corrige colunas NUMÉRICAS -->
        for col in colunas_numericas:
            if col in df.columns:
                # 1. Converte para string (para garantir)
                # 2. Substitui a vírgula (R$) pelo ponto (SQL)
                # 3. Converte para numérico. 'coerce' transforma falhas em NaT (Nulo)
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',', '.'), 
                    errors='coerce'
                )

        print(f"   -> Correção de tipos para '{nome_tabela}' concluída.")
        
    except Exception as e:
        print(f"Erro [Transform] ao corrigir tipos de dados: {e}")
        return pd.DataFrame() # Retorna DF vazio em caso de erro

    return df
