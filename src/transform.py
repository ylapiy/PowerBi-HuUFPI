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


def renomear_colunas_para_sql(df: pd.DataFrame, nome_tabela: str) -> pd.DataFrame:
    """
    Renomeia as colunas do DataFrame para bater exatamente com as colunas
    da tabela SQL de destino.
    
    !! IMPORTANTE !!
    As chaves (à esquerda) são "chutes" de como as colunas se chamam
    no seu CSV bruto. Você DEVE verificar e corrigir esses nomes.
    Os valores (à direita) são os nomes exatos da sua tabela SQL (NÃO MUDE).
    """
    print(f"   Iniciando renomeação para a tabela: {nome_tabela}")
    mapa_colunas = {}

    if nome_tabela == "projetos":
        # Mapeia de 'projetos_limpo.csv' para a tabela 'projetos'
        mapa_colunas = {
            # --- VERIFIQUE E CORRIJA OS NOMES DAS COLUNAS DE ORIGEM (À ESQUERDA) ---
            
            # --- Bloco Principal ---
            'Código': 'codigo',
            'Título': 'titulo',
            'Estado do Projeto': 'estado_projeto',
            'Data da Solicitação': 'data_solicitacao',
            'Data da Última Atualização': 'data_ultima_atualizacao',
            'HUF': 'huf',
            'Instituição Proponente': 'instituicao_proponente',
            'Classificação Institucional': 'classificacao_institucional',
            'Pesquisa Acadêmica': 'pesquisa_academica',
            'Envolve Seres Humanos?': 'envolve_seres_humanos',
            
            # --- Bloco de Detalhes da Pesquisa ---
            'Palavras-chave': 'palavras_chave',
            'População Alvo': 'populacao_alvo',
            'Tamanho da Amostra': 'tamanho_amostra',
            'Produtos Esperados': 'produtos_esperados',
            'Formação Acadêmica': 'formacao_academica',
            'Tipo de Dados': 'tipo_dados',
            'Curso/Área de Conhecimento': 'curso_area_conhecimento',
            'Abordagem da Pesquisa': 'abordagem_pesquisa',
            'Delineamento do Estudo': 'delineamento_estudo',
            'Tipo de Estudo': 'tipo_estudo',
            'CID': 'cid',
            
            # --- Bloco Sim/Não (Booleano) ---
            'Estudo Multicêntrico?': 'eh_estudo_multicentrico',
            'Coordenador?': 'eh_coordenador',
            'Instituição Coordenadora': 'instituicao_coordenadora_estudo',
            'Projeto com Coparticipação?': 'eh_projeto_coparticipacao',
            'Outras Instituições Participantes': 'outras_instituicoes_participantes',
            
            # --- Bloco Financeiro (Fomento) ---
            'Tipo de Fomento': 'tipo_fomento',
            'Instituição de Fomento': 'instituicao_fomento',
            'Valor - Bolsas': 'valor_bolsas',
            'Valor - Custeio': 'valor_custeio',
            'Valor - Capital': 'valor_capital',
            'Valor Total do Projeto': 'valor_total_projeto',
            
            # --- Bloco de Datas e Coleta ---
            'Data Início Previsão': 'data_inicio_previsao',
            'Data Fim Previsão': 'data_fim_previsao',
            'Previsão Coleta Dados HUF?': 'previsao_coleta_dados_huf',
            'Data Início Coleta Previsão': 'data_inicio_coleta_previsao',
            'Data Fim Coleta Previsão': 'data_fim_coleta_previsao',
            'Local Coleta Dados': 'local_coleta_dados',
            'Possui Aprovação Comitê Ética?': 'possui_aprovacao_comite_etica'
        }
        
    elif nome_tabela == "historico_status_projetos":
        # Mapeia de 'historico_limpo.csv' para a tabela 'historico_status_projetos'
        mapa_colunas = {
            # --- VERIFIQUE E CORRIJA OS NOMES DAS COLUNAS DE ORIGEM (À ESQUERDA) ---
            'Número do Projeto': 'numero_projeto', # Provavelmente a chave para ligar ao 'codigo' de projetos
            'Hospital Universitário (HU)': 'hospital_universitario_hu',
            'Título do Projeto': 'titulo_projeto',
            'Status Onde Estava': 'status_onde_estava',
            'Status (o que aconteceu)': 'status_o_que_aconteceu',
            'Status do Projeto': 'status_projeto_atual', # Coluna que chutei ser 'Status do Projeto'
            'Quem Fez': 'quem_fez',
            'Quando Fez (data e hora)': 'quando_fez_data_hora',
            'Duração': 'duracao'
        }
        
    elif nome_tabela == "pesquisa":
        # Mapeia de 'acompanhamento_limpo.csv' para a tabela 'pesquisa'
        mapa_colunas = {
            # --- VERIFIQUE E CORRIJA OS NOMES DAS COLUNAS DE ORIGEM (À ESQUERDA) ---
            'HU': 'hu',
            'Título do Projeto': 'titulo_projeto',
            'Classificação Institucional da Pesquisa': 'classificacao_institucional_pesquisa',
            'Envolve Seres Humanos': 'envolve_seres_humanos',
            'Delineamento do Estudo': 'delineamento_estudo',
            'Tipo de Estudo': 'tipo_estudo',
            'É Estudo Multicêntrico': 'eh_estudo_multicentrico',
            'É Projeto com Coparticipação': 'eh_projeto_com_coparticipacao',
            'Estado do Projeto': 'estado_projeto',
            'Tempo Decorrido (Status 1)': 'tempo_decorrido_entre_status_1',
            'Tempo Decorrido (Status 2)': 'tempo_decorrido_entre_status_2'
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
    # Isso é VITAL para o COPY funcionar, pois remove colunas extras
    # (ex: "Unnamed: 30" ou colunas que você não quer carregar)
    colunas_sql = list(mapa_colunas.values())
    
    # Verifica se todas as colunas SQL esperadas existem no DataFrame renomeado
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