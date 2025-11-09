TABEL_CONFIG = {
    "projetos": {
        "mapa_colunas": {
            # Mapeia do CSV (key) para o SQL (value)
            '\ufeffCódigo': 'codigo',
            'Título': 'titulo',
            'Estado do projeto': 'estado do projeto',
            'Data de solicitação': 'data de solicitacao',
            'Última atualização': 'ultima atualizacao',
            'HUF': 'huf',
            'Instituição proponente': 'instituicao proponente',
            'Classificação institucional': 'classificacao institucional',
            'Pesquisa Acadêmica': 'pesquisa academica',
            'Envolve seres humanos?': 'envolve seres humanos?',
            'Palavras-chave': 'palavras chave',
            'População alvo': 'populacao alvo',
            'Tamanho da amostra': 'tamanho da amostra',
            'Produtos esperados': 'produtos esperados',
            'Formação Academica': 'formacao academica',
            'Tipo de dados': 'tipo de dados',
            'Curso/Área de Conhecimento': 'curso area de conhecimento',
            'Abordagem da pesquisa': 'abordagem da pesquisa',
            'Delineamento do estudo': 'delineamento do estudo',
            'Tipo de estudo': 'tipo de estudo',
            'CID': 'cid',
            'É estudo multicêntrico?': 'e estudo multicentrico?',
            'É coordenador?': 'e coordenador?',
            'Instituição coordenadora de estudo': 'instituicao coordenadora de estudo',
            'É projeto com co-participação?': 'e projeto com co participacao?',
            'Outras Instituições participantes': 'outras instituicoes participantes',
            'Tipo de fomento': 'tipo de fomento',
            'Instituição de fomento': 'instituicao de fomento',
            'Bolsas (R$)': 'bolsas r$',
            'Recurso de custeio (R$)': 'recurso de custeio r$',
            'Recursos de capital (R$)': 'recursos de capital r$',
            'Recursos total do projeto (R$)': 'recursos total do projeto r$',
            'Inicío do projeto (previsão)': 'inicio do projeto previsao',
            'Fim do projeto (previsão)': 'fim do projeto previsao',
            'Há previsão de coleta de dados no HUF?': 'ha previsao de coleta de dados no huf?',
            'Inicío da coleta de dados (previsão)': 'inicio da coleta de dados previsao',
            'Fim da coleta de dados (previsão)': 'fim da coleta de dados previsao',
            'Local da coleta de dados': 'local da coleta de dados',
            'Possui aprovação do Comitê de Ética?': 'possui aprovacao do comite de etica?'
            
            # ATENÇÃO: As colunas de "Equipe de pesquisa" não estavam no seu CSV de exemplo
            # Se elas existirem, você deve mapeá-las aqui. Ex:
            # 'Equipe de pesquisa - Nome': 'equipe de pesquisa  nome',
            # 'Equipe de pesquisa - CPF': 'equipe de pesquisa  cpf',
            # 'Equipe de pesquisa - Email': 'equipe de pesquisa  email',
            # 'Equipe de pesquisa - Papel': 'equipe de pesquisa  papel',
            # 'Equipe de pesquisa - Lattes': 'equipe de pesquisa  lattes',
        },
        # As listas de tipos agora usam os nomes de coluna do SQL
        "colunas_data": [
            'data de solicitacao', 'inicio do projeto previsao', 'fim do projeto previsao',
            'inicio da coleta de dados previsao', 'fim da coleta de dados previsao',
            'ultima atualizacao' # Esta também é DATE no novo SQL
        ],
        "colunas_timestamp": [
            # 'ultima atualizacao' movido para colunas_data
        ],
        "colunas_booleano": [
            'envolve seres humanos?', 'e estudo multicentrico?', 'e coordenador?',
            'e projeto com co participacao?', 'ha previsao de coleta de dados no huf?',
            'possui aprovacao do comite de etica?'
        ],
        "colunas_numericas": [
            'bolsas r$', 'recurso de custeio r$', 'recursos de capital r$', 
            'recursos total do projeto r$', 'tamanho da amostra'
        ]
    },
    "projeto_historico": {
        "mapa_colunas": {
            # Mapeia do CSV (key) para o SQL (value)
            '\ufeffHospital Universitário (HU)': 'hospital universitario hu',
            'Número do Projeto': 'numero do projeto',
            'Título do Projeto': 'titulo do projeto',
            'Status Onde Estava': 'status onde estava',
            'Status O que Aconteceu': 'status o que aconteceu',
            'Status do Projeto': 'status do projeto',
            'Quem Fez': 'quem fez',
            'Quando Fez (Data/Hora)': 'quando fez data hora',
            'Duração': 'duracao'
            # A coluna 'codigo' é populada pelo próprio SQL, não pelo CSV
        },
        "colunas_timestamp": [
             # O novo SQL armazena 'quando fez data hora' como VARCHAR(255)
             # Portanto, não precisamos convertê-lo para timestamp no Pandas
        ],
        "colunas_data": [],
        "colunas_booleano": [],
        "colunas_numericas": []
    },
    "pesquisa": {
            "mapa_colunas": {
                # Mapeia do CSV (key) para o SQL (value)
                '\ufeffHU': 'hu',
                'Título do Projeto': 'titulo do projeto',
                'Classificação Institucional da Pesquisa': 'classificacao institucional da pesquisa',
                'Projeto envolve seres humanos': 'projeto envolve seres humanos',
                'Delineamento do Estudo': 'delineamento do estudo',
                'Tipo de Estudo': 'tipo de estudo',
                'É estudo multicêntrico': 'e estudo multicentrico',
                'É projeto com co-participação': 'e projeto com co participacao',
                'Estado do projeto': 'estado do projeto',
                'Tempo decorrido entre status': 'tempo decorrido entre status 1',
                'Tempo decorrido entre status.1': 'tempo decorrido entre status 2'
            },
            "colunas_booleano": [
                'projeto envolve seres humanos', 
                'e estudo multicentrico',
                'e projeto com co participacao'
            ],
            # Adicionado para garantir que a função não falhe
            "colunas_data": [],
            "colunas_timestamp": [],
            "colunas_numericas": []
        }
}