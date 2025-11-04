import os
from src.extract import ler_csv_bruto
from src.transform import (
    remover_duplicatas_por_completude,
    renomear_colunas_para_sql,
    corrigir_tipos_de_dados 
)
from src.load import salvar_csv, load_csv_to_postgres

# --- Configuração Central do Pipeline ---

ARQUIVOS_PARA_PROCESSAR = [
    {
        "bruto": "data/raw/brutos/relatorio_projetos (870)(in).csv",
        "limpo": "data/processed/projetos_limpo.csv",
        "removidos": "data/processed/duplicatas_removidas/projetos_duplicatas.csv",
        "tabela_destino": "projetos" # <-- MAPEAMENTO SQL
    },
    {
        "bruto": "data/raw/brutos/relatorio_projetos_historico(870)(in).csv",
        "limpo": "data/processed/historico_limpo.csv",
        "removidos": "data/processed/duplicatas_removidas/historico_duplicatas.csv",
        "tabela_destino": "historico_status_projetos" # <-- MAPEAMENTO SQL
    },
    {
        "bruto": "data/raw/brutos/relatorio_acompanhamento_projetos (870)(in) (1).csv",
        "limpo": "data/processed/acompanhamento_limpo.csv",
        "removidos": "data/processed/duplicatas_removidas/acompanhamento_duplicatas.csv",
        "tabela_destino": "pesquisa" # <-- MAPEAMENTO SQL
    }
]

# Garante que as pastas de saída existam
os.makedirs("data/processed", exist_ok=True)
os.makedirs("data/processed/duplicatas_removidas", exist_ok=True)

# ----------------------------------------

def run_pipeline():
    """
    Executa o pipeline de ETL completo, incluindo o LOAD para o PostgreSQL.
    """
    print("Iniciando pipeline de ETL...")
    
    for config_arquivo in ARQUIVOS_PARA_PROCESSAR:
        print(f"\nProcessando: {config_arquivo['bruto']}")
        
        # 1. EXTRACT
        df = ler_csv_bruto(config_arquivo["bruto"])
        if df.empty:
            print("   -> Arquivo vazio ou não encontrado. Pulando.")
            continue

        # 2. TRANSFORM (Passo 1: Filtro)
        # df_sem_inativos = filtrar_projetos_inativos(df) <--- deixar para o Bi --->
        
        # 2. TRANSFORM (Passo 2: Duplicatas)
        df_limpo, df_removido = remover_duplicatas_por_completude(df, config_arquivo["tabela_destino"])

        # 2. TRANSFORM (Passo 3: Renomear Colunas)
        df_sql_pronto = renomear_colunas_para_sql(df_limpo, config_arquivo["tabela_destino"])
        
        # Sempre salva as duplicatas removidas
        if not df_removido.empty:
            salvar_csv(df_removido, config_arquivo["removidos"])
            print(f"   -> Duplicatas salvas em: {config_arquivo['removidos']}")

        # Se a renomeação falhou, pula para o próximo arquivo
        if df_sql_pronto.empty:
            print(f"   Erro [Main]: Transformação (Renomear) para '{config_arquivo['tabela_destino']}' falhou (ver logs acima).")
            print(f"   -> PULANDO salvamento e carga para a tabela '{config_arquivo['tabela_destino']}'.")
            continue 
        
        # 2. TRANSFORM (Passo 4: Corrigir Tipos de Dados) <-- 2. ADICIONA A NOVA ETAPA
        df_corrigido = corrigir_tipos_de_dados(df_sql_pronto, config_arquivo["tabela_destino"])

        # Se a correção de tipos falhou, pula para o próximo arquivo
        if df_corrigido.empty:
            print(f"   Erro [Main]: Transformação (Correção de Tipos) para '{config_arquivo['tabela_destino']}' falhou (ver logs acima).")
            print(f"   -> PULANDO salvamento e carga para a tabela '{config_arquivo['tabela_destino']}'.")
            continue
        # --------------------------------------------------------
        
        # 3. LOAD (Passo 1: Salvar CSV intermediário)
        caminho_csv_limpo = config_arquivo["limpo"]
        salvar_csv(df_corrigido, caminho_csv_limpo) # <-- 3. Usa df_corrigido
        print(f"   -> Arquivo limpo salvo em: {caminho_csv_limpo}")
        
        # 4. LOAD (Passo 2: Carregar no PostgreSQL)
        load_csv_to_postgres(caminho_csv_limpo, config_arquivo["tabela_destino"], list(df_corrigido.columns))
            
    print("\nPipeline concluído com sucesso!")


if __name__ == "__main__":
    run_pipeline()
