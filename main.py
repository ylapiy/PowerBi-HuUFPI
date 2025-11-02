import os
from src.extract import ler_csv_bruto
from src.transform import filtrar_projetos_inativos, remover_duplicatas_por_completude
from src.load import salvar_csv

# --- Configuração Central do Pipeline ---

# Mapeia os arquivos de entrada para os de saída
# Isso é mais profissional do que usar glob("*"), pois é explícito.
ARQUIVOS_PARA_PROCESSAR = [
    {
        "bruto": "data/raw/relatorio_projetos (870)(in).csv",
        "limpo": "data/processed/projetos_limpo.csv",
        "removidos": "data/processed/duplicatas_removidas/projetos_duplicatas.csv"
    },
    {
        "bruto": "data/raw/relatorio_projetos_historico(870)(in).csv",
        "limpo": "data/processed/historico_limpo.csv",
        "removidos": "data/processed/duplicatas_removidas/historico_duplicatas.csv"
    },
    {
        "bruto": "data/raw/relatorio_acompanhamento_projetos (870)(in) (1).csv",
        "limpo": "data/processed/acompanhamento_limpo.csv",
        "removidos": "data/processed/duplicatas_removidas/acompanhamento_duplicatas.csv"
    }
]

# Garante que as pastas de saída existam
os.makedirs("data/processed", exist_ok=True)
os.makedirs("data/processed/duplicatas_removidas", exist_ok=True)

# ----------------------------------------

def run_pipeline():
    """
    Executa o pipeline de ETL completo.
    """
    print("Iniciando pipeline de ETL...")
    
    for config_arquivo in ARQUIVOS_PARA_PROCESSAR:
        print(f"\nProcessando: {config_arquivo['bruto']}")
        
        # 1. EXTRACT
        df = ler_csv_bruto(config_arquivo["bruto"])
        if df.empty:
            continue

        # 2. TRANSFORM (Passo 1: Filtro de Inativos)
        df_sem_inativos = filtrar_projetos_inativos(df)
        
        # 2. TRANSFORM (Passo 2: Remoção de Duplicatas)
        df_limpo, df_removido = remover_duplicatas_por_completude(df_sem_inativos)

        # 3. LOAD (Salvar em 'processed')
        salvar_csv(df_limpo, config_arquivo["limpo"])
        print(f"   -> Arquivo limpo salvo em: {config_arquivo['limpo']}")
        
        if not df_removido.empty:
            salvar_csv(df_removido, config_arquivo["removidos"])
            print(f"   -> Duplicatas salvas em: {config_arquivo['removidos']}")
            
    print("\nPipeline concluído com sucesso!")


if __name__ == "__main__":
    run_pipeline()
