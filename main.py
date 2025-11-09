import os
import sys # Recomendado adicionar para logs de erro
from src.extract import ler_csv_bruto
from src.transform import (
    remover_duplicatas_por_completude,
    processar_dataframe_para_sql  # <-- ALTERADO: Importa a nova função orquestradora
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
        "tabela_destino": "projeto_historico" # <-- MAPEAMENTO SQL
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

        # 2. TRANSFORM (Passo 1: Duplicatas)
        df_limpo, df_removido = remover_duplicatas_por_completude(df, config_arquivo["tabela_destino"])

        # Sempre salva as duplicatas removidas
        if not df_removido.empty:
            salvar_csv(df_removido, config_arquivo["removidos"])
            print(f"   -> Duplicatas salvas em: {config_arquivo['removidos']}")
        
        # =====================================================================
        # 2. TRANSFORM (Passo 2: Processamento Completo)
        #    Esta ÚNICA função agora faz:
        #    - Renomeação de colunas
        #    - Correção de tipos de dados (datas, números, etc.)
        #    - Aplicação de regras customizadas (como 'numero_projeto')
        
        print(f"   Iniciando transformação completa para: {config_arquivo['tabela_destino']}")
        df_processado = processar_dataframe_para_sql(df_limpo, config_arquivo['tabela_destino'])
        
        # Se o processamento falhou (verificando se o DF está vazio)
        if df_processado.empty:
            print(f"   Erro [Main]: A transformação 'processar_dataframe_para_sql' falhou para '{config_arquivo['tabela_destino']}' (ver logs acima).", file=sys.stderr)
            print(f"   -> PULANDO salvamento e carga para a tabela '{config_arquivo['tabela_destino']}'.")
            continue 
        # =====================================================================

        # 3. LOAD (Passo 1: Salvar CSV intermediário)
        caminho_csv_limpo = config_arquivo["limpo"]
        salvar_csv(df_processado, caminho_csv_limpo) # <-- Usa df_processado
        print(f"   -> Arquivo limpo salvo em: {caminho_csv_limpo}")
        
        # 4. LOAD (Passo 2: Carregar no PostgreSQL)
        load_csv_to_postgres(
            caminho_csv_limpo, 
            config_arquivo["tabela_destino"], 
            list(df_processado.columns) # <-- Usa df_processado
        )
            
    print("\nPipeline concluído com sucesso!")


if __name__ == "__main__":
    run_pipeline()
