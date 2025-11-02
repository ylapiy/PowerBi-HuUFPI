from src.extract import ler_csvs_brutos
from src.transform import remover_duplicatas, filtrar_inativos
from src.load import carregar_para_banco

def run_pipeline():
    print("Iniciando pipeline...")

    # 1. Extract
    df_projetos = ler_csvs_brutos('data/raw/relatorio_projetos.csv')
    df_historico = ler_csvs_brutos('data/raw/relatorio_historico.csv')

    # 2. Transform
    df_projetos_limpo = remover_duplicatas(df_projetos)
    df_projetos_final = filtrar_inativos(df_projetos_limpo)

    # 3. Load
    carregar_para_banco(df_projetos_final, "tabela_projetos")
    print("Pipeline concluído.")

if __name__ == "__main__":
    run_pipeline()
