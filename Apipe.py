import subprocess

import os


arquivos = ["relatorio_projetos.csv", "relatorio_projetos_historico.csv"]

pasta = "csvs/brutos"

for arquivo in arquivos:
    caminho_completo = os.path.join(pasta, arquivo)
    try:
        if os.path.isfile(caminho_completo):
            print(f"O arquivo '{arquivo}' existe na pasta '{pasta}'.")
        else:
            raise FileNotFoundError(
                f"O arquivo '{arquivo}' NÃO foi encontrado na pasta '{pasta}'."
            )
    except FileNotFoundError as e:
        print(e)


print("\n=== 🚀 Etapa 1: Filtrar projetos inativos ===")
subprocess.run(["python", "script_limpeza_projetos_inativos.py"], check=True)

print("\n=== 🚀 Etapa 2: Remover duplicatas ===")
subprocess.run(["python", "script_limpeza_duplicatas.py"], check=True)

print("\n=== 🚀 Etapa 3: Atualizando o Banco ===")
subprocess.run(["python", "Enviador.py"], check=True)

print("\n=== ✔️ Pipeline concluído com sucesso! ===")
