import subprocess

print("\n=== 🚀 Etapa 1: Filtrar projetos inativos ===")
subprocess.run(["python", "script_limpeza_projetos_inativos.py"], check=True)

print("\n=== 🚀 Etapa 2: Remover duplicatas ===")
subprocess.run(["python", "script_limpeza_duplicatas.py"], check=True)

print("\n=== 🚀 Etapa 3: Atualizando o Banco ===")
subprocess.run(["python", "Enviador.py"], check=True)

print("\n=== ✔️ Pipeline concluído com sucesso! ===")
