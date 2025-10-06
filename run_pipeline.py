import os
import sys
import subprocess

# Rutas base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Scripts individuales
SCRIPTS = [
    os.path.join(BASE_DIR, "1_extract", "extract_data.py"),
    os.path.join(BASE_DIR, "2_transform", "transform_data.py"),
    os.path.join(BASE_DIR, "4_validate", "validate_data.py"),
    os.path.join(BASE_DIR, "upload_to_gcs.py")
]

def run_script(script_path):
    print(f"\n Ejecutando: {script_path}")
    
    # Usamos el mismo interprete del entorno virtual actual
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f" Éxito en: {os.path.basename(script_path)}")
        print(result.stdout)
    else:
        print(f" Error en: {os.path.basename(script_path)}")
        print(result.stderr)
        exit(1)

if __name__ == "__main__":
    print(" Iniciando pipeline completo DeltaQuebec...")
    for script in SCRIPTS:
        run_script(script)
    print("\n Pipeline completado correctamente ")