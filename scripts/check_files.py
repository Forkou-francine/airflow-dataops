import os

def check_files():
    # Chemin défini dans l'énoncé pour les fichiers CSV dans le conteneur
    data_path = "/airflow/data/raw"
    
    # Liste des fichiers attendus (à adapter selon tes noms de fichiers réels)
    expected_files = ["customers.csv", "sales.csv", "products.csv"]
    
    missing_files = []

    # Vérification de l'existence de chaque fichier
    for file_name in expected_files:
        full_path = os.path.join(data_path, file_name)
        if not os.path.exists(full_path):
            missing_files.append(file_name)

    # Affichage du résultat selon les consignes
    if not missing_files:
        print("Fichiers présents")
    else:
        print(f"Fichiers manquants : {', '.join(missing_files)}")
        # Optionnel : quitter avec un code erreur pour qu'Airflow marque la tâche en 'failed'
        # import sys
        # sys.exit(1)

if __name__ == "__main__":
    check_files()