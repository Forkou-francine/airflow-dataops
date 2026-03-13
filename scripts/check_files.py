import os

def check_files():
    data_path = "/airflow/data/raw"
    
    expected_files = ["customers.csv", "sales.csv", "products.csv"]
    
    missing_files = []

    for file_name in expected_files:
        full_path = os.path.join(data_path, file_name)
        if not os.path.exists(full_path):
            missing_files.append(file_name)

    if not missing_files:
        print("Fichiers présents")
    else:
        print(f"Fichiers manquants : {', '.join(missing_files)}")


if __name__ == "__main__":
    check_files()