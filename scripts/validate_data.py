"""
Autheur: Ange-F.
Ce script vérifie que les colonnes de chaque fichier CSV correspondent au schéma attendu.
"""

import csv
import os
import sys

RAW_DIR = "/opt/airflow/data/raw"

EXPECTED_COLUMNS = {
    "customers.csv": ["customer_id", "first_name", "last_name", "city"],
    "orders.csv": ["order_id", "order_date", "customer_id", "product_id", "quantity"],
    "products.csv": ["product_id", "product_name", "category", "unit_price"],
}


def validate_file(filename, expected_cols):
    """Valide les colonnes du fichier CSV qui est lu. Retourne True si c'est OK, False sinon."""
    filepath = os.path.join(RAW_DIR, filename)

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    # On nettoie les espaces éventuels
    header = [col.strip() for col in header]

    if header != expected_cols:
        print(f"ERREUR dans {filename} :")
        print(f" Colonnes attendues : {expected_cols}")
        print(f" Colonnes trouvées  : {header}")
        return False

    print(f"{filename} : colonnes OK")
    return True


def main():
    all_valid = True

    for filename, expected_cols in EXPECTED_COLUMNS.items():
        if not validate_file(filename, expected_cols):
            all_valid = False

    if not all_valid:
        print("\nValidation échouée.")
        sys.exit(1)
    else:
        print("\nTous les fichiers sont valides.")


if __name__ == "__main__":
    main()
