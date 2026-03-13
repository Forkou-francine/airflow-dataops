"""
DAG Airflow pour le pipeline ETL des ventes.

Étapes :
1. check_files : Vérifie la présence des fichiers CSV
2. validate_data  : Vérifie les colonnes de chaque CSV
3. transform_sales : Jointure + calcul du montant, export dans processed/
4. et un dernier pour confirmer la fin du pipeline
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

SCRIPTS_DIR = "/opt/airflow/scripts"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="sales_etl_dag",
    default_args=default_args,
    description="Pipeline ETL de ventes - Atelier DataOps",
    schedule_interval=None,  # Déclenchement manuel
    start_date=datetime(2026, 3, 10),
    catchup=False,
    tags=["dataops", "etl", "tp"],
) as dag:

    check_files = BashOperator(
        task_id="check_files",
        bash_command=f"python {SCRIPTS_DIR}/check_files.py",
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command=f"python {SCRIPTS_DIR}/validate_data.py",
    )

    transform_sales = BashOperator(
        task_id="transform_sales",
        bash_command=f"python {SCRIPTS_DIR}/transform_sales.py",
    )

    pipeline_done = BashOperator(
        task_id="pipeline_done",
        bash_command='echo "Pipeline ETL terminé avec succès à $(date)"',
    )

    # Dépendances : exécution séquentielle
    check_files >> validate_data >> transform_sales >> pipeline_done

