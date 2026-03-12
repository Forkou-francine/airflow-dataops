import csv
import os


RAW_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"

CUSTOMERS_FILE = "customers.csv"
ORDERS_FILE = "orders.csv"
PRODUCTS_FILE = "products.csv"

ORDERS_DETAIL_FILE = "orders_detail.csv"
SALES_BY_CUSTOMER_FILE = "sales_by_customer.csv"
SALES_BY_PRODUCT_FILE = "sales_by_product.csv"


def read_customers(path):
    customers = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            customer_id = row.get("customer_id")
            if not customer_id:
                continue
            customers[customer_id] = {
                "first_name": row.get("first_name", ""),
                "last_name": row.get("last_name", ""),
                "city": row.get("city", ""),
            }
    return customers


def read_products(path):
    products = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            product_id = row.get("product_id")
            if not product_id:
                continue
            unit_price_value = row.get("unit_price", "")
            try:
                unit_price = float(unit_price_value) if unit_price_value != "" else 0.0
            except ValueError:
                unit_price = 0.0
            products[product_id] = {
                "product_name": row.get("product_name", ""),
                "category": row.get("category", ""),
                "unit_price": unit_price,
            }
    return products


def read_orders(path):
    orders = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            quantity_value = row.get("quantity", "")
            try:
                quantity = int(quantity_value) if quantity_value != "" else 0
            except ValueError:
                quantity = 0
            orders.append(
                {
                    "order_id": row.get("order_id", ""),
                    "order_date": row.get("order_date", ""),
                    "customer_id": row.get("customer_id", ""),
                    "product_id": row.get("product_id", ""),
                    "quantity": quantity,
                }
            )
    return orders


def build_order_details(customers, products, orders):
    details = []
    for order in orders:
        customer_id = order.get("customer_id", "")
        product_id = order.get("product_id", "")
        customer = customers.get(customer_id)
        product = products.get(product_id)
        if not customer or not product:
            continue
        quantity = order.get("quantity", 0)
        unit_price = product.get("unit_price", 0.0)
        line_total = quantity * unit_price
        details.append(
            {
                "order_id": order.get("order_id", ""),
                "order_date": order.get("order_date", ""),
                "customer_id": customer_id,
                "first_name": customer.get("first_name", ""),
                "last_name": customer.get("last_name", ""),
                "city": customer.get("city", ""),
                "product_id": product_id,
                "product_name": product.get("product_name", ""),
                "category": product.get("category", ""),
                "unit_price": unit_price,
                "quantity": quantity,
                "line_total": line_total,
            }
        )
    return details


def write_orders_detail(path, details):
    fieldnames = [
        "order_id",
        "order_date",
        "customer_id",
        "first_name",
        "last_name",
        "city",
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "quantity",
        "line_total",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in details:
            writer.writerow(row)


def aggregate_by_customer(details):
    aggregates = {}
    for row in details:
        customer_id = row.get("customer_id", "")
        if customer_id not in aggregates:
            aggregates[customer_id] = {
                "customer_id": customer_id,
                "first_name": row.get("first_name", ""),
                "last_name": row.get("last_name", ""),
                "city": row.get("city", ""),
                "total_quantity": 0,
                "total_amount": 0.0,
            }
        aggregates[customer_id]["total_quantity"] += row.get("quantity", 0)
        aggregates[customer_id]["total_amount"] += row.get("line_total", 0.0)
    return list(aggregates.values())


def write_sales_by_customer(path, rows):
    fieldnames = [
        "customer_id",
        "first_name",
        "last_name",
        "city",
        "total_quantity",
        "total_amount",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def aggregate_by_product(details):
    aggregates = {}
    for row in details:
        product_id = row.get("product_id", "")
        if product_id not in aggregates:
            aggregates[product_id] = {
                "product_id": product_id,
                "product_name": row.get("product_name", ""),
                "category": row.get("category", ""),
                "total_quantity": 0,
                "total_amount": 0.0,
            }
        aggregates[product_id]["total_quantity"] += row.get("quantity", 0)
        aggregates[product_id]["total_amount"] += row.get("line_total", 0.0)
    return list(aggregates.values())


def write_sales_by_product(path, rows):
    fieldnames = [
        "product_id",
        "product_name",
        "category",
        "total_quantity",
        "total_amount",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    customers_path = os.path.join(RAW_DIR, CUSTOMERS_FILE)
    products_path = os.path.join(RAW_DIR, PRODUCTS_FILE)
    orders_path = os.path.join(RAW_DIR, ORDERS_FILE)
    orders_detail_path = os.path.join(PROCESSED_DIR, ORDERS_DETAIL_FILE)
    sales_by_customer_path = os.path.join(PROCESSED_DIR, SALES_BY_CUSTOMER_FILE)
    sales_by_product_path = os.path.join(PROCESSED_DIR, SALES_BY_PRODUCT_FILE)
    customers = read_customers(customers_path)
    products = read_products(products_path)
    orders = read_orders(orders_path)
    details = build_order_details(customers, products, orders)
    write_orders_detail(orders_detail_path, details)
    customer_aggregates = aggregate_by_customer(details)
    write_sales_by_customer(sales_by_customer_path, customer_aggregates)
    product_aggregates = aggregate_by_product(details)
    write_sales_by_product(sales_by_product_path, product_aggregates)


if __name__ == "__main__":
    main()

