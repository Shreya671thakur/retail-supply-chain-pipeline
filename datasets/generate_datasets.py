import random
import csv
from datetime import datetime, timedelta

random.seed(42)

OUT_DIR = "/home/claude/retail-supply-chain-pipeline/datasets"

# ---------------------------------------------------------------------------
# Reference data pools
# ---------------------------------------------------------------------------
COUNTRIES_CITIES = [
    ("USA", "New York"), ("USA", "Chicago"), ("USA", "Los Angeles"),
    ("USA", "Dallas"), ("Canada", "Toronto"), ("Canada", "Vancouver"),
    ("UK", "London"), ("UK", "Manchester"), ("Germany", "Berlin"),
    ("Germany", "Munich"), ("France", "Paris"), ("France", "Lyon"),
    ("India", "Mumbai"), ("India", "Pune"), ("India", "Bengaluru"),
    ("India", "Delhi"), ("China", "Shanghai"), ("China", "Shenzhen"),
    ("Japan", "Tokyo"), ("Japan", "Osaka"), ("Australia", "Sydney"),
    ("Australia", "Melbourne"), ("Brazil", "Sao Paulo"), ("Mexico", "Monterrey"),
    ("Netherlands", "Rotterdam"), ("Vietnam", "Hanoi"), ("South Korea", "Seoul"),
    ("Italy", "Milan"), ("Spain", "Barcelona"), ("UAE", "Dubai"),
]

SUPPLIER_NAME_PARTS = [
    "Global", "Prime", "United", "National", "Pacific", "Atlantic", "Summit",
    "Apex", "Horizon", "Vertex", "Nova", "Sterling", "Crown", "Pioneer",
    "Falcon", "Titan", "Continental", "Evergreen", "Northstar", "Bluepeak",
]
SUPPLIER_NAME_SUFFIX = [
    "Supplies Co", "Trading Ltd", "Industries Inc", "Logistics Group",
    "Manufacturing Corp", "Distributors", "Exports Ltd", "Wholesale Partners",
    "Goods Network", "Sourcing Inc",
]

CATEGORIES = [
    "Electronics", "Apparel", "Groceries", "Furniture", "Home Appliances",
    "Toys", "Stationery", "Footwear", "Automotive Parts", "Health & Beauty",
    "Sports Equipment", "Hardware Tools",
]

PRODUCT_PREFIX = {
    "Electronics": ["Bluetooth Speaker", "LED Monitor", "Wireless Mouse", "Smartphone Charger", "USB Hub", "Power Bank"],
    "Apparel": ["Cotton T-Shirt", "Denim Jacket", "Wool Sweater", "Running Shorts", "Formal Shirt", "Hooded Sweatshirt"],
    "Groceries": ["Basmati Rice 5kg", "Olive Oil 1L", "Organic Honey 500g", "Green Tea Pack", "Almonds 1kg", "Pasta 1kg"],
    "Furniture": ["Office Chair", "Wooden Bookshelf", "Study Desk", "Recliner Sofa", "Dining Table", "Bed Frame"],
    "Home Appliances": ["Microwave Oven", "Electric Kettle", "Vacuum Cleaner", "Air Fryer", "Ceiling Fan", "Water Purifier"],
    "Toys": ["Building Blocks Set", "Remote Control Car", "Puzzle 1000pc", "Action Figure", "Plush Bear", "Board Game"],
    "Stationery": ["Notebook Pack", "Gel Pens Set", "Sticky Notes", "Highlighter Set", "Desk Organizer", "Whiteboard Markers"],
    "Footwear": ["Running Shoes", "Leather Boots", "Sandals", "Sneakers", "Formal Loafers", "Sports Slides"],
    "Automotive Parts": ["Brake Pad Set", "Engine Oil Filter", "Car Battery", "Wiper Blades", "Spark Plug Set", "Air Filter"],
    "Health & Beauty": ["Face Moisturizer", "Shampoo 500ml", "Electric Toothbrush", "Sunscreen SPF50", "Hair Dryer", "Vitamin C Serum"],
    "Sports Equipment": ["Yoga Mat", "Dumbbell Set", "Cricket Bat", "Football", "Resistance Bands", "Cycling Helmet"],
    "Hardware Tools": ["Cordless Drill", "Hammer Set", "Tool Box", "Measuring Tape", "Screwdriver Kit", "Adjustable Wrench"],
}

WAREHOUSES = [
    "WH-NORTH-01", "WH-SOUTH-02", "WH-EAST-03", "WH-WEST-04",
    "WH-CENTRAL-05", "WH-PORT-06", "WH-METRO-07", "WH-HUB-08",
]

SHIPMENT_STATUSES = ["DELIVERED", "IN_TRANSIT", "DELAYED", "CANCELLED", "PENDING"]
STATUS_WEIGHTS = [0.55, 0.18, 0.15, 0.05, 0.07]


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    rand_days = random.randint(0, max(delta.days, 0))
    return start + timedelta(days=rand_days)


def fmt(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def inject_nulls_and_dupes(rows, null_field_choices, null_rate=0.02, dupe_rate=0.01):
    """Mutate a list of dict rows in place to add some empty values, then
    append a handful of duplicate rows. This mimics real-world messy data."""
    n = len(rows)
    n_nulls = int(n * null_rate)
    for _ in range(n_nulls):
        row = random.choice(rows)
        field = random.choice(null_field_choices)
        row[field] = ""

    n_dupes = max(1, int(n * dupe_rate))
    for _ in range(n_dupes):
        rows.append(dict(random.choice(rows[:n])))
    return rows


# ---------------------------------------------------------------------------
# 1. SUPPLIERS (100 rows)
# ---------------------------------------------------------------------------
def gen_suppliers(n=100):
    rows = []
    used_names = set()
    for i in range(1, n + 1):
        supplier_id = f"SUP{i:04d}"
        while True:
            name = f"{random.choice(SUPPLIER_NAME_PARTS)} {random.choice(SUPPLIER_NAME_SUFFIX)}"
            if name not in used_names:
                used_names.add(name)
                break
        country, city = random.choice(COUNTRIES_CITIES)
        category = random.choice(CATEGORIES)
        rating = round(random.uniform(2.5, 5.0), 1)
        onboarded = random_date(datetime(2018, 1, 1), datetime(2024, 12, 31))
        email_domain = name.lower().replace(" ", "").replace(",", "") + ".com"
        contact_email = f"contact@{email_domain}"
        phone = f"+{random.randint(1, 99)}-{random.randint(1000000000, 9999999999)}"
        rows.append({
            "supplier_id": supplier_id,
            "supplier_name": name,
            "category": category,
            "country": country,
            "city": city,
            "contact_email": contact_email,
            "phone": phone,
            "rating": rating,
            "onboarded_date": fmt(onboarded),
        })
    rows = inject_nulls_and_dupes(
        rows, null_field_choices=["contact_email", "phone", "rating"],
        null_rate=0.03, dupe_rate=0.01,
    )
    return rows


# ---------------------------------------------------------------------------
# 2. INVENTORY (1000 rows)
# ---------------------------------------------------------------------------
def gen_inventory(suppliers, n=1000):
    rows = []
    supplier_ids = [s["supplier_id"] for s in suppliers]
    for i in range(1, n + 1):
        inventory_id = f"INV{i:05d}"
        category = random.choice(CATEGORIES)
        product_name = f"{random.choice(PRODUCT_PREFIX[category])} #{random.randint(100,999)}"
        product_id = f"PRD{i:05d}"
        supplier_id = random.choice(supplier_ids)
        warehouse = random.choice(WAREHOUSES)
        stock_quantity = random.randint(0, 5000)
        reorder_level = random.randint(50, 500)
        unit_price = round(random.uniform(2.5, 1200.0), 2)
        last_restock = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
        rows.append({
            "inventory_id": inventory_id,
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "supplier_id": supplier_id,
            "warehouse_location": warehouse,
            "stock_quantity": stock_quantity,
            "reorder_level": reorder_level,
            "unit_price": unit_price,
            "last_restock_date": fmt(last_restock),
        })
    rows = inject_nulls_and_dupes(
        rows, null_field_choices=["stock_quantity", "unit_price", "warehouse_location"],
        null_rate=0.02, dupe_rate=0.01,
    )
    return rows


# ---------------------------------------------------------------------------
# 3. SHIPMENTS (2000 rows)
# ---------------------------------------------------------------------------
def gen_shipments(suppliers, inventory, n=2000):
    rows = []
    supplier_ids = [s["supplier_id"] for s in suppliers]
    inv_lookup = [(it["inventory_id"], it["supplier_id"]) for it in inventory if it["supplier_id"]]

    for i in range(1, n + 1):
        shipment_id = f"SHP{i:06d}"
        inventory_id, supplier_id = random.choice(inv_lookup)
        # occasionally mismatch supplier on purpose to mimic real noisy data
        if random.random() < 0.05:
            supplier_id = random.choice(supplier_ids)

        shipment_date = random_date(datetime(2024, 1, 1), datetime(2025, 12, 31))
        expected_days = random.randint(2, 14)
        expected_delivery = shipment_date + timedelta(days=expected_days)

        status = random.choices(SHIPMENT_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        if status == "DELIVERED":
            drift = random.randint(-2, 5)
            actual_delivery = expected_delivery + timedelta(days=drift)
            actual_delivery_str = fmt(actual_delivery)
        elif status == "DELAYED":
            drift = random.randint(1, 10)
            actual_delivery = expected_delivery + timedelta(days=drift)
            actual_delivery_str = fmt(actual_delivery)
        else:
            actual_delivery_str = ""

        quantity_shipped = random.randint(10, 1000)
        destination_warehouse = random.choice(WAREHOUSES)
        shipping_cost = round(random.uniform(15.0, 2500.0), 2)

        rows.append({
            "shipment_id": shipment_id,
            "inventory_id": inventory_id,
            "supplier_id": supplier_id,
            "shipment_date": fmt(shipment_date),
            "expected_delivery_date": fmt(expected_delivery),
            "actual_delivery_date": actual_delivery_str,
            "quantity_shipped": quantity_shipped,
            "status": status,
            "destination_warehouse": destination_warehouse,
            "shipping_cost": shipping_cost,
        })
    rows = inject_nulls_and_dupes(
        rows, null_field_choices=["shipping_cost", "destination_warehouse"],
        null_rate=0.02, dupe_rate=0.01,
    )
    return rows


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


if __name__ == "__main__":
    suppliers = gen_suppliers(100)
    inventory = gen_inventory(suppliers, 1000)
    shipments = gen_shipments(suppliers, inventory, 2000)

    write_csv(f"{OUT_DIR}/suppliers.csv", suppliers, [
        "supplier_id", "supplier_name", "category", "country", "city",
        "contact_email", "phone", "rating", "onboarded_date",
    ])
    write_csv(f"{OUT_DIR}/inventory.csv", inventory, [
        "inventory_id", "product_id", "product_name", "category", "supplier_id",
        "warehouse_location", "stock_quantity", "reorder_level", "unit_price",
        "last_restock_date",
    ])
    write_csv(f"{OUT_DIR}/shipments.csv", shipments, [
        "shipment_id", "inventory_id", "supplier_id", "shipment_date",
        "expected_delivery_date", "actual_delivery_date", "quantity_shipped",
        "status", "destination_warehouse", "shipping_cost",
    ])

    print(f"suppliers: {len(suppliers)} rows")
    print(f"inventory: {len(inventory)} rows")
    print(f"shipments: {len(shipments)} rows")
