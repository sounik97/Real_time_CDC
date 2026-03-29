#!/usr/bin/env python3
import psycopg2
import random
import time
import argparse
import signal
import sys
from faker import Faker

# Initialize Faker
fake = Faker()

# Database connection settings
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'enterprise_db',
    'user': 'cdc_user',
    'password': 'cdc_password'
}

# Control flag for graceful shutdown
running = True

def signal_handler(signum, frame):
    global running
    print("\nStopping data generator...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

def create_customer(conn):
    """Create a new customer"""
    cursor = conn.cursor()
    
    tier = random.choices(
        ['bronze', 'silver', 'gold', 'platinum'],
        weights=[50, 30, 15, 5]
    )[0]
    
    cursor.execute("""
        INSERT INTO ecommerce.customers (email, first_name, last_name, phone, tier, total_spent)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, first_name, last_name
    """, (
        fake.unique.email(),
        fake.first_name(),
        fake.last_name(),
        fake.phone_number(),
        tier,
        round(random.uniform(0, 5000), 2)
    ))
    
    result = cursor.fetchone()
    conn.commit()
    return result[0]

def update_customer(conn):
    """Update a random customer"""
    cursor = conn.cursor()
    
    # Get a random customer
    cursor.execute("SELECT id, first_name, last_name FROM ecommerce.customers ORDER BY RANDOM() LIMIT 1")
    customer = cursor.fetchone()
    
    if not customer:
        return
    
    # Update tier or total spent
    if random.random() > 0.5:
        new_tier = random.choice(['bronze', 'silver', 'gold', 'platinum'])
        cursor.execute("""
            UPDATE ecommerce.customers
            SET tier = %s
            WHERE id = %s
        """, (new_tier, customer[0]))
    else:
        additional_spent = round(random.uniform(50, 500), 2)
        cursor.execute("""
            UPDATE ecommerce.customers
            SET total_spent = total_spent + %s
            WHERE id = %s
        """, (additional_spent, customer[0]))
    
    conn.commit()

def create_order(conn):
    """Create a new order with items"""
    cursor = conn.cursor()
    
    # Get a random customer
    cursor.execute("SELECT id FROM ecommerce.customers WHERE is_active = true ORDER BY RANDOM() LIMIT 1")
    customer = cursor.fetchone()
    
    if not customer:
        return
    
    # Get random products
    cursor.execute("SELECT id, price FROM ecommerce.products WHERE is_available = true ORDER BY RANDOM() LIMIT %s", 
                   (random.randint(1, 4),))
    products = cursor.fetchall()
    
    if not products:
        return
    
    # Calculate totals
    subtotal = 0
    order_items = []
    
    for product_id, price in products:
        quantity = random.randint(1, 3)
        item_total = float(price) * quantity
        subtotal += item_total
        order_items.append((product_id, quantity, float(price), item_total))
    
    tax = round(subtotal * 0.08, 2)
    shipping = round(random.uniform(5, 25), 2)
    total = round(subtotal + tax + shipping, 2)
    
    # Create shipping address
    shipping_address = {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "zip": fake.zipcode(),
        "country": "USA"
    }
    
    # Create order
    cursor.execute("""
        INSERT INTO ecommerce.orders (customer_id, subtotal, tax, shipping, total, shipping_address, payment_method, status)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
        RETURNING id, order_number
    """, (
        customer[0],
        subtotal,
        tax,
        shipping,
        total,
        str(shipping_address).replace("'", '"'),
        random.choice(['credit_card', 'debit_card', 'paypal', 'apple_pay']),
        'pending'
    ))
    
    order = cursor.fetchone()
    
    # Create order items
    for product_id, quantity, unit_price, total_price in order_items:
        cursor.execute("""
            INSERT INTO ecommerce.order_items (order_id, product_id, quantity, unit_price, total_price)
            VALUES (%s, %s, %s, %s, %s)
        """, (order[0], product_id, quantity, unit_price, total_price))
    
    conn.commit()
    return order[0]

def update_order_status(conn):
    """Update order status (simulating order lifecycle)"""
    cursor = conn.cursor()
    
    status_flow = {
        'pending': 'confirmed',
        'confirmed': 'processing',
        'processing': 'shipped',
        'shipped': 'delivered'
    }
    
    # Get an order that can be updated
    cursor.execute("""
        SELECT id, order_number, status 
        FROM ecommerce.orders 
        WHERE status IN ('pending', 'confirmed', 'processing', 'shipped')
        ORDER BY RANDOM() 
        LIMIT 1
    """)
    
    order = cursor.fetchone()
    
    if not order:
        return
    
    new_status = status_flow.get(order[2])
    
    if new_status:
        cursor.execute("""
            UPDATE ecommerce.orders
            SET status = %s
            WHERE id = %s
        """, (new_status, order[0]))
        conn.commit()

def update_inventory(conn):
    """Update inventory levels"""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT i.id, p.name, i.quantity, i.warehouse_location
        FROM ecommerce.inventory i
        JOIN ecommerce.products p ON p.id = i.product_id
        ORDER BY RANDOM() 
        LIMIT 1
    """)
    
    inventory = cursor.fetchone()
    
    if not inventory:
        return
    
    # Simulate inventory change (restock or sale)
    if random.random() > 0.3:
        # Sale - decrease
        change = -random.randint(1, 5)
        new_qty = max(0, inventory[2] + change)
    else:
        # Restock - increase
        change = random.randint(10, 50)
        new_qty = inventory[2] + change
    
    cursor.execute("""
        UPDATE ecommerce.inventory
        SET quantity = %s,
            last_restocked_at = CASE WHEN %s > 0 THEN CURRENT_TIMESTAMP ELSE last_restocked_at END
        WHERE id = %s
    """, (new_qty, change, inventory[0]))

    conn.commit()

def update_product_price(conn):
    """Update product price"""
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name, price FROM ecommerce.products ORDER BY RANDOM() LIMIT 1")
    product = cursor.fetchone()
    
    if not product:
        return
    
    # Adjust price by -10% to +15%
    adjustment = random.uniform(0.9, 1.15)
    new_price = round(float(product[2]) * adjustment, 2)
    
    cursor.execute("""
        UPDATE ecommerce.products
        SET price = %s
        WHERE id = %s
    """, (new_price, product[0]))

    conn.commit()

def run_demo(interval=2, duration=None):
    """Run the demo data generator"""
    conn = get_connection()
    start_time = time.time()
    operation_count = 0

    operations = [
        (create_customer, 10),
        (update_customer, 15),
        (create_order, 25),
        (update_order_status, 20),
        (update_inventory, 20),
        (update_product_price, 10),
    ]

    print(f"Starting data generation (interval: {interval}s)")
    
    while running:
        if duration and (time.time() - start_time) > duration:
            break

        try:
            funcs, weights = zip(*operations)
            operation = random.choices(funcs, weights=weights)[0]
            operation(conn)
            operation_count += 1
            time.sleep(interval)

        except psycopg2.Error as e:
            print(f"Database error: {e}")
            conn.rollback()
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

    conn.close()
    print(f"\nCompleted {operation_count} operations in {time.time() - start_time:.1f}s")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CDC Demo Data Generator')
    parser.add_argument('-i', '--interval', type=float, default=2, 
                        help='Interval between operations in seconds (default: 2)')
    parser.add_argument('-d', '--duration', type=int, default=None,
                        help='Duration to run in seconds (default: unlimited)')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    
    args = parser.parse_args()
    
    DB_CONFIG['host'] = args.host
    DB_CONFIG['port'] = args.port
    
    run_demo(interval=args.interval, duration=args.duration)
