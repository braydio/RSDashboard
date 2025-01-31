
import threading
import numpy as np
import time
import pandas as pd
import json
import os
import keyboard  # Capture keypress events
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.text import Text
from collections import deque


# Base directory for logs
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))

# File paths
HOLDINGS_LOG_CSV = os.path.join(BASE_DIR, "holdings_log.csv")
ORDERS_LOG_CSV = os.path.join(BASE_DIR, "orders_log.csv")
ERROR_LOG_FILE = os.path.join(BASE_DIR, "error_log.txt")
APP_LOG_FILE = os.path.join(BASE_DIR, "app.log")
ACCOUNT_MAPPING_FILE = os.path.join(BASE_DIR, "account_mapping.json")
WATCH_LIST_FILE = os.path.join(BASE_DIR, "watch_list.json")

# Ensure the logs directory exists
os.makedirs(BASE_DIR, exist_ok=True)

console = Console()
selected_broker = None  # Global variable to track broker filtering
last_logs = deque(maxlen=5)

# Load account mappings
def load_account_mappings():
    if os.path.exists(ACCOUNT_MAPPING_FILE):
        with open(ACCOUNT_MAPPING_FILE, "r") as f:
            data = json.load(f)

        if isinstance(data, dict):
            return data  # Correct format
        else:
            print("⚠️ Error: account_mappings.json is not in the expected dictionary format.")
            return {}

    return {}

# Load watch list
def load_watch_list():
    return json.load(open(WATCH_LIST_FILE)) if os.path.exists(WATCH_LIST_FILE) else {}

# Load holdings data
def load_holdings():
    if os.path.exists(HOLDINGS_LOG_CSV) and os.path.getsize(HOLDINGS_LOG_CSV) > 0:
        df = pd.read_csv(HOLDINGS_LOG_CSV, dtype={"Key": str})
        df.rename(columns={"Key": "account_id", "Stock": "ticker", "Quantity": "quantity", "Price": "average_price"}, inplace=True)
        return df
    return pd.DataFrame(columns=["account_id", "ticker", "quantity", "average_price"])

# Load orders data
def load_orders():
    if os.path.exists(ORDERS_LOG_CSV) and os.path.getsize(ORDERS_LOG_CSV) > 0:
        df = pd.read_csv(ORDERS_LOG_CSV, usecols=["Account Number", "Stock", "Order Type", "Quantity", "Price", "Date"], dtype={"Account Number": str})
        df.rename(columns={"Account Number": "account_id", "Stock": "ticker", "Order Type": "action", "Quantity": "quantity", "Price": "price", "Date": "date"}, inplace=True)
        return df
    return pd.DataFrame(columns=["account_id", "ticker", "action", "quantity", "price", "date"])

# Store last 5 logs persistently
def load_app_logs():
    global last_logs

    if os.path.exists(APP_LOG_FILE):
        with open(APP_LOG_FILE, "r") as f:
            logs = f.readlines()

        # Keep only the last 5 logs
        last_logs.extend(logs[-5:])
    return list(last_logs)

# Grouped holdings_log.csv by broker
def group_holdings_by_broker(holdings_df, account_mappings):
    """
    Groups holdings by broker using account_mappings.
    """
    # Create a mapping: account_id → broker
    account_to_broker = {}
    for broker, groups in account_mappings.items():
        for group in groups.values():
            account_to_broker.update({account_id: broker for account_id in group.keys()})

    # Add broker column to holdings
    holdings_df["broker"] = holdings_df["account_id"].map(account_to_broker)

    # Group by broker, summing quantity & computing weighted avg price
    grouped = holdings_df.groupby("broker").agg(
        total_quantity=("quantity", "sum"),
        total_value=("average_price", "sum")  # Sum of average price * quantity
    ).reset_index()

    return grouped

# Function to filter holdings & orders by broker
def filter_data_by_broker(holdings_df, orders_df, account_mappings, selected_broker=None):
    if selected_broker:
        broker_accounts = set()

        # Extract all account numbers under the selected broker
        if selected_broker in account_mappings:
            for group in account_mappings[selected_broker].values():
                broker_accounts.update(group.keys())

        holdings_df = holdings_df[holdings_df["account_id"].isin(broker_accounts)]
        orders_df = orders_df[orders_df["account_id"].isin(broker_accounts)]

    return holdings_df, orders_df

'''
# Create the dashboard display
def create_dashboard():
    holdings = load_holdings()
    orders = load_orders()
    account_mappings = load_account_mappings()

    holdings, orders = filter_data_by_broker(holdings, orders, account_mappings, selected_broker)

    table = Table(title=f"📊 Portfolio Overview ({selected_broker if selected_broker else 'All Brokers'})")
    table.add_column("Account ID", justify="center", style="cyan", no_wrap=True)
    table.add_column("Ticker", justify="center", style="magenta")
    table.add_column("Quantity", justify="right", style="green")
    table.add_column("Avg Price", justify="right", style="yellow")

    for _, row in holdings.iterrows():
        account_id = str(row["account_id"]) if pd.notna(row["account_id"]) else "N/A"
        ticker = str(row["ticker"]) if pd.notna(row["ticker"]) else "N/A"
        quantity = str(row["quantity"]) if pd.notna(row["quantity"]) else "0"
        avg_price_str = f"${row['average_price']:.2f}" if pd.notna(row["average_price"]) else "N/A"

        table.add_row(account_id, ticker, quantity, avg_price_str)

    return table
'''

# Set up main dashboard for display
def create_dashboard():
    holdings = load_holdings()
    orders = load_orders()
    account_mappings = load_account_mappings()

    # Group holdings by broker
    broker_holdings = group_holdings_by_broker(holdings, account_mappings)

    # 📊 Broker Overview Table
    table = Table(title="📊 Broker Overview", style="bold cyan")
    table.add_column("Broker", justify="left", style="cyan", no_wrap=True)
    table.add_column("Total Quantity", justify="right", style="green")
    table.add_column("Total Value", justify="right", style="yellow")

    for _, row in broker_holdings.iterrows():
        broker = row["broker"]
        total_quantity = f"{row['total_quantity']:.2f}"
        total_value = f"${row['total_value']:.2f}"
        table.add_row(broker, total_quantity, total_value)

    # 📈 Recent Orders Table
    orders_table = Table(title="📈 Recent Orders", style="bold magenta")
    orders_table.add_column("Account ID", justify="center", style="cyan", no_wrap=True)
    orders_table.add_column("Ticker", justify="center", style="magenta")
    orders_table.add_column("Action", justify="center", style="bold")
    orders_table.add_column("Quantity", justify="right", style="green")
    orders_table.add_column("Price", justify="right", style="yellow")

    recent_orders = orders.tail(5)
    for _, row in recent_orders.iterrows():
        action_color = "green" if row["action"].lower() == "buy" else "red"
        account_id = str(row["account_id"]) if pd.notna(row["account_id"]) else "N/A"
        ticker = str(row["ticker"]) if pd.notna(row["ticker"]) else "N/A"
        quantity = str(row["quantity"]) if pd.notna(row["quantity"]) else "0"
        price_str = f"${row['price']:.2f}" if pd.notna(row["price"]) else "N/A"
        orders_table.add_row(account_id, ticker, Text(row["action"], style=action_color), quantity, price_str)

    # 📜 Log Monitoring Table
    log_table = Table(title="📜 Monitoring Logs (Last 5 Entries)", style="bold blue")
    log_table.add_column("Log Entries", justify="left", style="blue")

    logs = load_app_logs()
    for log in logs:
        log_table.add_row(log.strip())

    return table, orders_table, log_table

# Function to handle broker filtering via keyboard input
def listen_for_broker_change():
    global selected_broker
    account_mappings = load_account_mappings()

    if not isinstance(account_mappings, dict):
        print("⚠️ Error: `account_mappings` is not a dictionary.")
        return

    brokers = sorted(account_mappings.keys())  # Extract broker names

    if not brokers:
        print("⚠️ No brokers found in account mappings.")
        return

    while True:
        print("\nSelect a broker to filter (Press Enter to keep current selection):")
        for i, broker in enumerate(brokers, start=1):
            print(f"[{i}] {broker}")
        print("[0] Show all brokers")

        try:
            choice = input("\nEnter choice: ").strip()
            if choice == "":
                return  # Keep current broker
            choice = int(choice)
            if choice == 0:
                selected_broker = None
            elif 1 <= choice <= len(brokers):
                selected_broker = brokers[choice - 1]
            else:
                print("⚠️ Invalid selection, try again.")
                continue

            print(f"\n🔄 Broker changed to: {selected_broker if selected_broker else 'All Brokers'}")
            return  # Exit function after selection

        except ValueError:
            print("⚠️ Invalid input, please enter a number.")

# Main function to run the dashboard
def main():
    try:
        # Start broker listener in a background thread
        threading.Thread(target=listen_for_broker_change, daemon=True).start()

        with Live(console=console, refresh_per_second=1) as live:
            last_check = time.time()

            while True:
                broker_table, orders_table, log_table = create_dashboard()  # ✅ Corrected unpacking
                live.update(f"\n{broker_table}\n\n{orders_table}\n\n{log_table}\n")
                console.print(broker_table)
                console.print(orders_table)  # ✅ Now printing Recent Orders
                console.print(log_table)

                time.sleep(1)

                if time.time() - last_check > 3:  # Small refresh delay
                    last_check = time.time()

    except KeyboardInterrupt:
        print("\n🔴 Program terminated by user.")
        exit(0)

if __name__ == "__main__":
    main()
