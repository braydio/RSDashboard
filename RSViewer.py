
import threading
import numpy as np
import time
import pandas as pd
import json
import os
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
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


def load_holdings():
    if os.path.exists(HOLDINGS_LOG_CSV) and os.path.getsize(HOLDINGS_LOG_CSV) > 0:
        df = pd.read_csv(HOLDINGS_LOG_CSV)
        print("Holdings CSV Columns:", df.columns.tolist())  # ✅ Debug print
        return df
    return pd.DataFrame()

'''
# Load holdings data
def load_holdings():
    if os.path.exists(HOLDINGS_LOG_CSV) and os.path.getsize(HOLDINGS_LOG_CSV) > 0:
        df = pd.read_csv(HOLDINGS_LOG_CSV, dtype={"Key": str})
        df.rename(columns={"Key": "account_id", "Stock": "ticker", "Quantity": "quantity", "Price": "average_price"}, inplace=True)
        return df
    return pd.DataFrame(columns=["account_id", "ticker", "quantity", "average_price"])
'''

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
def group_holdings_by_broker(holdings_df):
    """
    Groups holdings by broker using the 'Broker Name' column directly.
    """
    if holdings_df.empty:
        return pd.DataFrame(columns=["Broker Name", "total_quantity", "total_value"])

    # 🔹 Ensure correct column names
    quantity_col = "Quantity" if "Quantity" in holdings_df.columns else "Shares"
    value_col = "Position Value" if "Position Value" in holdings_df.columns else "Account Total"

    # Group by Broker Name
    grouped = holdings_df.groupby("Broker Name", as_index=False).agg(
        total_quantity=(quantity_col, "sum"),
        total_value=(value_col, "sum")  # Sum of total value of positions
    )

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

# Set up main dashboard for display
def create_dashboard():
    holdings = load_holdings()
    orders = load_orders()
    account_mappings = load_account_mappings()
    watchlist = load_watch_list()  # ✅ Load watchlist

    # Group holdings by broker
    broker_holdings = group_holdings_by_broker(holdings)

    # 📊 Broker Overview Table
    broker_table = Table(title="📊 Broker Overview", style="bold cyan")
    broker_table.add_column("Broker", justify="left", style="cyan", no_wrap=True)
    broker_table.add_column("Total Quantity", justify="right", style="green")
    broker_table.add_column("Total Value", justify="right", style="yellow")

    for _, row in broker_holdings.iterrows():
        broker = row["Broker Name"]
        total_quantity = f"{row['total_quantity']:.2f}"
        total_value = f"${row['total_value']:.2f}"
        broker_table.add_row(broker, total_quantity, total_value)

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
        orders_table.add_row(
            str(row["account_id"]),
            row["ticker"],
            Text(row["action"], style=action_color),
            str(row["quantity"]),
            f"${row['price']:.2f}"
        )

    # 📜 Log Monitoring Table
    log_table = Table(title="📜 Monitoring Logs (Last 5 Entries)", style="bold blue")
    log_table.add_column("Log Entries", justify="left", style="blue")

    logs = load_app_logs()
    for log in logs:
        log_table.add_row(log.strip())

    # 📌 Watchlist Table
    watchlist_table = Table(title="📌 Watchlist Overview", style="bold yellow")
    watchlist_table.add_column("Stock", justify="left", style="magenta")
    watchlist_table.add_column("Total Brokers", justify="right", style="cyan")
    watchlist_table.add_column("Holding Brokers", justify="right", style="green")

    total_brokers = len(account_mappings)  # ✅ Total unique brokers

    for stock in watchlist:
        # Count brokers with positions in this stock
        brokers_with_positions = holdings[holdings["Stock"] == stock]["Broker Name"].nunique()
        watchlist_table.add_row(stock, str(total_brokers), str(brokers_with_positions))

    return broker_table, orders_table, log_table, watchlist_table

# Function to handle broker filtering via keyboard input
def listen_for_broker_change():
    global selected_broker
    account_mappings = load_account_mappings()

    if not isinstance(account_mappings, dict):
        print("⚠️ Error: `account_mappings` is not a dictionary.")
        return

    brokers = sorted(account_mappings.keys())

    if not brokers:
        print("⚠️ No brokers found in account mappings.")
        return

    print("\n🔄 Available Brokers:")
    for i, broker in enumerate(brokers, start=1):
        print(f"[{i}] {broker}")
    print("[0] Show all brokers")

    while True:
        choice = input("\nSelect a broker (Press Enter to keep current): ").strip()
        if choice == "":
            return  # ✅ Keep the last selected broker
        try:
            choice = int(choice)
            if choice == 0:
                selected_broker = None  # ✅ Reset filtering
            elif 1 <= choice <= len(brokers):
                selected_broker = brokers[choice - 1]  # ✅ Update broker
            else:
                print("⚠️ Invalid selection, try again.")
                continue

            print(f"\n✅ Broker changed to: {selected_broker if selected_broker else 'All Brokers'}")
            return  # ✅ Exit once a broker is selected

        except ValueError:
            print("⚠️ Invalid input, please enter a number.")


# Main function to run the dashboard
def main():
    try:
        # ✅ Allow input time before running the dashboard
        time.sleep(2)
        print("\n🔄 Starting Dashboard...")

        # Start broker listener in a background thread
        threading.Thread(target=listen_for_broker_change, daemon=True).start()

        with Live(console=console, refresh_per_second=1, screen=True) as live:
            last_check = time.time()

            while True:
                broker_table, orders_table, log_table, watchlist_table = create_dashboard()

                # ✅ Properly display all tables
                layout = Layout()
                layout.split_column(
                    Layout(Panel(broker_table, title="📊 Broker Overview", border_style="cyan"), size=10),
                    Layout(Panel(orders_table, title="📈 Recent Orders", border_style="magenta")),
                    Layout(Panel(log_table, title="📜 Monitoring Logs", border_style="blue"), size=8),
                    Layout(Panel(watchlist_table, title="📌 Watchlist Overview", border_style="yellow"), size=8),
                )

                live.update(layout)

                time.sleep(1)

                if time.time() - last_check > 3:  # Small refresh delay
                    last_check = time.time()

    except KeyboardInterrupt:
        print("\n🔴 Program terminated by user.")
        exit(0)



if __name__ == "__main__":
    main()
