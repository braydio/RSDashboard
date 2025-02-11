
#!/usr/bin/env python3
import threading
import time
import pandas as pd
import json
import os
import sys
import select
import re
from collections import deque, defaultdict
from datetime import datetime
from rich.align import Align
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.table import Table
from rich.console import Console
from rich.text import Text
import argparse
from rich import box

# Base directory for logs
BASE_DIR_LOGS = os.path.abspath(os.path.join(os.path.dirname(__file__), "Volumes/logs"))
BASE_DIR_CONFIG = os.path.abspath(os.path.join(os.path.dirname(__file__), "Volumes/config"))

# File paths
HOLDINGS_LOG_CSV = os.path.join(BASE_DIR_LOGS, "holdings_log.csv")
ORDERS_LOG_CSV = os.path.join(BASE_DIR_LOGS, "orders_log.csv")
ERROR_LOG_FILE = os.path.join(BASE_DIR_LOGS, "error_log.txt")
APP_LOG_FILE = os.path.join(BASE_DIR_LOGS, "app.log")
ACCOUNT_MAPPING_FILE = os.path.join(BASE_DIR_CONFIG, "account_mapping.json")
WATCH_LIST_FILE = os.path.join(BASE_DIR_CONFIG, "watch_list.json")

# Ensure the logs directory exists
os.makedirs(BASE_DIR_LOGS, exist_ok=True)
os.makedirs(BASE_DIR_CONFIG, exist_ok=True)

console = Console()

# Global variables
selected_broker = None  # for interactive filtering by broker (by account mapping)
LOG_COUNT = 5           # default number of log messages to display (can be changed via flag)
BROKER_FILTER = None    # if set via flag, only show data for this Broker Number

last_logs = deque(maxlen=LOG_COUNT)
nasdaq_alerts = deque(maxlen=5)  # holds up to 5 Nasdaq corporate actions alerts
pending_orders = {}  # key: (action, ticker, broker), value: order details

# Global style variables (set later based on command-line flags)
PANEL_BORDER_STYLE = "white"  # default: panel border white
TABLE_BOX_STYLE = None        # default: no table borders

# --- Helper function to compute a module’s “height ratio” ---
def compute_ratio(table, base=1, threshold=5, max_ratio=3):
    """
    Computes a layout ratio based on the table’s number of rows.
    Every `threshold` rows increases the ratio by one unit (up to max_ratio).
    """
    try:
        count = table.row_count
    except Exception:
        count = 0
    # For every threshold rows, add one extra unit:
    ratio = base + (count // threshold)
    return min(ratio, max_ratio)

# --- Non-blocking Input Utility ---
def nonblocking_input(prompt="", timeout=0.1):
    sys.stdout.write(prompt)
    sys.stdout.flush()
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    if ready:
        return sys.stdin.readline().rstrip("\n")
    return None

# --- Data Loading Functions ---
def load_account_mappings():
    if os.path.exists(ACCOUNT_MAPPING_FILE):
        with open(ACCOUNT_MAPPING_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        else:
            console.print("[red]⚠️ Error: account_mapping.json is not in the expected dictionary format.[/red]")
            return {}
    return {}

def load_watch_list():
    if os.path.exists(WATCH_LIST_FILE):
        return json.load(open(WATCH_LIST_FILE))
    return {}

def load_holdings():
    if os.path.exists(HOLDINGS_LOG_CSV) and os.path.getsize(HOLDINGS_LOG_CSV) > 0:
        df = pd.read_csv(HOLDINGS_LOG_CSV)
        return df
    return pd.DataFrame()

def load_orders():
    if os.path.exists(ORDERS_LOG_CSV) and os.path.getsize(ORDERS_LOG_CSV) > 0:
        df = pd.read_csv(ORDERS_LOG_CSV, dtype={"Account Number": str})
        df.rename(columns={
            "Broker Name": "broker_name",
            "Broker Number": "broker_number",
            "Account Number": "account_id",
            "Order Type": "action",
            "Stock": "ticker",
            "Quantity": "quantity",
            "Price": "price",
            "Date": "date",
            "Timestamp": "timestamp"
        }, inplace=True)
        return df
    return pd.DataFrame(columns=["broker_name", "broker_number", "account_id", "action", "ticker", "quantity", "price", "date", "timestamp"])

def load_app_logs():
    global last_logs, LOG_COUNT
    if os.path.exists(APP_LOG_FILE):
        with open(APP_LOG_FILE, "r") as f:
            logs = f.readlines()
        last_logs = deque(logs[-LOG_COUNT:], maxlen=LOG_COUNT)
    return list(last_logs)

# --- Nasdaq Alerts Handling ---
def update_nasdaq_alerts():
    global nasdaq_alerts
    nasdaq_alerts.clear()
    if not os.path.exists(APP_LOG_FILE):
        return list(nasdaq_alerts)
    with open(APP_LOG_FILE, "r") as f:
        lines = f.readlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if "Received message:" in line:
            m_time = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}),\d+", line)
            if m_time:
                date_str, time_str = m_time.groups()
                try:
                    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                    formatted_dt = dt.strftime("%m/%d/%Y %I:%M %p").lstrip("0")
                except Exception:
                    formatted_dt = f"{time_str} {date_str}"
            else:
                formatted_dt = ""
            m_ticker = re.search(r"\((\w+)\)", line)
            ticker = m_ticker.group(1) if m_ticker else "UNKNOWN"
            url = None
            confirmed = False
            j = i + 1
            while j < len(lines) and "Received message:" not in lines[j]:
                if "URL detected in alert message:" in lines[j]:
                    parts = lines[j].split("URL detected in alert message:")
                    if len(parts) > 1:
                        url = parts[1].strip()
                if "Returning parsed info. Reverse split confirmed:" in lines[j]:
                    if "True" in lines[j]:
                        confirmed = True
                j += 1
            if confirmed:
                alert_msg = f"Reverse Stock Split Confirmed for {ticker} on {formatted_dt}"
                if url:
                    alert_msg += f" - {url}"
                if not alert_msg.startswith("📰"):
                    alert_msg = "📰 " + alert_msg
                nasdaq_alerts.append(alert_msg)
            i = j
        else:
            i += 1
    return list(nasdaq_alerts)

# --- Pending Orders Processing ---
def update_pending_orders():
    global pending_orders
    if not os.path.exists(APP_LOG_FILE):
        return
    with open(APP_LOG_FILE, "r") as f:
        lines = f.readlines()
    sched_pattern = re.compile(
        r"Scheduled\s+(buy|sell)\s+order:\s*([A-Za-z0-9,]+),\s*quantity:\s*([\d.]+),\s*broker:\s*([\w]+),\s*time:\s*([\d-]+\s[\d:]+)"
    )
    for line in lines:
        m = sched_pattern.search(line)
        if m:
            action = m.group(1).lower()
            tickers = [t.strip().upper() for t in m.group(2).split(",") if t.strip()]
            quantity = m.group(3)
            broker_field = m.group(4).lower()
            scheduled_time = m.group(5)
            for ticker in tickers:
                key = (action, ticker, broker_field)
                if key not in pending_orders:
                    pending_orders[key] = {
                        "quantity": quantity,
                        "time": scheduled_time,
                        "action": action,
                        "ticker": ticker,
                        "broker": broker_field,
                    }
    comp_pattern = re.compile(
        r"Sent command:\s*!rsa\s+(buy|sell)\s+[\d.]+\s+([A-Za-z0-9]+)(?:\s+(\w+))?"
    )
    for line in lines:
        m = comp_pattern.search(line)
        if m:
            action = m.group(1).lower()
            ticker = m.group(2).upper()
            broker_field = m.group(3) if m.group(3) else ""
            broker_field = broker_field.lower()
            key = (action, ticker, broker_field)
            if key in pending_orders:
                del pending_orders[key]

# --- Table Building Functions ---
def build_pending_orders_table():
    table = Table(title="📝 Scheduled Orders", style="bold green", box=TABLE_BOX_STYLE)
    table.add_column("Ticker", justify="center", style="magenta")
    table.add_column("Action", justify="center", style="cyan")
    table.add_column("Count", justify="center", style="yellow")
    table.add_column("Broker(s)", justify="center", style="green")
    table.add_column("Date & Time", justify="center", style="red")
    
    groups = defaultdict(list)
    for (action, ticker, broker), order in pending_orders.items():
        groups[(action, ticker)].append(order)
    
    for (action, ticker), orders_list in groups.items():
        count = len(orders_list)
        brokers = set(o["broker"] for o in orders_list)
        broker_str = f"{len(brokers)} Brokers" if len(brokers) > 1 else list(brokers)[0].capitalize()
        times = []
        for o in orders_list:
            try:
                dt = datetime.strptime(o["time"], "%Y-%m-%d %H:%M:%S")
                times.append(dt)
            except Exception:
                continue
        formatted_time = (min(times).strftime("%m/%d/%Y %I:%M %p").lstrip("0")
                          if times else "")
        table.add_row(ticker, action.capitalize(), str(count), broker_str, formatted_time)
    
    if not groups:
        table.add_row("No scheduled orders", "", "", "", "")
    return table

def build_recent_orders_table():
    orders_df = load_orders()
    if orders_df.empty:
        table = Table(title="⏳ Recent Orders", style="bold magenta", box=TABLE_BOX_STYLE)
        table.add_column("Ticker", justify="center", style="magenta")
        table.add_column("Action", justify="center", style="cyan")
        table.add_column("Count", justify="center", style="yellow")
        table.add_column("Broker(s)", justify="center", style="green")
        table.add_column("Date & Time", justify="center", style="red")
        table.add_row("N/A", "N/A", "0", "N/A", "")
        return table

    orders_df["timestamp_dt"] = pd.to_datetime(orders_df["timestamp"], errors="coerce")
    grouped = orders_df.groupby(["ticker", "action"]).agg(
        count=("ticker", "size"),
        last_timestamp=("timestamp_dt", "max"),
        brokers=("broker_name", lambda x: set(x.dropna()))
    ).reset_index()
    grouped = grouped.sort_values(by="last_timestamp", ascending=False).head(5)
    
    table = Table(title="⏳ Recent Orders", style="bold magenta", box=TABLE_BOX_STYLE)
    table.add_column("Ticker", justify="center", style="magenta")
    table.add_column("Action", justify="center", style="cyan")
    table.add_column("Count", justify="center", style="yellow")
    table.add_column("Broker(s)", justify="center", style="green")
    table.add_column("Date & Time", justify="center", style="red")
    
    for _, row in grouped.iterrows():
        ticker = row["ticker"].upper()
        action = row["action"].capitalize()
        count = row["count"]
        brokers = row["brokers"]
        broker_str = f"{len(brokers)} Brokers" if brokers and len(brokers) > 1 else (list(brokers)[0] if brokers else "N/A")
        last_dt = row["last_timestamp"]
        formatted_time = (last_dt.strftime("%m/%d/%Y %I:%M %p").lstrip("0")
                          if pd.notna(last_dt) else "")
        table.add_row(ticker, action, str(count), broker_str, formatted_time)
    
    return table

def build_order_summary_by_broker_table():
    orders_df = load_orders()
    if orders_df.empty:
        table = Table(title="Order Summary by Broker", style="bold blue", box=TABLE_BOX_STYLE)
        table.add_column("Broker", justify="center", style="magenta")
        table.add_column("Order Count", justify="center", style="yellow")
        table.add_column("Unique Accounts", justify="center", style="green")
        table.add_column("Total Quantity", justify="center", style="cyan")
        table.add_column("Total Order Value", justify="center", style="red")
        table.add_row("N/A", "0", "0", "0", "$0.00")
        return table
    
    orders_df["quantity"] = pd.to_numeric(orders_df["quantity"], errors="coerce").fillna(0)
    orders_df["price"] = pd.to_numeric(orders_df["price"], errors="coerce").fillna(0)
    orders_df["order_value"] = orders_df["quantity"] * orders_df["price"]
    
    grouped = orders_df.groupby("broker_name").agg(
        order_count=("broker_name", "size"),
        unique_accounts=("account_id", pd.Series.nunique),
        total_quantity=("quantity", "sum"),
        total_value=("order_value", "sum")
    ).reset_index()
    
    table = Table(title="Order Summary by Broker", style="bold blue", box=TABLE_BOX_STYLE)
    table.add_column("Broker", justify="center", style="magenta")
    table.add_column("Order Count", justify="center", style="yellow")
    table.add_column("Unique Accounts", justify="center", style="green")
    table.add_column("Total Quantity", justify="center", style="cyan")
    table.add_column("Total Order Value", justify="center", style="red")
    
    for _, row in grouped.iterrows():
        table.add_row(
            str(row["broker_name"]),
            str(row["order_count"]),
            str(row["unique_accounts"]),
            f"{row['total_quantity']:.2f}",
            f"${row['total_value']:.2f}"
        )
    # Add Grand Total row for Total Order Value if data exists
    if not grouped.empty:
        grand_total_order_value = grouped["total_value"].sum()
        table.add_row("Grand Total", "", "", "", f"${grand_total_order_value:.2f}", style="bold")
    return table

def build_watchlist_table():
    raw_watchlist = load_watch_list()
    holdings = load_holdings()
    table = Table(title="📌 Watchlist", style="bold yellow", box=TABLE_BOX_STYLE)
    table.add_column("Stock", justify="left", style="magenta")
    table.add_column("Price", justify="right", style="green")
    table.add_column("Split Date", justify="center", style="cyan")
    table.add_column("Split Ratio", justify="center", style="yellow")
    
    watchlist = []
    if isinstance(raw_watchlist, dict):
        for stock, details in raw_watchlist.items():
            if isinstance(details, dict):
                watchlist.append({
                    "stock": stock,
                    "split_date": details.get("split_date", "N/A"),
                    "split_ratio": details.get("split_ratio", "N/A")
                })
            else:
                watchlist.append({"stock": stock, "split_date": "N/A", "split_ratio": "N/A"})
    elif isinstance(raw_watchlist, list):
        for entry in raw_watchlist:
            if isinstance(entry, dict):
                watchlist.append(entry)
            else:
                watchlist.append({"stock": entry, "split_date": "N/A", "split_ratio": "N/A"})
    else:
        watchlist = []

    for entry in watchlist:
        stock = entry.get("stock", "").upper()
        raw_date = entry.get("split_date", "N/A")
        if raw_date != "N/A":
            try:
                dt = datetime.strptime(raw_date, "%m/%d")
                formatted_date = dt.strftime("%m/%d")
            except Exception:
                formatted_date = raw_date
        else:
            formatted_date = "N/A"
        split_ratio = str(entry.get("split_ratio", "N/A"))
        if not holdings.empty and "Stock" in holdings.columns:
            sub = holdings[holdings["Stock"].str.upper() == stock]
            price_str = f"${sub['Price'].mean():.2f}" if not sub.empty else "N/A"
        else:
            price_str = "N/A"
        table.add_row(stock, price_str, formatted_date, split_ratio)
    
    if table.row_count == 0:
        table.add_row("No watchlist stocks", "", "", "")
    return table

def group_holdings_by_broker(holdings_df):
    if holdings_df.empty:
        return pd.DataFrame(columns=["Broker Name", "Broker Number", "Accounts", "Total Quantity", "Total Value"])
    grouped = holdings_df.groupby(["Broker Name", "Broker Number"], as_index=False).agg(
        Accounts=("Account Number", "nunique"),
        **{
            "Total Quantity": ("Quantity", "sum"),
            "Total Value": ("Position Value", "sum")
        }
    )
    return grouped

def filter_data_by_broker(holdings_df, orders_df, account_mappings, selected_broker=None):
    global BROKER_FILTER
    # If a broker filter (by number) is provided, filter by that instead of account mapping.
    if BROKER_FILTER is not None:
        if "Broker Number" in holdings_df.columns:
            try:
                holdings_df = holdings_df[holdings_df["Broker Number"].astype(int) == BROKER_FILTER]
            except Exception:
                holdings_df = holdings_df[holdings_df["Broker Number"] == BROKER_FILTER]
        if "broker_number" in orders_df.columns:
            orders_df["broker_number"] = pd.to_numeric(orders_df["broker_number"], errors="coerce")
            orders_df = orders_df[orders_df["broker_number"] == BROKER_FILTER]
    elif selected_broker:
        broker_accounts = set()
        if selected_broker in account_mappings:
            for group in account_mappings[selected_broker].values():
                broker_accounts.update(group.keys())
        if "account_id" in holdings_df.columns:
            holdings_df = holdings_df[holdings_df["account_id"].isin(broker_accounts)]
        if "account_id" in orders_df.columns:
            orders_df = orders_df[orders_df["account_id"].isin(broker_accounts)]
    return holdings_df, orders_df

def create_dashboard():
    update_pending_orders()
    update_nasdaq_alerts()
    
    holdings = load_holdings()
    orders = load_orders()
    account_mappings = load_account_mappings()
    _ = load_watch_list()  # not used directly here
    
    holdings, orders = filter_data_by_broker(holdings, orders, account_mappings, selected_broker)
    
    grouped = group_holdings_by_broker(holdings)
    # Build Broker Overview Table
    broker_table = Table(title="📂 Broker Overview", style="bold cyan", box=TABLE_BOX_STYLE)
    broker_table.add_column("Broker Name", justify="left", style="cyan", no_wrap=True)
    broker_table.add_column("Broker Number", justify="center", style="magenta")
    broker_table.add_column("Accounts", justify="center", style="green")
    broker_table.add_column("Total Quantity", justify="right", style="yellow")
    broker_table.add_column("Total Value", justify="right", style="red")
    if not grouped.empty:
        for _, row in grouped.iterrows():
            broker_table.add_row(
                str(row["Broker Name"]),
                str(row["Broker Number"]),
                str(row["Accounts"]),
                f'{row["Total Quantity"]:.2f}',
                f'${row["Total Value"]:.2f}'
            )
        # Add Grand Total row for Total Value
        grand_total_value = grouped["Total Value"].sum()
        broker_table.add_row("Grand Total", "", "", "", f'${grand_total_value:.2f}', style="bold")
    else:
        broker_table.add_row("N/A", "N/A", "0", "0", "$0.00")
    
    watchlist_table = build_watchlist_table()
    recent_orders_table = build_recent_orders_table()
    scheduled_orders_table = build_pending_orders_table()
    order_summary_table = build_order_summary_by_broker_table()
    
    # --- Build the Log Entries Table with latest timestamp in its title ---
    logs = load_app_logs()
    latest_timestamp = ""
    if logs:
        latest_line = logs[-1].strip()
        m = re.match(r'^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}),\d+', latest_line)
        if m:
            date_str, time_str = m.groups()
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                latest_timestamp = dt.strftime("%m/%d/%Y %I:%M %p").lstrip("0")
            except Exception:
                latest_timestamp = ""
    new_title = f"📜 Monitoring Logs (Last {LOG_COUNT})"
    if latest_timestamp:
        new_title += f" - Last Log: {latest_timestamp}"
    log_table = Table(title=new_title, style="bold blue", box=TABLE_BOX_STYLE)
    log_table.add_column("Log Entry", justify="left", style="blue")
    if logs:
        for log in logs:
            # Remove the timestamp from each log line
            clean_log = re.sub(r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d+\s*', '', log.strip())
            log_table.add_row(clean_log)
    else:
        log_table.add_row("No logs available")
    
    nasdaq_list = update_nasdaq_alerts()
    nasdaq_table = Table(title="📰 Nasdaq Corporate Actions", style="bold red", box=TABLE_BOX_STYLE)
    nasdaq_table.add_column("Alert Message", justify="left", style="red")
    if nasdaq_list:
        for alert in nasdaq_list:
            nasdaq_table.add_row(alert.strip())
    else:
        nasdaq_table.add_row("No alerts")
    
    return broker_table, watchlist_table, recent_orders_table, scheduled_orders_table, order_summary_table, log_table, nasdaq_table

def listen_for_broker_change():
    global selected_broker
    account_mappings = load_account_mappings()
    brokers = sorted(account_mappings.keys())
    if not brokers:
        return
    console.print("\n[bold green]Tip: At any time, type the broker number (or 0 for all) and press Enter to update the filter.[/bold green]")
    while True:
        user_input = nonblocking_input("", timeout=0.5)
        if user_input is not None and user_input.strip() != "":
            try:
                choice_int = int(user_input)
                if choice_int == 0:
                    selected_broker = None
                elif 1 <= choice_int <= len(brokers):
                    selected_broker = brokers[choice_int - 1]
                else:
                    console.print("[red]⚠️ Invalid selection, try again.[/red]")
                    continue
                console.print(f"\n[bold green]✅ Broker changed to: {selected_broker if selected_broker else 'All Brokers'}[/bold green]")
            except ValueError:
                console.print("[red]⚠️ Invalid input, please enter a number.[/red]")
        time.sleep(0.5)

def main():
    parser = argparse.ArgumentParser(
        description="Dashboard with options to hide/show modules and control border styles."
    )
    parser.add_argument("--hide-broker", action="store_true", help="Hide Broker Overview module")
    parser.add_argument("--hide-watchlist", action="store_true", help="Hide Watchlist module")
    parser.add_argument("--hide-recent", action="store_true", help="Hide Recent Orders module")
    parser.add_argument("--hide-scheduled", action="store_true", help="Hide Scheduled Orders module")
    parser.add_argument("--hide-summary", action="store_true", help="Hide Order Summary by Broker module")
    parser.add_argument("--hide-logs", action="store_true", help="Hide Log Monitoring module")
    parser.add_argument("--hide-nasdaq", action="store_true", help="Hide Nasdaq Corporate Actions module")
    parser.add_argument("--no-panel-border", action="store_true", help="Disable panel borders")
    parser.add_argument("--no-table-border", action="store_true", help="Disable table borders")
    parser.add_argument("--panel-only", action="store_true", help="Show only panel borders (no table borders)")
    parser.add_argument("--table-only", action="store_true", help="Show only table borders (no panel borders)")
    parser.add_argument("--log-count", type=int, default=5, help="Number of log messages to display in the Monitoring Logs table (default 5)")
    parser.add_argument("--broker-filter", type=int, default=None, help="Filter to display only data for the specified Broker Number")
    args = parser.parse_args()

    global PANEL_BORDER_STYLE, TABLE_BOX_STYLE, LOG_COUNT, BROKER_FILTER, selected_broker
    LOG_COUNT = args.log_count
    if args.broker_filter is not None:
        BROKER_FILTER = args.broker_filter
        # Optionally disable interactive filtering when a broker filter is provided.
        selected_broker = None
        console.print(f"[bold green]Filtering data for Broker Number: {BROKER_FILTER}[/bold green]")
        
    if args.panel_only:
        PANEL_BORDER_STYLE = "white"
        TABLE_BOX_STYLE = box.ROUNDED  # still show table borders by default
    elif args.table_only:
        PANEL_BORDER_STYLE = "black"
        TABLE_BOX_STYLE = box.ROUNDED
    else:
        PANEL_BORDER_STYLE = None if args.no_panel_border else "black"
        TABLE_BOX_STYLE = None if args.no_table_border else box.ROUNDED

    try:
        console.print("\n[bold white on black]RSDashboard 🔄 Initializing...[/bold white on black]")
        time.sleep(3)
        
        console.print("\n[bold green]🔄 Starting Dashboard...[/bold green]")
        # Start interactive broker change thread only if no broker filter flag was provided
        if BROKER_FILTER is None:
            threading.Thread(target=listen_for_broker_change, daemon=True).start()
        with Live(refresh_per_second=2, screen=True) as live:
            rows = []  # list of row Layouts to be added to master_layout

            while True:
                # Get the module tables
                broker_table, watchlist_table, recent_orders_table, scheduled_orders_table, order_summary_table, log_table, nasdaq_table = create_dashboard()
                
                rows = []  # rebuild rows each refresh
                
                # --- Row 1: Broker Overview & Watchlist ---
                modules_row1 = []
                ratios_row1 = []
                if not args.hide_broker:
                    r = compute_ratio(broker_table)
                    modules_row1.append(Layout(Panel(Align.center(broker_table), border_style=PANEL_BORDER_STYLE), name="broker", ratio=r))
                    ratios_row1.append(r)
                if not args.hide_watchlist:
                    r = compute_ratio(watchlist_table)
                    modules_row1.append(Layout(Panel(Align.center(watchlist_table), border_style=PANEL_BORDER_STYLE), name="watchlist", ratio=r))
                    ratios_row1.append(r)
                if modules_row1:
                    row1 = Layout()
                    row1.split_row(*modules_row1)
                    row1_ratio = max(ratios_row1) if ratios_row1 else 1
                    rows.append(Layout(row1, name="top", ratio=row1_ratio))
                
                # --- Row 2: Recent Orders & Scheduled Orders ---
                modules_row2 = []
                ratios_row2 = []
                if not args.hide_recent:
                    r = compute_ratio(recent_orders_table)
                    modules_row2.append(Layout(Panel(Align.center(recent_orders_table), border_style=PANEL_BORDER_STYLE), name="recent", ratio=r))
                    ratios_row2.append(r)
                if not args.hide_scheduled:
                    r = compute_ratio(scheduled_orders_table)
                    modules_row2.append(Layout(Panel(Align.center(scheduled_orders_table), border_style=PANEL_BORDER_STYLE), name="scheduled", ratio=r))
                    ratios_row2.append(r)
                if modules_row2:
                    row2 = Layout()
                    row2.split_row(*modules_row2)
                    row2_ratio = max(ratios_row2) if ratios_row2 else 1
                    rows.append(Layout(row2, name="orders", ratio=row2_ratio))
                
                # --- Row 3: Order Summary by Broker ---
                if not args.hide_summary:
                    r = compute_ratio(order_summary_table)
                    rows.append(Layout(Panel(Align.center(order_summary_table), border_style=PANEL_BORDER_STYLE), name="summary", ratio=r))
                
                # --- Row 4: Logs ---
                if not args.hide_logs:
                    r = compute_ratio(log_table)
                    rows.append(Layout(Panel(Align.center(log_table), border_style=PANEL_BORDER_STYLE), name="logs", ratio=r))
                
                # --- Row 5: Nasdaq Corporate Actions ---
                if not args.hide_nasdaq:
                    r = compute_ratio(nasdaq_table)
                    rows.append(Layout(Panel(Align.center(nasdaq_table), border_style=PANEL_BORDER_STYLE), name="nasdaq", ratio=r))
                
                master_layout = Layout(name="root")
                if rows:
                    master_layout.split_column(*rows)
                else:
                    master_layout.update(Panel("No modules selected to display.", style="bold red"))
                
                live.update(master_layout)
                time.sleep(1)
    except KeyboardInterrupt:
        console.print("\n[bold red]🔴 Program terminated by user.[/bold red]")
        exit(0)

if __name__ == "__main__":
    main()
