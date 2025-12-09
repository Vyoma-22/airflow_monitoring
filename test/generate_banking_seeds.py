#!/usr/bin/env python3
"""
generate_banking_seeds.py

Generates synthetic banking seeds for Option A (full data regenerated daily).
Creates CSV files under ./seeds/:
  - customers.csv
  - accounts.csv
  - transactions.csv
  - cards.csv
  - branches.csv
  - employees.csv

Each row includes `business_effective_date`. The script is configurable.
"""

import csv
import os
import random
from datetime import datetime, timedelta
from uuid import uuid4

# ---------- CONFIG ----------
OUTPUT_DIR = "seeds"
START_DATE = "2024-01-01"        # first business date (YYYY-MM-DD)
NUM_DAYS = 60                    # number of business days to generate
CUSTOMERS_PER_DAY = 20           # adjust: number of unique customers created each day
MAX_ACCOUNTS_PER_CUSTOMER = 3
MIN_TRANSACTIONS_PER_ACCOUNT = 3
MAX_TRANSACTIONS_PER_ACCOUNT = 12
BRANCHES_PER_DAY = 4             # will rotate/variate branch names
SEED_RANDOM = 42
# -----------------------------

random.seed(SEED_RANDOM)

os.makedirs(OUTPUT_DIR, exist_ok=True)

start = datetime.strptime(START_DATE, "%Y-%m-%d").date()

# Helpers for fake data
first_names = ["Alex","Priya","Daniel","Emily","Marco","Liam","Olivia","Noah","Ava","Ethan","Sophia","Mason","Isabella","Lucas","Mia"]
last_names  = ["Carter","Shah","Gomez","Nguyen","Silva","Smith","Johnson","Brown","Davis","Wilson","Garcia","Martinez"]
cities = ["New York","Chicago","Miami","Seattle","San Diego","Austin","Denver","Boston","Atlanta","San Francisco"]
merchants = ["Starbucks","Amazon","Walmart","Spotify","Uber","Apple Store","Payroll Inc","Cash Deposit","Bank Transfer","Target","Netflix","Shell"]
categories = ["food","shopping","grocery","subscription","transport","electronics","salary","deposit","transfer","utility","entertainment"]

# Persistent branch templates (we'll rotate some variations per day)
base_branches = [
    (301, "Midtown Branch"),
    (302, "Lakeshore Branch"),
    (303, "Capitol Hill"),
    (304, "La Jolla Branch"),
]

# Create CSV writer utility
def write_csv(path, header, rows):
    with open(path, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

# We'll aggregate all rows across days for each table (dbt seeds will have all dates)
customers_rows = []
accounts_rows = []
transactions_rows = []
cards_rows = []
branches_rows = []
employees_rows = []

txn_global_id = 500000

for day_offset in range(NUM_DAYS):
    bdate = start + timedelta(days=day_offset)
    bdate_str = bdate.isoformat()

    # build branches for the day (slightly vary names/ids so Option A is full regen)
    day_branches = []
    for i in range(BRANCHES_PER_DAY):
        branch_id = 301 + i + day_offset*10  # unique-ish id per day
        branch_name = f"{base_branches[i % len(base_branches)][1]} - {bdate_str}"
        city = random.choice(cities)
        day_branches.append((branch_id, branch_name, city, bdate_str))
        branches_rows.append([branch_id, branch_name, city, bdate_str])

    # employees per day (unique per day)
    for i in range(3):  # 3 employees per day
        emp_id = 8000 + day_offset*10 + i
        branch_choice = day_branches[i % len(day_branches)][0]
        full_name = random.choice(first_names) + " " + random.choice(last_names)
        role = random.choice(["manager","teller","associate"])
        hire_date = bdate - timedelta(days=random.randint(0, 365*3))
        employees_rows.append([emp_id, branch_choice, full_name, role, hire_date.isoformat(), bdate_str])

    # create customers for the day
    for c in range(CUSTOMERS_PER_DAY):
        customer_id = 1000 + day_offset*CUSTOMERS_PER_DAY + c
        first = random.choice(first_names)
        last = random.choice(last_names)
        dob = datetime.strftime(datetime(1960,1,1) + timedelta(days=random.randint(0,20000)), "%Y-%m-%d")
        email = f"{first.lower()}.{last.lower()}{customer_id}@example.com"
        phone = f"555-{random.randint(1000,9999)}"
        join_date = bdate - timedelta(days=random.randint(0,365))
        city = random.choice(cities)
        customers_rows.append([customer_id, first, last, dob, email, phone, join_date.isoformat(), city, bdate_str])

        # create accounts for this customer
        num_accounts = random.randint(1, MAX_ACCOUNTS_PER_CUSTOMER)
        for a in range(num_accounts):
            account_id = 20000 + customer_id*10 + a
            acct_type = random.choice(["checking","savings","credit"])
            open_date = join_date + timedelta(days=random.randint(0,30))
            status = random.choice(["active","active","active","closed"])  # mostly active
            branch_id = random.choice(day_branches)[0]
            current_balance = round(random.uniform(-1000, 20000), 2)
            accounts_rows.append([account_id, customer_id, acct_type, open_date.isoformat(), status, branch_id, current_balance, bdate_str])

            # create cards for this account
            if acct_type in ("checking","savings") or random.random() < 0.6:
                card_id = 9000 + account_id
                card_type = random.choice(["debit","credit"]) if acct_type!="savings" else "debit"
                issued_date = open_date + timedelta(days=random.randint(0,30))
                card_status = random.choice(["active","active","active","blocked"])
                cards_rows.append([card_id, customer_id, account_id, card_type, issued_date.isoformat(), card_status, bdate_str])

            # create transactions for this account this business date
            num_txns = random.randint(MIN_TRANSACTIONS_PER_ACCOUNT, MAX_TRANSACTIONS_PER_ACCOUNT)
            for t in range(num_txns):
                txn_global_id += 1
                txn_id = txn_global_id
                txn_time = datetime.combine(bdate, datetime.min.time()) + timedelta(
                    seconds=random.randint(0, 86399)
                )
                txn_type = random.choices(["debit","credit"], weights=[0.75, 0.25])[0]
                amount = round(random.uniform(1.0, 5000.0), 2)
                merchant = random.choice(merchants)
                city = random.choice(cities)
                category = random.choice(categories)
                transactions_rows.append([txn_id, account_id, txn_time.isoformat(sep=' '), txn_type, amount, merchant, city, category, bdate_str])

# write CSVs
write_csv(os.path.join(OUTPUT_DIR, "customers.csv"),
          ["business_effective_date","customer_id","first_name","last_name","dob","email","phone","join_date","city"],
          [[r[3], r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]] if False else # placeholder
           None for _ in range(0)])  # hack: we'll write properly below

# Because we shaped rows in different column orders, write each with correct headers:

# customers
write_csv(os.path.join(OUTPUT_DIR, "customers.csv"),
          ["business_effective_date","customer_id","first_name","last_name","dob","email","phone","join_date","city"],
          [[row[8], row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]] for row in customers_rows])

# accounts
write_csv(os.path.join(OUTPUT_DIR, "accounts.csv"),
          ["business_effective_date","account_id","customer_id","account_type","open_date","status","branch_id","current_balance"],
          [[row[7], row[0], row[1], row[2], row[3], row[4], row[5], row[6]] for row in accounts_rows])

# transactions
write_csv(os.path.join(OUTPUT_DIR, "transactions.csv"),
          ["business_effective_date","txn_id","account_id","txn_timestamp","txn_type","amount","merchant","city","category"],
          [[row[8], row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]] for row in transactions_rows])

# cards
write_csv(os.path.join(OUTPUT_DIR, "cards.csv"),
          ["business_effective_date","card_id","customer_id","account_id","card_type","issued_date","status"],
          [[row[6], row[0], row[1], row[2], row[3], row[4], row[5]] for row in cards_rows])

# branches
write_csv(os.path.join(OUTPUT_DIR, "branches.csv"),
          ["business_effective_date","branch_id","branch_name","city"],
          [[row[3], row[0], row[1], row[2]] for row in branches_rows])

# employees
write_csv(os.path.join(OUTPUT_DIR, "employees.csv"),
          ["business_effective_date","employee_id","branch_id","full_name","role","hire_date"],
          [[row[5], row[0], row[1], row[2], row[3], row[4]] for row in employees_rows])

print(f"Generated seeds in ./{OUTPUT_DIR}/")
print("Files:")
for f in os.listdir(OUTPUT_DIR):
    print("  -", f)
