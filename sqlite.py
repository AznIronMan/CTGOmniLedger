import pandas as pd
import sqlite3
from sqlite3 import Error

categories_list = [['Business, Assets '], ['Business, Business insurance'], ['Business, Commissions'], ['Business, Communications'], ['Business, Contractors'], ['Business, Credit Loans'], ['Business, Equipment Rental'], ['Business, Home office'], ['Business, Insurance Premiums'], ['Business, Inventory'], ['Business, Legal Pro Fees'], ['Business, Meals (Full)'], ['Business, Meals (Lite)'], ['Business, Office expenses'], ['Business, Office Rental'], ['Business, Misc Expenses'], ['Business, Repairs'], ['Business, Supplies'], ['Business, Taxes and licenses'], ['Business, Travel'], ['Business, Utilities'], ['Business, Vehicle'], ['Charitable Gifts, Charity Offerings'], ['Charitable Gifts, Tithes'], ['Clothing, Adrian'], ['Clothing, Arihana'], ['Clothing, Caliana'], ['Clothing, Geoff'], ['Clothing, Micah'], ['Clothing, Skylar'], ['Debt Payoff, American Express'], ['Debt Payoff, Apple Card'], ['Debt Payoff, Buick Enclave Lease'], ['Debt Payoff, PayPal Line'], ['Debt Payoff, Skylar PA Mohela'], ['Debt Payoff, Skylar PhD Navient'], ['Food, Groceries'], ['Food, Kids Lunch'], ['Food, Restaurants'], ['Healthcare, Dentist Visits'], ['Healthcare, Glasses/Contacts'], ['Healthcare, Medical Visits'], ['Healthcare, Medications'], ['Healthcare, Optometrist Visits'], ['Healthcare, Orthodonic Visits'], ['Healthcare, Other'], ['Housing, _'], ['Housing, _'], ['Housing, Air Filters'], ['Housing, HOA Dues'], ['Housing, Home Upgrades'], ['Housing, Mortgage'], [
    'Housing, Exterminators'], ['Housing, Pool Cleaning'], ['Housing, Replace Furniture'], ['Insurance, Minnesota Life'], ['Insurance, Progressive Car'], ['Insurance, Securian Finance'], ['Insurance, Standard Disability'], ['Insurance, Umbrella Policy'], ['Insurance, Zander Theft'], ['Kids, Adrian Activities'], ['Kids, Adrian Commission'], ['Kids, Boys Gym'], ['Kids, Childcare'], ['Kids, Girls Activities'], ['Kids, Kids Books'], ['Kids, Kids Subscriptions'], ['Kids, Micah Activities'], ['Kids, Micah Commission'], ['Personal, Geoff Gym'], ['Personal, Geoff Pocket'], ['Personal, Gifts'], ['Personal, House Care'], ['Personal, Skylar Pocket'], ['Personal, Skylar Training'], ['Personal, Starbucks'], ['Personal, Streaming Subscriptions'], ['Personal, Toiletries House Supplies'], ['Personal, Zumba/Les Mills'], ['Recreation, Date Night Fund'], ['Recreation, Movie Fund'], ['Recreation, Vacation Fund'], ['Savings, College Fund'], ['Savings, Emergency Fund'], ['Savings, Retirement Fund'], ['Transportation, Car Wash'], ['Transportation, Fines/Tickets'], ['Transportation, Fuel'], ['Transportation, License Taxes'], ['Transportation, Oil Change'], ['Transportation, OnStar'], ['Transportation, Parking Fees'], ['Transportation, Tires'], ['Transportation, Vehicle Preiars'], ['Utilities, Cell Phone'], ['Utilities, Portland General Electric'], ['Utilities, Tualatin Valley Water District'], ['Utilities, Walker Garbage'], ['Utilities, Xfinity Internet']]
split_categories_list = [item[0].split(', ') for item in categories_list]
categories_df = pd.DataFrame(split_categories_list, columns=[
                             'main_category', 'sub_category'])

database = 'financials.db'  # Replace with your actual database name


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def write_to_table(conn, table_name, data_dict):
    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join(['?' for _ in data_dict])
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cur = conn.cursor()
    cur.execute(sql, list(data_dict.values()))
    conn.commit()
    return cur.lastrowid


def read_from_table(conn, table_name, condition=None):
    cur = conn.cursor()
    query = f"SELECT * FROM {table_name}"

    if condition:
        query += f" WHERE {condition}"

    cur.execute(query)
    rows = cur.fetchall()

    return rows


def execute_raw_sql(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    return cur.fetchall()


def build_categories(conn, categories_df):
    table_name = 'categories'
    create_table_sql = """CREATE TABLE IF NOT EXISTS categories (
        id integer PRIMARY KEY AUTOINCREMENT,
        main_category text NOT NULL,
        sub_category text NOT NULL
    );"""
    create_table(conn, create_table_sql)

    for index, row in categories_df.iterrows():
        main_category, sub_category = row['main_category'], row['sub_category']
        write_to_table(conn, table_name, {
                       'main_category': main_category, 'sub_category': sub_category})


def build_transactions(conn):
    table_name = 'transactions'
    create_table_sql = """CREATE TABLE IF NOT EXISTS transactions (
        uuid text PRIMARY KEY,
        source text NOT NULL,
        type text NOT NULL,
        acctid text NOT NULL,
        date text NOT NULL,
        description text NOT NULL,
        debit real,
        credit real,
        main text,
        sub text
    );"""
    create_table(conn, create_table_sql)


if __name__ == '__main__':
    database = 'financials.db'
    conn = create_connection(database)
    if conn:
        build_transactions(conn)
        build_categories(conn, categories_df)
