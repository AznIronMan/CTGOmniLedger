
from datetime import datetime
import csv
import os
import pdfplumber
import re
import warnings
import pandas as pd
import hashlib
import time
import random
from sqlite import create_connection, create_table, read_from_table, write_to_table
from dateutil.parser import parse


def format_datetime(input_time, just_date=False, milliseconds=False):
    try:
        if isinstance(input_time, (int, float)):
            input_time = datetime.fromtimestamp(input_time / 1000)
        elif isinstance(input_time, str):
            input_time = parse(input_time)

        if just_date:
            return input_time.strftime("%Y-%m-%d")
        elif milliseconds and not just_date:
            return input_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        else:
            return input_time.strftime("%Y-%m-%d %H:%M:%S")

    except (ValueError, TypeError) as e:
        print(f"Error: {e}")
        return None


def convert_datetime_to_milliseconds(input_time):
    try:
        if isinstance(input_time, (int, float)):
            return str(int(input_time))

        formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]

        for fmt in formats:
            try:
                dt_object = datetime.strptime(input_time, fmt)
                return str(int(dt_object.timestamp() * 1000))
            except ValueError:
                continue

        print("Error: time data does not match any expected format")
        return None

    except (ValueError, TypeError) as e:
        print(f"Error: {e}")
        return None


def create_uuid(conn, name, table_name, datetime_stamp=None):
    while True:
        try:
            part1_len = len(name)
            if part1_len < 10:
                part1_len = 10
            elif part1_len > 99:
                part1_len = 91 + (part1_len - 90) % 9
            part1 = str(part1_len)

            hash_obj = hashlib.md5(name.encode())
            hash_value = int(hash_obj.hexdigest(), 16)
            part2_value = 1000 + (hash_value % 9000)
            if part2_value > 9999:
                part2_value = 9001 + (part2_value - 10000) % 999
            part2 = str(part2_value)

            if datetime_stamp:
                formatted_time = format_datetime(datetime_stamp)
                if formatted_time:
                    part3 = convert_datetime_to_milliseconds(formatted_time)
                else:
                    part3 = str(int(time.time() * 1000))
            else:
                part3 = str(int(time.time() * 1000))

            part4 = str(random.randint(1000, 9999))
            user_id = f"{part1}-{part2}-{part3}-{part4}"

            existing_rows = read_from_table(
                conn, table_name, f"uuid = '{user_id}'")

            if not existing_rows:
                return user_id

        except Exception as e:
            raise Exception(f"Error: {e}")


def clean_string(s):
    cleaned = re.sub(r'[^a-zA-Z0-9\s\-/.]', '', s)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def get_installment_date(text):
    match = re.search(r"(\w{3})\s\d+\s—\s(\w{3})\s\d+,\s(\d{4})", text)
    if match:
        end_month = match.group(2)
        year = match.group(3)
        try:
            dt_object = datetime.strptime(f"{end_month}/01/{year}", '%b/%d/%Y')
            formatted_date = dt_object.strftime('%m/%d/%Y')
            return formatted_date
        except Exception as e:
            print(f"Exception caught during date formatting: {e}")
            return None


def process_apple_card_installments(text, first_day):
    text = text.lower()

    monthly_installment_match = re.search(
        r'this month’s installment:\s+\$([\d.]+)', text)
    total_remaining_match = re.search(
        r'total remaining\s+\$([\d.]+)', text)
    purchase_date_match = re.search(
        r'(\d{2}/\d{2}/\d{4}) apple online store cupertino ca\s+\$([\d.]+)', text)

    monthly_installment = monthly_installment_match.group(
        1) if monthly_installment_match else None
    total_remaining = total_remaining_match.group(
        1) if total_remaining_match else None
    purchase_date = purchase_date_match.group(
        1) if purchase_date_match else None
    purchase_amount = purchase_date_match.group(
        2) if purchase_date_match else None

    transactions = []

    if monthly_installment:
        description = f"Monthly Installment from {purchase_date} purchase of {purchase_amount}"
        transactions.append(('apple_card', first_day,
                            description, monthly_installment))

    if total_remaining:
        description = f"Apple Card Finance from {purchase_date} purchase of {purchase_amount}"
        transactions.append(('apple_card', first_day,
                            description, total_remaining))

    return transactions


def insert_into_transactions(conn, transaction):
    try:
        config_name = transaction[0]

        acct_id = configs[config_name].get('acct_id', 'unknown')
        acct_type = configs[config_name].get('acct_type', 'unknown')

        uuid = create_uuid(
            conn, config_name, 'transactions', str(transaction[1]))

        amount = float(transaction[3])
        if amount > 0:
            debit = amount
            credit = None
        else:
            debit = None
            credit = -amount

        date = format_datetime(transaction[1], just_date=True)

        write_to_table(conn, 'transactions', {
            'uuid': uuid,
            'source': config_name,
            'acctid': acct_id,  # Replace 'x' with acct_id
            'type': acct_type,  # Replace 'x' with acct_type
            'date': date,
            'description': transaction[2],
            'debit': debit,
            'credit': credit
        })

    except Exception as e:
        print(f"An error occurred: {e}.")


def insert_into_error(conn, error_line):
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='error'")

    current_time = format_datetime(time.time(), milliseconds=True)

    if c.fetchone() is None:
        create_table_sql = """CREATE TABLE IF NOT EXISTS error (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                timestamp TEXT NOT NULL,
                                raw_line TEXT NOT NULL
                              );"""
        create_table(conn, create_table_sql)

    write_to_table(conn, 'error', {
        'timestamp': current_time,
        'raw_line': error_line
    })


def process_amex_line(df, conn, config_name):
    filtered_df = df[['Date', 'Description', 'Amount']]
    for _, row in filtered_df.iterrows():
        try:
            cleaned_date = clean_string(str(row['Date']))
            formatted_date = format_datetime(cleaned_date, just_date=True)
            cleaned_description = clean_string(str(row['Description']))
            cleaned_amount = clean_string(str(row['Amount']))

            if formatted_date and cleaned_description and cleaned_amount:
                transaction = (config_name, formatted_date,
                               cleaned_description, cleaned_amount)
                insert_into_transactions(conn, transaction)
        except Exception as e:
            insert_into_error(conn, str(row))
            print(f"Exception caught: {e}")


def process_apple_card_line(text, conn, config_name):
    installment_date = get_installment_date(text)
    if not installment_date:
        return

    in_installment_section = False
    installments = process_apple_card_installments(text, installment_date)

    if len(installments) >= 2:
        installment_transaction = installments[0]
        installment_remaining_balance = installments[1]
        try:
            insert_into_transactions(conn, installment_transaction)
        except Exception as e:
            insert_into_error(conn, str(installment_transaction))
            print(f"Exception caught: {e}")

    for line in text.split('\n'):
        try:
            if "Apple Card Monthly Installments" in line:
                in_installment_section = True

            if in_installment_section:
                continue

            cleaned_line = clean_string(line)
            parts = cleaned_line.split()

            if len(parts) > 2 and re.match(r'\d{2}/\d{2}/\d{4}', parts[0]):
                date = parts[0]
                formatted_date = format_datetime(date, just_date=True)
                amount = parts[-1]
                description = ' '.join(parts[1:-1])
                transaction = (config_name, formatted_date,
                               description, amount)
                insert_into_transactions(conn, transaction)
        except Exception as e:
            insert_into_error(conn, line)
            print(f"Exception caught: {e}")


def process_bluevine_line(df, conn, config_name):
    transactions = []
    if isinstance(df, str):
        lines = df.split("\n")
        start_idx = None
        for i, line in enumerate(lines):
            if "Date Description Amount" in line and start_idx is None:
                start_idx = i + 1
                break

        if start_idx is None:
            start_idx = 0

        for line in lines[start_idx:]:
            line = clean_string(line)
            regex_pattern = r"(\d{2}/\d{2}/\d{2})\s+(.*?)(?:\s+)(-?\d+\.\d{2})$"
            match = re.search(regex_pattern, line)

            if match:
                date, description, amount = match.groups()
                formatted_date = format_datetime(date, just_date=True)
                amount = str(float(amount) * -1)

                transactions.append(
                    {'Date': formatted_date, 'Description': description, 'Amount': amount})

        if transactions:
            processed_df = pd.DataFrame(transactions)
        else:
            return

        filtered_df = processed_df[['Date', 'Description', 'Amount']]

        for _, row in filtered_df.iterrows():
            transaction = (
                config_name, format_datetime(
                    row['Date'], just_date=True), row['Description'], row['Amount']
            )
            insert_into_transactions(conn, transaction)


def process_boa_line(text, conn, config_name):
    for line in text.split('\n'):
        try:
            cleaned_line = clean_string(line)
            parts = cleaned_line.split()

            if len(parts) > 2 and re.match(r'\d{2}/\d{2}/\d{2}', parts[0]):
                date = parts[0]
                formatted_date = format_datetime(date, just_date=True)
                amount = parts[-1]
                amount = str(float(amount) * -1)
                description = ' '.join(parts[1:-1])
                transaction = (config_name, formatted_date,
                               description, amount)
                insert_into_transactions(conn, transaction)
        except Exception as e:
            insert_into_error(conn, line)
            print(f"Exception caught: {e}")


def process_chase_line(reader, conn, config_name):
    df = pd.DataFrame(reader)
    df.columns = df.columns.str.strip()

    try:
        filtered_df = df[['Post Date', 'Description', 'Amount']]
    except KeyError:
        print("KeyError: One or more required columns are missing from the DataFrame.")
        return

    for _, row in filtered_df.iterrows():
        try:
            cleaned_post_date = clean_string(row['Post Date'])
            formatted_date = format_datetime(cleaned_post_date, just_date=True)
            cleaned_description = clean_string(row['Description'])
            cleaned_amount = clean_string(row['Amount'])
            cleaned_amount = str(float(cleaned_amount) * -1)

            if formatted_date and cleaned_description and cleaned_amount:
                transaction = (config_name, formatted_date,
                               cleaned_description, cleaned_amount)
                insert_into_transactions(conn, transaction)
        except Exception as e:
            insert_into_error(conn, str(row))
            print(f"Exception caught: {e}")


def process_files(folder_paths, configs, conn):
    for folder_path in folder_paths:
        folder_name = os.path.basename(folder_path)
        config = configs.get(folder_name)

        if config is None:
            print(f"Skipping unknown folder: {folder_name}")
            continue

        for filename in os.listdir(folder_path):
            if filename.lower().endswith(config['file_extension']):
                file_path = os.path.join(folder_path, filename)

                if config['file_type'] == 'xlsx':
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        df = pd.read_excel(
                            file_path, skiprows=range(0, config['skip_rows']))
                    config['process_line'](df, conn, folder_name)

                elif config['file_type'] == 'pdf':
                    with pdfplumber.open(file_path) as pdf:
                        for page in pdf.pages[config['skip_rows']:]:
                            text = page.extract_text()
                            config['process_line'](text, conn, folder_name)

                elif config['file_type'] == 'csv':
                    with open(file_path, 'r') as f:
                        reader = csv.DictReader(f)
                        config['process_line'](reader, conn, folder_name)


account_type = ['checking', 'credit_card', 'crypto', 'e_wallet',
                'investment', 'line_of_credit', 'loan', 'mortgage',
                'retirement', 'savings']

configs = {
    'amex': {'file_type': 'xlsx', 'file_extension': '.xlsx', 'skip_rows': 6, 'process_line': process_amex_line, 'acct_type': 'credit_card', 'acct_id': '1001'},
    'apple_card': {'file_type': 'pdf', 'file_extension': '.pdf', 'skip_rows': 1, 'process_line': process_apple_card_line, 'acct_type': 'credit_card', 'acct_id': '7088'},
    'bank_of_america': {'file_type': 'pdf', 'file_extension': '.pdf', 'skip_rows': 2, 'process_line': process_boa_line, 'acct_type': 'checking', 'acct_id': '2246'},
    'bluevine': {'file_type': 'pdf', 'file_extension': '.pdf', 'skip_rows': 0, 'process_line': process_bluevine_line, 'acct_type': 'checking', 'acct_id': '2235'},
    'chase': {'file_type': 'csv', 'file_extension': '.csv', 'skip_rows': 0, 'process_line': process_chase_line, 'acct_type': 'credit_card', 'acct_id': '2452'},
}


folder_paths_to_process = ['./amex', './apple_card',
                           './bank_of_america', './bluevine', './chase']

if __name__ == '__main__':
    database = 'financials.db'
    conn = create_connection(database)
    process_files(folder_paths_to_process, configs, conn)
