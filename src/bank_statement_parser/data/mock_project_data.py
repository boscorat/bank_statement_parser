import random
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path


def generate_mock_data(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    cursor.execute("DELETE FROM checks_and_balances")
    cursor.execute("DELETE FROM statement_lines")
    cursor.execute("DELETE FROM batch_lines")
    cursor.execute("DELETE FROM statement_heads")
    cursor.execute("DELETE FROM batch_heads")
    conn.commit()

    random.seed(42)

    companies = ["Acme Corp", "Beta Ltd", "Gamma Inc"]
    account_holders = ["John Smith", "Jane Doe", "Bob Wilson"]
    account_numbers = ["12345678", "87654321", "11223344"]
    sortcodes = ["00-11-22", "33-44-55", "66-77-88"]
    account_types = ["Current", "Business", "Savings"]

    transaction_types = [
        ("Payment", "POS"),
        ("Transfer", "TRF"),
        ("Direct Debit", "DD"),
        ("Standing Order", "SO"),
        ("Cash Withdrawal", "ATM"),
        ("Fee", "FEE"),
        ("Interest", "INT"),
        ("Refund", "REF"),
    ]

    transaction_descs = [
        "Tesco Supermarket",
        "Amazon Marketplace",
        "British Gas Ltd",
        "Sky Television",
        "Salary Payment",
        "Council Tax",
        "Water Board",
        "National Insurance",
        "Bank Transfer",
        "Cash Deposit",
        "ATM Withdrawal",
        "Restaurant Payment",
        "Online Purchase",
        "Utility Bill",
        "Insurance Premium",
    ]

    batch_ids = [str(uuid.uuid4()) for _ in range(3)]
    batch_dates = [
        "2024-01-15 10:00:00",
        "2024-02-15 11:00:00",
        "2024-03-15 12:00:00",
    ]

    batch_heads_data = []
    for i, batch_id in enumerate(batch_ids):
        batch_heads_data.append(
            (
                batch_id,
                f"/path/to/batch_{i + 1}",
                companies[i % len(companies)],
                account_types[i % len(account_types)],
                random.randint(3, 4),
                0,
                random.uniform(10.0, 60.0),
                batch_dates[i],
            )
        )

    cursor.executemany(
        "INSERT INTO batch_heads (ID_BATCH, STD_PATH, STD_COMPANY, STD_ACCOUNT, STD_PDF_COUNT, STD_ERROR_COUNT, STD_DURATION_SECS, STD_UPDATETIME) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        batch_heads_data,
    )
    print(f"Inserted {len(batch_heads_data)} batch_heads")

    statement_ids = [str(uuid.uuid4()) for _ in range(10)]
    statement_dates = [
        "2024-01-31",
        "2024-02-15",
        "2024-02-29",
        "2024-02-29",
        "2024-03-15",
        "2024-03-31",
        "2024-03-31",
        "2024-04-15",
        "2024-04-30",
        "2024-04-30",
    ]

    batch_assignment = [0, 0, 0, 0, 1, 1, 1, 1, 2, 2]

    statement_heads_data = []
    for i, statement_id in enumerate(statement_ids):
        batch_idx = batch_assignment[i]
        company = companies[batch_idx % len(companies)]
        account_holder = account_holders[batch_idx % len(account_holders)]
        account_number = account_numbers[batch_idx % len(account_numbers)]
        sortcode = sortcodes[batch_idx % len(sortcodes)]
        account_type = account_types[batch_idx % len(account_types)]
        id_account = f"{sortcode.replace('-', '')}{account_number}"
        id_batch = batch_ids[batch_idx]

        cursor.execute("SELECT SUM(STD_PAYMENTS_IN), SUM(STD_PAYMENTS_OUT) FROM statement_lines WHERE ID_STATEMENT = ?", (statement_id,))
        result = cursor.fetchone()

        opening_balance = random.uniform(1000, 5000)
        payments_in = random.uniform(2000, 8000)
        payments_out = random.uniform(2000, 8000)
        closing_balance = opening_balance + payments_in - payments_out

        statement_heads_data.append(
            (
                statement_id,
                id_batch,
                id_account,
                company,
                "Bank Statement",
                account_type,
                sortcode,
                account_number,
                account_holder,
                statement_dates[i],
                opening_balance,
                payments_in,
                payments_out,
                closing_balance,
            )
        )

    cursor.executemany(
        "INSERT INTO statement_heads (ID_STATEMENT, ID_BATCH, ID_ACCOUNT, STD_COMPANY, STD_STATEMENT_TYPE, STD_ACCOUNT, STD_SORTCODE, STD_ACCOUNT_NUMBER, STD_ACCOUNT_HOLDER, STD_STATEMENT_DATE, STD_OPENING_BALANCE, STD_PAYMENTS_IN, STD_PAYMENTS_OUT, STD_CLOSING_BALANCE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        statement_heads_data,
    )
    print(f"Inserted {len(statement_heads_data)} statement_heads")

    batch_lines_data = []
    for i, statement_id in enumerate(statement_ids):
        batch_idx = batch_assignment[i]
        batch_lines_data.append(
            (
                batch_ids[batch_idx],
                str(uuid.uuid4()),
                statement_id,
                i + 1,
                f"statement_{i + 1}.pdf",
                account_types[batch_idx % len(account_types)],
                random.uniform(0.5, 5.0),
                batch_dates[batch_idx],
                1,
                None,
                0,
                0,
            )
        )

    cursor.executemany(
        "INSERT INTO batch_lines (ID_BATCH, ID_BATCHLINE, ID_STATEMENT, STD_BATCH_LINE, STD_FILENAME, STD_ACCOUNT, STD_DURATION_SECS, STD_UPDATETIME, STD_SUCCESS, STD_ERROR_MESSAGE, ERROR_CAB, ERROR_CONFIG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch_lines_data,
    )
    print(f"Inserted {len(batch_lines_data)} batch_lines")

    statement_lines_data = []
    for stmt_idx, statement_id in enumerate(statement_ids):
        num_transactions = 30
        current_balance = statement_heads_data[stmt_idx][10]

        for trn_idx in range(num_transactions):
            transaction_id = str(uuid.uuid4())
            page_number = 1
            transaction_date = (datetime.strptime(statement_dates[stmt_idx], "%Y-%m-%d") - timedelta(days=random.randint(1, 28))).strftime(
                "%Y-%m-%d"
            )
            transaction_number = trn_idx + 1

            is_credit = random.random() > 0.5
            cd = "C" if is_credit else "D"
            amount = random.uniform(10, 1000)

            if is_credit:
                payments_in = round(amount, 2)
                payments_out = 0
            else:
                payments_in = 0
                payments_out = round(amount, 2)

            current_balance = current_balance + payments_in - payments_out

            transaction_type, type_cd = random.choice(transaction_types)
            transaction_desc = random.choice(transaction_descs)

            opening_balance = round(current_balance - payments_in + payments_out, 2)
            closing_balance = round(current_balance, 2)

            statement_lines_data.append(
                (
                    transaction_id,
                    statement_id,
                    page_number,
                    transaction_date,
                    transaction_number,
                    cd,
                    transaction_type,
                    type_cd,
                    transaction_desc,
                    opening_balance,
                    payments_in,
                    payments_out,
                    closing_balance,
                )
            )

    cursor.executemany(
        "INSERT INTO statement_lines (ID_TRANSACTION, ID_STATEMENT, STD_PAGE_NUMBER, STD_TRANSACTION_DATE, STD_TRANSACTION_NUMBER, STD_CD, STD_TRANSACTION_TYPE, STD_TRANSACTION_TYPE_CD, STD_TRANSACTION_DESC, STD_OPENING_BALANCE, STD_PAYMENTS_IN, STD_PAYMENTS_OUT, STD_CLOSING_BALANCE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        statement_lines_data,
    )
    print(f"Inserted {len(statement_lines_data)} statement_lines")

    checks_and_balances_data = []
    for i, statement_id in enumerate(statement_ids):
        cab_id = str(uuid.uuid4())
        batch_idx = batch_assignment[i]

        cursor.execute("SELECT SUM(STD_PAYMENTS_IN), SUM(STD_PAYMENTS_OUT) FROM statement_lines WHERE ID_STATEMENT = ?", (statement_id,))
        line_totals = cursor.fetchone()

        checks_and_balances_data.append(
            (
                cab_id,
                statement_id,
                batch_ids[batch_idx],
                1,
                statement_heads_data[i][10],
                statement_heads_data[i][11],
                statement_heads_data[i][12],
                statement_heads_data[i][11] - statement_heads_data[i][12],
                statement_heads_data[i][13],
                0,
                line_totals[0] if line_totals[0] else 0,
                line_totals[1] if line_totals[1] else 0,
                (line_totals[0] if line_totals[0] else 0) - (line_totals[1] if line_totals[1] else 0),
                line_totals[0] + line_totals[1] - statement_heads_data[i][13],
                1 if round(line_totals[0], 2) == round(statement_heads_data[i][11], 2) else 0,
                1 if round(line_totals[1], 2) == round(statement_heads_data[i][12], 2) else 0,
                1
                if round((line_totals[0] if line_totals[0] else 0) - (line_totals[1] if line_totals[1] else 0), 2)
                == round(statement_heads_data[i][11] - statement_heads_data[i][12], 2)
                else 0,
                1 if round(line_totals[0] + line_totals[1] - statement_heads_data[i][13], 2) == 0 else 0,
            )
        )

    cursor.executemany(
        "INSERT INTO checks_and_balances (ID_CAB, ID_STATEMENT, ID_BATCH, HAS_TRANSACTIONS, STD_OPENING_BALANCE_HEADS, STD_PAYMENTS_IN_HEADS, STD_PAYMENTS_OUT_HEADS, STD_MOVEMENT_HEADS, STD_CLOSING_BALANCE_HEADS, STD_OPENING_BALANCE_LINES, STD_PAYMENTS_IN_LINES, STD_PAYMENTS_OUT_LINES, STD_MOVEMENT_LINES, STD_CLOSING_BALANCE_LINES, CHECK_PAYMENTS_IN, CHECK_PAYMENTS_OUT, CHECK_MOVEMENT, CHECK_CLOSING) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        checks_and_balances_data,
    )
    print(f"Inserted {len(checks_and_balances_data)} checks_and_balances")

    conn.commit()
    conn.close()
    print(f"\nMock data inserted successfully into {db_path}")


if __name__ == "__main__":
    generate_mock_data(db_path=Path(__file__).parent.joinpath("project.db"))
