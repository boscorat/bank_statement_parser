from bank_statement_parser.modules.config import get_config_from_account, get_config_from_company, get_config_from_statement
from bank_statement_parser.modules.functions.pdf_functions import pdf_close, pdf_open


class Statement:
    def __init__(self, file_path: str, key_company: str | None = None, key_account: str | None = None):
        self.file_path = file_path
        self.key_company = key_company
        self.key_account = key_account
        self.pdf = pdf_open(file_path)
        self.config = self.get_config()

    def get_config(self):
        config = None
        if self.key_account:
            config = get_config_from_account(self.key_account, self.file_path)
        elif self.key_company:
            config = get_config_from_company(self.key_company, self.pdf, self.file_path)
        else:
            config = get_config_from_statement(self.pdf, self.file_path)
        return config

    def close_pdf(self):
        pdf_close(self.pdf)
        self.pdf = None


stmt = Statement("/home/boscorat/Downloads/2025-07-12_Statement_Rewards_Credit_Card.pdf")

print()

stmt.close_pdf()

del stmt

print()
