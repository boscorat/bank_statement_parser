import polars as pl

from bank_statement_parser.modules.config import get_config_from_account, get_config_from_company, get_config_from_statement
from bank_statement_parser.modules.functions.pdf_functions import pdf_close, pdf_open
from bank_statement_parser.modules.functions.statement_functions import get_field_values


class Statement:
    def __init__(self, file_path: str, key_company: str | None = None, key_account: str | None = None):
        self.file_path = file_path
        self.key_company = key_company
        self.key_account = key_account
        self.pdf = pdf_open(file_path)
        self.config = self.get_config()
        self.company = self.config.company.company if self.config.company and hasattr(self.config.company, "company") else ""
        self.account = self.config.account if self.config and hasattr(self.config, "account") else ""
        self.statement_type = (
            self.config.statement_type.statement_type
            if self.config.statement_type and hasattr(self.config.statement_type, "statement_type")
            else None
        )
        self.config_header = (
            self.config.statement_type.header.configs
            if self.config.statement_type and hasattr(self.config.statement_type, "header")
            else None
        )
        self.config_pages = (
            self.config.statement_type.pages.configs
            if self.config.statement_type and hasattr(self.config.statement_type, "pages")
            else None
        )
        self.config_lines = (
            self.config.statement_type.lines.configs
            if self.config.statement_type and hasattr(self.config.statement_type, "lines")
            else None
        )
        del self.config
        self.header_results = get_field_values(
            self.config_header, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        )
        self.page_results = get_field_values(
            self.config_pages, pdf=self.pdf, file=self.file_path, company=self.company, account=self.account
        )

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


stmtCC = Statement("/home/boscorat/Downloads/2025-07-12_Statement_Rewards_Credit_Card.pdf")
stmtAdv = Statement("/home/boscorat/Downloads/2025-07-08_Statement_Advance_Account.pdf")
stmtFlex = Statement("/home/boscorat/Downloads/2025-07-08_Statement_Flexible_Saver.pdf")

with pl.Config(tbl_cols=-1, tbl_rows=-1):
    print(f"\n\n{(stmtCC.company + '---' + stmtCC.account).center(80, '=')}")
    print(f"HEADER:\n{stmtCC.header_results.results_field}")
    print(f"PAGES:\n{stmtCC.page_results.results_field}")

    print(f"\n\n{(stmtAdv.company + '---' + stmtAdv.account).center(80, '=')}")
    print(f"HEADER:\n{stmtAdv.header_results.results_field}")
    print(f"PAGES:\n{stmtAdv.page_results.results_field}")

    print(f"\n\n{(stmtFlex.company + '---' + stmtFlex.account).center(80, '=')}")
    print(f"HEADER:\n{stmtFlex.header_results.results_field}")
    print(f"PAGES:\n{stmtFlex.page_results.results_field}")

# stmtCC.close_pdf()
stmtAdv.close_pdf()
stmtFlex.close_pdf()

# del stmtCC
del stmtAdv
del stmtFlex

print()
