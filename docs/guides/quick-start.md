# Quick Start

This guide walks you through installing Bank Statement Parser and running it on a folder of PDF bank statements for the first time. No technical experience is required.

The whole process takes about five minutes.

---

## Before you begin

You will need:

- PDF statements downloaded from your bank's online portal
- A folder to keep them in (e.g. `Documents/statements`)
- An internet connection for the one-time installation

!!! note "Supported statements"
    Bank Statement Parser currently supports statements from **HSBC UK** (current accounts, savings accounts, and Rewards Credit Card) and **TSB UK** (Spend & Save current account). See the [home page](../index.md#supported-banks-and-accounts) for the full list.

---

## Step-by-step installation and first run

=== "Windows"

    ### Step 1 — Install uv

    `uv` is a fast, lightweight tool that manages Python and Python packages for you. You do not need to install Python separately.

    1. Open **PowerShell** — press `Win + S`, type `PowerShell`, and press Enter
    2. Copy and paste the following command, then press Enter:

        ```powershell
        powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
        ```

    3. When it finishes, **close PowerShell and open a new window** — this ensures the new commands are available

    !!! tip "Checking it worked"
        In the new PowerShell window, type `uv --version` and press Enter. You should see a version number printed, such as `uv 0.5.0`.

    ---

    ### Step 2 — Install Bank Statement Parser

    In your PowerShell window, run:

    ```powershell
    uv tool install uk-bank-statement-parser
    ```

    This downloads and installs the tool. It only needs to be done once.

    !!! tip "Checking it worked"
        Run `bsp --help` — you should see a list of available commands.

    ---

    ### Step 3 — Put your PDF statements in a folder

    Create a folder somewhere easy to find, for example:

    ```
    C:\Users\YourName\Documents\statements
    ```

    Copy all of your PDF bank statements into that folder. They can be from different accounts and different months — Bank Statement Parser will sort them out.

    ---

    ### Step 4 — Run Bank Statement Parser

    In PowerShell, run the following command, replacing the path with the location of your statements folder:

    ```powershell
    bsp process --pdfs "C:\Users\YourName\Documents\statements"
    ```

    You will see progress messages as each statement is processed. When it finishes, a new folder called `bsp_project` will have been created inside your statements folder.

    ---

    ### Step 5 — Find your output files

    Open `C:\Users\YourName\Documents\statements\bsp_project\` in File Explorer.

    Your exported files are in the **`export`** sub-folder:

    ```
    bsp_project\
    └── export\
        ├── transactions.csv        ← all transactions, one row per line
        ├── transactions.xlsx       ← same data as an Excel workbook
        └── balances.csv            ← opening and closing balances per statement
    ```

    Open `transactions.xlsx` in Excel to see all of your transactions in one place.

    !!! tip "Running again later"
        When you receive new statements, just copy them into the same folder and run the same `bsp process` command again. Existing records will not be duplicated.

=== "macOS"

    ### Step 1 — Install uv

    `uv` is a fast, lightweight tool that manages Python and Python packages for you. You do not need to install Python separately.

    1. Open **Terminal** — press `Cmd + Space`, type `Terminal`, and press Enter
    2. Copy and paste the following command, then press Enter:

        ```bash
        curl -LsSf https://astral.sh/uv/install.sh | sh
        ```

    3. When it finishes, run the following to make the new commands available in your current window:

        ```bash
        source $HOME/.local/bin/env
        ```

    !!! tip "Checking it worked"
        Type `uv --version` and press Enter. You should see a version number printed, such as `uv 0.5.0`.

    ---

    ### Step 2 — Install Bank Statement Parser

    In Terminal, run:

    ```bash
    uv tool install uk-bank-statement-parser
    ```

    This downloads and installs the tool. It only needs to be done once.

    !!! tip "Checking it worked"
        Run `bsp --help` — you should see a list of available commands.

    ---

    ### Step 3 — Put your PDF statements in a folder

    Create a folder somewhere easy to find, for example:

    ```
    /Users/yourname/Documents/statements
    ```

    In Finder you can do this by going to **Documents** and pressing `Shift + Cmd + N` to create a new folder called `statements`.

    Copy all of your PDF bank statements into that folder. They can be from different accounts and different months — Bank Statement Parser will sort them out.

    ---

    ### Step 4 — Run Bank Statement Parser

    In Terminal, run the following command, replacing the path with the location of your statements folder:

    ```bash
    bsp process --pdfs ~/Documents/statements
    ```

    You will see progress messages as each statement is processed. When it finishes, a new folder called `bsp_project` will have been created inside your statements folder.

    ---

    ### Step 5 — Find your output files

    Open Finder and navigate to:

    ```
    ~/Documents/statements/bsp_project/export/
    ```

    Your exported files are:

    ```
    bsp_project/
    └── export/
        ├── transactions.csv        ← all transactions, one row per line
        ├── transactions.xlsx       ← same data as an Excel workbook
        └── balances.csv            ← opening and closing balances per statement
    ```

    Open `transactions.xlsx` in Numbers or Excel to see all of your transactions in one place.

    !!! tip "Running again later"
        When you receive new statements, just copy them into the same folder and run the same `bsp process` command again. Existing records will not be duplicated.

=== "Linux"

    ### Step 1 — Install uv

    `uv` is a fast, lightweight tool that manages Python and Python packages for you. You do not need to install Python separately.

    1. Open a **terminal**
    2. Copy and paste the following command, then press Enter:

        ```bash
        curl -LsSf https://astral.sh/uv/install.sh | sh
        ```

    3. When it finishes, run the following to make the new commands available in your current session:

        ```bash
        source $HOME/.local/bin/env
        ```

    !!! tip "Checking it worked"
        Type `uv --version` and press Enter. You should see a version number printed, such as `uv 0.5.0`.

    ---

    ### Step 2 — Install Bank Statement Parser

    In your terminal, run:

    ```bash
    uv tool install uk-bank-statement-parser
    ```

    This downloads and installs the tool. It only needs to be done once.

    !!! tip "Checking it worked"
        Run `bsp --help` — you should see a list of available commands.

    ---

    ### Step 3 — Put your PDF statements in a folder

    Create a folder somewhere easy to find, for example:

    ```bash
    mkdir -p ~/Documents/statements
    ```

    Copy all of your PDF bank statements into that folder. They can be from different accounts and different months — Bank Statement Parser will sort them out.

    ---

    ### Step 4 — Run Bank Statement Parser

    Run the following command, replacing the path with the location of your statements folder:

    ```bash
    bsp process --pdfs ~/Documents/statements
    ```

    You will see progress messages as each statement is processed. When it finishes, a new folder called `bsp_project` will have been created inside your statements folder.

    ---

    ### Step 5 — Find your output files

    Navigate to the output folder:

    ```bash
    cd ~/Documents/statements/bsp_project/export/
    ls
    ```

    Your exported files are:

    ```
    bsp_project/
    └── export/
        ├── transactions.csv        ← all transactions, one row per line
        ├── transactions.xlsx       ← same data as an Excel workbook
        └── balances.csv            ← opening and closing balances per statement
    ```

    Open `transactions.xlsx` in LibreOffice Calc or any spreadsheet application to see all of your transactions in one place.

    !!! tip "Running again later"
        When you receive new statements, just copy them into the same folder and run the same `bsp process` command again. Existing records will not be duplicated.

---

## Something went wrong?

| Problem | What to try |
|---|---|
| `bsp: command not found` | Close your terminal/PowerShell window and open a fresh one, then try again |
| A statement was not processed | Check it is from a [supported bank](../index.md#supported-banks-and-accounts) and that the PDF is not password-protected |
| The export folder is empty | Check the terminal output for any error messages and raise an issue on [GitHub](https://github.com/boscorat/bank_statement_parser/issues) |

---

## What's next?

- **[Export Options](exports.md)** — learn about CSV, Excel, and QuickBooks export formats
- **[Anonymisation](anonymisation.md)** — redact personal details from PDFs before sharing them
- **[Project Structure](project-structure.md)** — understand how your output files are organised
