-- QuickBooks Online UK — 3-column bank upload format
--
-- Returns one row per transaction for a given account, with optional date
-- and statement filters.  id_statement is included so the Python layer can
-- partition by statement when split_by_statement = true; it is dropped from
-- the final output by _apply_column_mapping.
--
-- Named parameters (all required; pass NULL for optional ones):
--   :account_key   — DimAccount.id_account value to filter by (required)
--   :date_from     — earliest transaction_date to include, ISO-8601 (NULL = no lower bound)
--   :date_to       — latest  transaction_date to include, ISO-8601 (NULL = no upper bound)
--   :statement_key — id_statement to restrict to a single statement  (NULL = all statements)

SELECT
    ft.id_date            AS transaction_date,
    ds.statement_date,
    ds.filename,
    da.company,
    da.account_type,
    da.account_number,
    da.sortcode,
    da.account_holder,
    ft.transaction_number,
    ft.transaction_credit_or_debit AS CD,
    ft.transaction_type            AS type,
    ft.transaction_desc,
    SUBSTR(ft.transaction_desc, 1, 25) AS short_desc,
    ft.value_in,
    ft.value_out,
    ft.value,
    ft.id_statement
FROM FactTransaction ft
INNER JOIN DimStatement ds ON ft.statement_id = ds.statement_id
INNER JOIN DimAccount   da ON ft.account_id   = da.account_id
WHERE da.id_account = :account_key
  AND (:date_from     IS NULL OR ft.id_date      >= :date_from)
  AND (:date_to       IS NULL OR ft.id_date      <= :date_to)
  AND (:statement_key IS NULL OR ft.id_statement  = :statement_key)
