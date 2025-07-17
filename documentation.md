

---

## 📌 1. List of All Triggers

| Trigger Name                                 | Event                                                                | Table                                       |
| -------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------- |
| `TRIG_INS_STG_MDRN_EXT_BNK_ACCT`             | `AFTER INSERT OR UPDATE`                                             | `ods_stg.stage_ach_modern_ext_bank_account` |
| `TRIG_INS_STG_MDRN_MIC_DEP`                  | `AFTER INSERT OR UPDATE OF verification_attempts, locked, md_status` | `ods_stg.stage_modern_micro_deposit`        |
| `TRIG_INS_T_BAV_MD_VERIFICATION_ATTEMPT`     | `AFTER INSERT`                                                       | `ach.t_bav_md_verification_attempt`         |
| `TRIG_INS_UPD_DEL_T_CUSTOMER_BANK_ACCOUNT`   | `AFTER INSERT OR UPDATE OR DELETE`                                   | `account.t_customer_bank_account`           |
| `TRIG_UPD_BAV_BANK_ACCOUNT_VERIFICATION`     | `AFTER UPDATE OF verified_date`                                      | `ach.t_bav_bank_account_verification`       |
| `TRIG_UPD_T_BAV_MD_VERIFICATION_TRANSACTION` | `AFTER UPDATE OF status`                                             | `ach.t_bav_md_verification_transaction`     |

---

## 🗂️ 2. List of All Tables Involved

* `ods_stg.stage_ach_modern_ext_bank_account`
* `ods_stg.stage_modern_micro_deposit`
* `ods_stg.stage_ach_ext_bank_account`
* `account.t_customer_bank_account`
* `account.t_card`
* `account.t_customer_bank_account_sync`
* `ach.t_bav_bank_account`
* `ach.t_bav_bank_account_verification`
* `ach.t_bav_md_verification_attempt`
* `ach.t_bav_md_verification_transaction`
* `ach.t_bav_md_verification_locking`
* `ach.t_bav_user`
* `ach.t_rt_thompson_01`
* `ach.t_bav_md_bank_account_verification`
* `t_credit_account`
* `t_primary_chd`
* `t_name`
* `t_uc`
* `t_uc_audit`
* `ods_stg.legacy_modern_ext_acct_map`
* `dbproc.trigger_log`

---

ON INSERT or UPDATE on MODERN_EXT_BANK_ACCOUNT

  1. Ensure User Exists:
     - If a user with the given LEGACY_CUSTOMER_ID is not found in `t_bav_user`,
       fetch the full customer name from `t_primary_chd`/`t_name` and insert a new record.

  2. Validate or Insert Bank Account:
     - Check if a matching routing/account number exists in `t_bav_bank_account`.
     - If found, update the account type.
     - If not, insert a new bank account.

  3. Get Associated Credit Card:
     - Retrieve the current card (`current_card = 1`) for the credit account.

  4. Check for Existing Customer Bank Account:
     - Look for a matching entry in `t_customer_bank_account`.

  5. If this is a **new insert** (not single-use, not already existing):
     - If `operation_type = 'ADD'`:
       a. Create a new Work Case (`t_uc`) with type depending on verification result.
       b. Insert audit steps into `t_uc_audit` (e.g., method used, status).
       c. Insert record into `t_bav_bank_account_verification` with relevant info.
       d. If verification result = 'ACTIVE':
          - Use context package to set a flag and:
            - Insert customer bank account via `PKG_CUSTOMER_BANK_ACCOUNT`
            - Record the insertion in the bank account history
            - Create a new Work Case for ACH Registration
            - Add audit steps including bank info and registration status
       e. Create mapping in `LEGACY_MODERN_EXT_ACCT_MAP` table.

     - If account is marked `Is_Primary = 1`:
       - Set all existing accounts' `is_primary = 0` for this customer
       - Set the new account's `is_primary = 1`
       - Add entry to `t_customer_bank_account_sync` for change tracking

  6. If this is an **update**:
     - When moving from `PENDING_ACH_VERIFICATION` → `PENDING_ACH_VALIDATION`:
       - Insert the bank account and history if it doesn’t exist
       - Create audit trail

     - When moving from `PENDING_ACH_VALIDATION` → `ACTIVE`:
       - Update the corresponding BAV record with `verified_date` and set result to `Pass`
       - Log audit status: Verification success, platform, closed status

     - If `operation_type` is `PRIMARY_UPDATE` and `Is_Primary = 1`:
       - Similar logic to insert case: unset previous primary, set new primary, sync log


