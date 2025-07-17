Thanks! The section you shared is already present in the documentation and clearly formatted for GitLab. Here's how it appears with markdown rendering:

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

Let me know if you want:

* A **diagram** showing the relationship between triggers and tables
* A **navigation sidebar** for wiki-style GitLab browsing
* Or **split `.md` files** for each trigger for modular documentation.
