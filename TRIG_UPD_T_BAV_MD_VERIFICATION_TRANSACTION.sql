CREATE OR REPLACE TRIGGER ODS_STG.TRIG_UPD_T_BAV_MD_VERIFICATION_TRANSACTION
  AFTER UPDATE OF status 
  ON ach.t_bav_md_verification_transaction
  REFERENCING NEW AS NEW
  FOR EACH ROW  
  
DECLARE
  -- Cursor to fetch credit account, bank account, and migration status for the current BAV
  CURSOR cs_bav_verification IS
         SELECT credit_account_id, bank_account_id, is_migrated
         FROM ach.t_bav_bank_account_verification
         WHERE bank_account_verification_id = :NEW.Bank_Account_Verification_Id;
  
  cs_bav_verification_rec cs_bav_verification%ROWTYPE;
  
  -- Cursor to get customer ID for a given credit account
  CURSOR cs_customer(p_credit_account_id t_credit_account.credit_account_id%TYPE) IS
         SELECT customer_id
         FROM t_credit_account
         WHERE credit_account_id = p_credit_account_id;
         
  cs_customer_rec cs_customer%ROWTYPE;
  
BEGIN
  -- Check if this is an UPDATE, status has changed, and this is a Debit transaction
  IF UPDATING AND :OLD.Status != :NEW.Status AND :NEW.Accounting_Transaction_Type = 'Debit'
    THEN 
  
        -- Fetch BAV verification context (credit account, bank account, migration flag)
        OPEN cs_bav_verification;
        FETCH cs_bav_verification INTO cs_bav_verification_rec;
        CLOSE cs_bav_verification;
        
        -- Fetch customer ID for the relevant credit account
        OPEN cs_customer(cs_bav_verification_rec.credit_account_id);
        FETCH cs_customer INTO cs_customer_rec;
        CLOSE cs_customer;
  
        -- If this is NOT a migrated record (legacy), update all MD statuses in the legacy staging table
        IF cs_bav_verification_rec.Is_Migrated != 2 
           THEN 
             UPDATE ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT
             SET md1_status = :NEW.Status,
                 md2_status = :NEW.Status,
                 md3_status = :NEW.Status
             WHERE legacy_customer_id = cs_customer_rec.customer_id
               AND account_id = cs_bav_verification_rec.Bank_Account_Id
               AND credit_account_id = cs_bav_verification_rec.Credit_Account_Id;
        END IF;
  END IF;        

EXCEPTION
    -- On error, log to trigger_log table for traceability
    WHEN OTHERS THEN
        INSERT INTO dbproc.trigger_log(
            trigger_name,
            table_name,
            table_id,
            source_system,
            destination_system,
            status,
            failure_reason,
            failure_count,
            created_by,
            created_timestamp,
            updated_by,
            updated_timestamp,
            domain,
            trigger_input
        )
        VALUES (
            'TRIG_UPD_T_BAV_MD_VERIFICATION_TRANSACTION',
            'T_BAV_MD_VERIFICATION_TRANSACTION',
            'Bank_Account_Verification_Id:' || TO_CHAR(:NEW.Bank_Account_Verification_Id),
            'LEGACY',
            'MODERN',
            'FAILED',
            DBMS_UTILITY.format_error_stack || DBMS_UTILITY.format_error_backtrace,
            0,
            'TRIGGER',
            SYSTIMESTAMP,
            '', 
            '', 
            'EXTERNAL_ACCOUNTS',
            ''
        ); 
       -- Optionally, you can raise an application error to propagate to the caller:
       -- RAISE_APPLICATION_ERROR(-20001, DBMS_UTILITY.format_error_stack || DBMS_UTILITY.format_error_backtrace);                                
END;
