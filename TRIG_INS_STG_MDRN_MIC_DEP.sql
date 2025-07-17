CREATE OR REPLACE TRIGGER ACH.TRIG_INS_STG_MDRN_MIC_DEP
    AFTER INSERT OR UPDATE OF verification_attempts, locked, md_status
    ON ods_Stg.STAGE_MODERN_MICRO_DEPOSIT
    REFERENCING NEW AS NEW
    FOR EACH ROW
DECLARE
    -- Fetch legacy bank account ID from core bank account table
    CURSOR cs_bank IS
        SELECT ID as legacy_bank_account_id
        FROM ach.t_bav_bank_account 
        WHERE account_number = :NEW.Account_Number
          AND aba_number = :NEW.Routing_Number
          AND account_type = CASE WHEN :NEW.Account_Type = 'CHK' THEN 'Checking' ELSE 'Savings' END;
    bank_rec cs_bank%ROWTYPE;
    
    -- Used for linking attempts and locking to the correct BAV
    v_bank_account_verification_id NUMBER;
    v_atempt_id ach.t_bav_md_verification_attempt.id%TYPE;
    
    -- Get credit_account_id and verified_date from modern bank account staging
    CURSOR cs_stage_mod_ext_bank_account IS
         SELECT credit_account_id, Verified_Date
         FROM ods_Stg.STAGE_ACH_MODERN_EXT_BANK_ACCOUNT
         WHERE modern_bank_account_id = :NEW.Modern_Bank_Account_Id;
    cs_stage_mod_ext_bank_account_rec cs_stage_mod_ext_bank_account%ROWTYPE;
    
    -- Fetch BAV user for legacy customer
    CURSOR cs_bav_user IS
        SELECT ID, individual_name 
        FROM ach.t_bav_user
        WHERE individual_identifier = :NEW.LEGACY_CUSTOMER_ID
          AND identifier_type = 2;
    cs_bav_user_rec cs_bav_user%ROWTYPE;
    
    -- Variables for work case (UC) tracking
    v_uc_id NUMBER;
    v_uc_audit_id NUMBER;
    v_ref_num VARCHAR2(30);
    v_uc_id_existing NUMBER;
    
    -- Last 4 digits for audit trails
    v_account_last_four VARCHAR2(4) := CASE WHEN LENGTH(:NEW.ACCOUNT_NUMBER) <= 4 THEN :NEW.ACCOUNT_NUMBER ELSE SUBSTR(:NEW.ACCOUNT_NUMBER, -4) END;
    
    -- Card lookup by credit_account_id
    CURSOR cs_card(p_credit_account_id cs_stage_mod_ext_bank_account_rec.credit_Account_id%TYPE) IS
        SELECT CARD_ID
        FROM account.t_Card
        WHERE credit_Account_id = p_credit_account_id
          AND current_Card = 1;
    v_card_id NUMBER;
    
    -- Lookup or create verification locking row by BAV ID
    CURSOR cs_locking_id(p_locking_id NUMBER) IS
        SELECT ID  
        FROM ach.T_BAV_MD_VERIFICATION_LOCKING
        WHERE bank_account_verification_id = p_locking_id;
    cs_locking_id_rec cs_locking_id%ROWTYPE;

BEGIN
    -- Generate a reference number for the UC (work case)
    SELECT fn_ref_num_seq() INTO v_ref_num FROM DUAL;
    
    -- Handle INSERT event for micro-deposit workflow
    IF INSERTING THEN
        -- On insert, if status is 'InProgress', initiate BAV, audit, and MD transactions
        IF :NEW.Md_Status = 'InProgress' THEN
            -- Fetch credit account ID and verified date
            OPEN cs_stage_mod_ext_bank_account;
            FETCH cs_stage_mod_ext_bank_account INTO cs_stage_mod_ext_bank_account_rec;
            CLOSE cs_stage_mod_ext_bank_account;

            -- Fetch legacy bank account ID
            OPEN cs_bank;
            FETCH cs_bank INTO bank_rec;
            CLOSE cs_bank;

            -- Fetch BAV user for this legacy customer
            OPEN cs_bav_user;
            FETCH cs_bav_user INTO cs_bav_user_rec;
            CLOSE cs_bav_user;

            -- Get sequence for UC and audit ID
            SELECT fnbm_seq.NEXTVAL  INTO v_uc_id      FROM DUAL; 
            SELECT fnbm_seq.NEXTVAL  INTO v_uc_audit_id FROM DUAL;

            -- Fetch card for this credit account
            OPEN cs_card(cs_stage_mod_ext_bank_account_rec.credit_account_id);
            FETCH cs_card INTO v_card_id;    
            CLOSE cs_card;

            -- Insert work case (UC) for micro deposit verification
            INSERT INTO t_uc (status_id, type_id, ref_num, open_date, open_agent_id, status_date, status_agent_id, uc_id, credit_account_id, card_id)
            VALUES (26, 415470, v_ref_num, :NEW.CREATED_TIMESTAMP, 1, :NEW.UPDATED_TIMESTAMP, 1, v_uc_id, cs_stage_mod_ext_bank_account_rec.CREDIT_ACCOUNT_ID, v_card_id);

            -- Insert UC audit trail for workflow steps
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'WORK CASE CREATED, STATUS OPEN', :NEW.UPDATED_TIMESTAMP, v_uc_id);
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'Verification on account ending in: '|| v_account_last_four, :NEW.UPDATED_TIMESTAMP, v_uc_id);
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'Verification Status Update: In Progress', :NEW.UPDATED_TIMESTAMP, v_uc_id);
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'STATUS CHANGED TO PEND', :NEW.UPDATED_TIMESTAMP, v_uc_id);   

            -- Insert bank account verification header (MicroDeposit)
            INSERT INTO ACH.T_BAV_BANK_ACCOUNT_VERIFICATION(
                verification_type, created_date, verified_date, work_case_id, agent, channel, RESULT, credit_account_id, is_migrated, bank_account_id, user_id)
            VALUES ('MicroDeposit', :NEW.created_timestamp, NULL, v_uc_id, '640', CASE TO_CHAR(:NEW.Channel) WHEN 'WEB' THEN 'Web' Else TO_CHAR(:NEW.Channel) END, NULL, cs_stage_mod_ext_bank_account_rec.credit_account_id, 2, bank_rec.legacy_bank_account_id, cs_bav_user_rec.ID)
            RETURNING bank_account_verification_id INTO v_bank_account_verification_id;

            -- Insert MD BAV-specific audit info row
            INSERT INTO ach.T_BAV_MD_BANK_ACCOUNT_VERIFICATION(
                canceled_date, ach_posted_date, verification_agent, verification_channel, bank_account_verification_id, bank_account_verification_id_old)
            VALUES (NULL, NULL, 1, CASE TO_CHAR(:NEW.Channel) WHEN 'WEB' THEN 'Web' Else TO_CHAR(:NEW.Channel) END, v_bank_account_verification_id, NULL);

            -- Insert MD credit/debit transactions for micro deposit flow
            INSERT INTO ach.T_BAV_MD_VERIFICATION_TRANSACTION(
                created_date, post_date, amount, accounting_transaction_type, status, status_update_date, ach_transaction_id, bank_account_verification_id, bank_account_verification_id_old)
            VALUES (:NEW.created_timestamp, :NEW.updated_timestamp, :NEW.mdamount1, 'Credit', 'Received', :NEW.updated_timestamp, NULL, v_bank_account_verification_id, NULL);
            INSERT INTO ach.T_BAV_MD_VERIFICATION_TRANSACTION(
                created_date, post_date, amount, accounting_transaction_type, status, status_update_date, ach_transaction_id, bank_account_verification_id, bank_account_verification_id_old)
            VALUES (:NEW.created_timestamp, :NEW.updated_timestamp, :NEW.mdamount2, 'Credit', 'Received', :NEW.updated_timestamp, NULL, v_bank_account_verification_id, NULL);                                                           
            INSERT INTO ach.T_BAV_MD_VERIFICATION_TRANSACTION(
                created_date, post_date, amount, accounting_transaction_type, status, status_update_date, ach_transaction_id, bank_account_verification_id, bank_account_verification_id_old)
            VALUES (:NEW.created_timestamp, :NEW.updated_timestamp, :NEW.mdamount1+:NEW.MDAMOUNT2, 'Debit', 'Received', :NEW.updated_timestamp, NULL, v_bank_account_verification_id, NULL);

            -- Map modern MD ID to legacy MD BAV ID
            UPDATE ods_stg.LEGACY_MODERN_EXT_ACCT_MAP
            SET MODERN_MICRO_DEPOSIT_ID = :NEW.md_id,
                LEGACY_MICRO_DEPOSIT_ID = v_bank_account_verification_id
            WHERE MODERN_BANK_ACCOUNT_ID = :NEW.Modern_Bank_Account_Id;                                                                                                                         
        END IF;
    END IF;    

    -- Handle UPDATE events for micro-deposit workflow
    IF UPDATING THEN  
        -- Fetch bank and modern staging info for update actions
        OPEN cs_bank;
        FETCH cs_bank INTO bank_rec;
        CLOSE cs_bank;
        
        OPEN cs_stage_mod_ext_bank_account;
        FETCH cs_stage_mod_ext_bank_account INTO cs_stage_mod_ext_bank_account_rec;
        CLOSE cs_stage_mod_ext_bank_account;

        -- Get the current BAV (MicroDeposit, migrated) record for this account and credit account
        BEGIN
            SELECT bank_account_verification_id, work_case_id INTO v_bank_account_verification_id, v_uc_id_existing
            FROM ach.T_BAV_BANK_ACCOUNT_VERIFICATION 
            WHERE bank_account_id = bank_rec.legacy_bank_account_id
              AND credit_account_id = cs_stage_mod_ext_bank_account_rec.credit_account_id
              AND is_migrated = 2
              AND verification_type = 'MicroDeposit';
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_bank_account_verification_id := NULL; 
                v_uc_id_existing := NULL; 
                -- Log if unable to find verification row
                INSERT INTO dbproc.trigger_log(trigger_name, table_name, table_id, source_system, destination_system, status, failure_reason, failure_count, created_by, created_timestamp, updated_by, updated_timestamp, domain, trigger_input)
                VALUES (
                    'TRIG_INS_STG_MDRN_MIC_DEP',
                    'ODS_STG.STAGE_MODERN_MICRO_DEPOSIT',
                    'STAGE_SEQUENCE_ID:' || TO_CHAR(:NEW.STAGE_SEQUENCE_ID),
                    'MODERN',
                    'LEGACY',
                    'FAILED',
                    'Bank Account Verification Id is Null',
                    1,
                    'TRIGGER',
                    SYSTIMESTAMP,
                    '', '', 'EXTERNAL_ACCOUNTS', ''
                ); 
                RETURN;
        END;

        -- When status moves from 'InProgress' to 'Processed', mark MD as processed
        IF :OLD.Md_Status = 'InProgress' AND :NEW.md_status = 'Processed' THEN
            -- Mark MD as posted/processed in all relevant tables
            UPDATE ach.T_BAV_MD_BANK_ACCOUNT_VERIFICATION
            SET ach_posted_date = :NEW.updated_timestamp
            WHERE bank_account_verification_id = v_bank_account_verification_id;
            
            UPDATE ach.T_BAV_MD_VERIFICATION_TRANSACTION
            SET post_date = :NEW.updated_timestamp,
                status = 'Processed'
            WHERE bank_account_verification_id = v_bank_account_verification_id;
        END IF;

        -- If status is changed to Cancel, update cancel date
        IF :OLD.Md_Status <> :NEW.md_status AND :NEW.md_status = 'Cancel' THEN
            UPDATE ach.T_BAV_MD_BANK_ACCOUNT_VERIFICATION
            SET canceled_date = :NEW.updated_timestamp
            WHERE bank_account_verification_id = v_bank_account_verification_id;
        END IF;

        -- If number of attempts changed, log and create locking/attempt rows
        IF :OLD.Verification_Attempts != :NEW.verification_attempts AND :NEW.verification_attempts > 0 THEN
            -- Lookup (or create) verification locking row
            OPEN cs_locking_id(v_bank_account_verification_id);
            FETCH cs_locking_id INTO cs_locking_id_rec;
            CLOSE cs_locking_id;
            IF cs_locking_id_rec.id IS NULL THEN
                INSERT INTO ach.T_BAV_MD_VERIFICATION_LOCKING(open_date, bank_account_verification_id)
                VALUES (:NEW.updated_timestamp, v_bank_account_verification_id)
                RETURNING id INTO cs_locking_id_rec.id;
            END IF;
            -- Create attempt row for this MD
            INSERT INTO ach.T_BAV_MD_VERIFICATION_ATTEMPT(verification_locking_id, created_date, agent, channel)
            VALUES (cs_locking_id_rec.id, :NEW.updated_timestamp, '640', 'CASH')
            RETURNING id INTO v_atempt_id;
            -- Set channel to Web for this attempt (for reporting)
            UPDATE ach.T_BAV_MD_VERIFICATION_ATTEMPT SET channel = 'Web' WHERE id = v_atempt_id;

            -- Audit failed verification (for retry scenario)
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'Verification Status Update: Failed', :NEW.UPDATED_TIMESTAMP, v_uc_id_existing);
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'Verification Platform: UCRM', :NEW.UPDATED_TIMESTAMP, v_uc_id_existing);
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'Status changed to PENDING', :NEW.UPDATED_TIMESTAMP, v_uc_id_existing);
        END IF;

        -- If 'locked' flag changes to '1', log error closure
        IF :OLD.Locked != :NEW.locked AND :NEW.locked = '1' THEN
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'Verification Status Update: ERROR', :NEW.UPDATED_TIMESTAMP, v_uc_id_existing);
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1, 'STATUS CHANGED TO ERRCLOSED', :NEW.UPDATED_TIMESTAMP, v_uc_id_existing);
        END IF;
    END IF;

EXCEPTION
    -- Any error: log for traceability
    WHEN OTHERS THEN
        INSERT INTO dbproc.trigger_log(trigger_name, table_name, table_id, source_system, destination_system, status, failure_reason, failure_count, created_by, created_timestamp, updated_by, updated_timestamp, domain, trigger_input)
        VALUES (
            'TRIG_INS_STG_MDRN_MIC_DEP',
            'ODS_STG.STAGE_MODERN_MICRO_DEPOSIT',
            'STAGE_SEQUENCE_ID:' || TO_CHAR(:NEW.STAGE_SEQUENCE_ID),
            'MODERN',
            'LEGACY',
            'FAILED',
            DBMS_UTILITY.format_error_stack || DBMS_UTILITY.format_error_backtrace,
            1,
            'TRIGGER',
            SYSTIMESTAMP,
            '', '', 'EXTERNAL_ACCOUNTS', ''
        ); 
END TRIG_INS_STG_MDRN_MIC_DEP;
