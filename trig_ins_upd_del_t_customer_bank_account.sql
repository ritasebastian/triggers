CREATE OR REPLACE TRIGGER ODS_STG.trig_ins_upd_del_t_customer_bank_account
AFTER INSERT OR UPDATE OR DELETE
ON account.t_customer_bank_account
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
  ---------------------------------------------------------------------------
  -- Cursors to fetch ACH verification data for this bank account (for both DML and DELETE)
  ---------------------------------------------------------------------------
  CURSOR cs_bav_bank_verification IS
    SELECT bav.bank_account_verification_id, bav.verification_type, bav.verified_date,
           bav.result, bav.channel, bav.agent, bav.created_date, bav.credit_account_id,
           ba.id AS bank_account_id, bav.is_migrated
    FROM ach.t_bav_bank_account_verification bav
    JOIN ach.t_bav_bank_account ba ON ba.id = bav.bank_account_id
    WHERE bav.credit_account_id IN (
            SELECT credit_account_id FROM t_credit_account WHERE customer_id = :NEW.customer_id)
      AND ba.aba_number = :NEW.routing_number
      AND ba.account_number = :NEW.account_number
      AND TRUNC(bav.created_date) = TRUNC(:NEW.created_date)
      AND ((bav.result = 'Pass') OR (bav.verification_type = 'MicroDeposit' AND bav.result IS NULL))
    ORDER BY bav.created_date DESC
    FETCH FIRST 1 ROWS ONLY;

  CURSOR cs_bav_bank_verification_del IS
    SELECT bav.bank_account_verification_id, bav.verification_type, bav.verified_date,
           bav.result, bav.channel, bav.agent, bav.created_date, bav.credit_account_id,
           ba.id AS bank_account_id, bav.is_migrated
    FROM ach.t_bav_bank_account_verification bav
    JOIN ach.t_bav_bank_account ba ON ba.id = bav.bank_account_id
    WHERE bav.credit_account_id IN (
            SELECT credit_account_id FROM t_credit_account WHERE customer_id = :OLD.customer_id)
      AND ba.aba_number = :OLD.routing_number
      AND ba.account_number = :OLD.account_number
      AND TRUNC(bav.created_date) = TRUNC(:OLD.created_date)
      AND ((bav.result = 'Pass') OR (bav.verification_type = 'MicroDeposit' AND bav.result IS NULL))
    ORDER BY bav.created_date DESC
    FETCH FIRST 1 ROWS ONLY;

  CURSOR cs_customer(p_credit_account_id NUMBER) IS
    SELECT customer_id FROM t_credit_account WHERE credit_account_id = p_credit_account_id;

  ---------------------------------------------------------------------------
  -- MicroDeposit verification details for the account: amounts, statuses, timestamps, locking, attempts
  ---------------------------------------------------------------------------
  CURSOR cs_md(p_bav_id NUMBER) IS
    SELECT * FROM (
      SELECT bank_account_verification_id, amount,
             ROW_NUMBER() OVER (PARTITION BY bank_account_verification_id ORDER BY created_date) rnum
      FROM ach.T_BAV_MD_VERIFICATION_TRANSACTION
      WHERE bank_account_verification_id = p_bav_id
    ) PIVOT (SUM(amount) FOR rnum IN (1 AS mdamount1, 2 AS mdamount2, 3 AS mdamount3));

  CURSOR cs_md_status(p_bav_id NUMBER) IS
    SELECT * FROM (
      SELECT bank_account_verification_id, status,
             ROW_NUMBER() OVER (PARTITION BY bank_account_verification_id ORDER BY created_date) rnum
      FROM ach.T_BAV_MD_VERIFICATION_TRANSACTION
      WHERE bank_account_verification_id = p_bav_id
    ) PIVOT (MAX(status) FOR rnum IN (1 AS md1_status, 2 AS md2_status, 3 AS md3_status));

  CURSOR cs_md_ts(p_bav_id NUMBER) IS
    SELECT * FROM (
      SELECT bank_account_verification_id, created_date,
             ROW_NUMBER() OVER (PARTITION BY bank_account_verification_id ORDER BY created_date) rnum
      FROM ach.T_BAV_MD_VERIFICATION_TRANSACTION
      WHERE bank_account_verification_id = p_bav_id
    ) PIVOT (MAX(created_date) FOR rnum IN (1 AS md1_created_timestamp, 2 AS md2_created_timestamp, 3 AS md3_created_timestamp));

  CURSOR cs_md_locking(p_bav_id NUMBER) IS
    SELECT id FROM ach.T_BAV_MD_VERIFICATION_LOCKING WHERE bank_account_verification_id = p_bav_id;

  CURSOR cs_md_attempt(p_locking_id NUMBER) IS
    SELECT COUNT(*) AS attempts
    FROM ach.T_BAV_MD_VERIFICATION_ATTEMPT
    WHERE verification_locking_id = p_locking_id
    GROUP BY verification_locking_id;

  ---------------------------------------------------------------------------
  -- Record variables for fetched data and transformation
  ---------------------------------------------------------------------------
  cs_customer_rec cs_customer%ROWTYPE;
  cs_md_rec cs_md%ROWTYPE;
  cs_md_status_rec cs_md_status%ROWTYPE;
  cs_md_ts_rec cs_md_ts%ROWTYPE;
  cs_md_locking_rec cs_md_locking%ROWTYPE;
  cs_md_attempt_rec cs_md_attempt%ROWTYPE;

  -- Verification attributes
  v_bank_account_verification_id ach.t_bav_bank_account_verification.bank_account_verification_id%TYPE;
  v_verification_type ach.t_bav_bank_account_verification.verification_type%TYPE;
  v_verified_date ach.t_bav_bank_account_verification.verified_date%TYPE;
  v_result ach.t_bav_bank_account_verification.result%TYPE;
  v_channel ach.t_bav_bank_account_verification.channel%TYPE;
  v_agent ach.t_bav_bank_account_verification.agent%TYPE;
  v_created_date ach.t_bav_bank_account_verification.created_date%TYPE;
  v_credit_account_id ach.t_bav_bank_account_verification.credit_account_id%TYPE;
  v_bank_account_id ach.t_bav_bank_account.ID%TYPE;
  v_is_migrated ach.t_bav_bank_account_verification.is_migrated%TYPE;

  -- Output/transform fields for staging
  v_result_new ach.t_bav_bank_account_verification.result%TYPE;
  v_verification_type_new ach.t_bav_bank_account_verification.verification_type%TYPE;
  v_verified_date_new ach.t_bav_bank_account_verification.verified_date%TYPE;
  v_channel_new ach.t_bav_bank_account_verification.channel%TYPE;
  v_agent_new ach.t_bav_bank_account_verification.agent%TYPE;
  v_created_date_new ach.t_bav_bank_account_verification.created_date%TYPE;
  v_credit_account_id_new ach.t_bav_bank_account_verification.credit_account_id%TYPE;
  v_bank_account_id_new ach.t_bav_bank_account.ID%TYPE;

  -- Timestamp for auditing changes in PST
  v_Change_timestamp_pst ods_stg.stage_ach_ext_bank_account.change_timestamp%type;

  -- Used for flagging MOD-originated changes
  v_Is_Change_From_MOD varchar2(10):=CASE WHEN ods_stg.trig_ctx_pkg.is_from_trig_MBA THEN 'True' Else 'False' END;

begin
  ---------------------------------------------------------------------------
  -- Fetch verification details for INSERT/UPDATE
  ---------------------------------------------------------------------------
  OPEN cs_bav_bank_verification;
  SELECT SYSTIMESTAMP AT TIME ZONE 'PST' INTO v_Change_timestamp_pst FROM dual;
  FETCH cs_bav_bank_verification INTO 
    v_bank_account_verification_id,
    v_verification_type,
    v_verified_date,
    v_result,
    v_channel,
    v_agent,
    v_created_date,
    v_credit_account_id,
    v_bank_account_id,
    v_is_migrated;

  -- Determine the output staging result & type based on rules
  IF (v_verification_type in ('Initial', 'Ews') AND v_result = 'Pass') THEN
    v_result_new := 'ACTIVE';
    v_verification_type_new := 'EWS';
    v_verified_date_new := v_verified_date;
  ELSIF (v_verification_type = 'MicroDeposit' AND NVL(v_result, 'X') = 'Pass') THEN
    v_result_new := 'ACTIVE';
    v_verification_type_new := 'MICRODEPOSIT';
    v_verified_date_new := v_verified_date;
  ELSIF (v_verification_type = 'MicroDeposit') THEN
    v_result_new := 'PENDING_ACH_VALIDATION';
    v_verification_type_new := 'MICRODEPOSIT';
    v_verified_date_new := v_verified_date;
  ELSIF (v_verification_type = 'Yodlee' AND v_result = 'Pass') THEN
    v_result_new := 'ACTIVE';
    v_verification_type_new := 'YODLEE';
    v_verified_date_new := v_verified_date;
  ELSIF (v_verification_type = 'Yodlee') THEN
    v_result_new := 'PENDING_ACH_VALIDATION';
    v_verification_type_new := 'YODLEE';
    v_verified_date_new := v_verified_date;
  END IF;
  CLOSE cs_bav_bank_verification;

  -- Assign all *new fields (for easier use in DML below)
  v_channel_new := v_channel;
  v_agent_new := v_agent;
  v_created_date_new := v_created_date;
  v_credit_account_id_new := v_credit_account_id;
  v_bank_account_id_new := v_bank_account_id;

  ---------------------------------------------------------------------------
  -- Fetch extra data (customer, micro-deposit) if verification found
  ---------------------------------------------------------------------------
  IF v_bank_account_verification_id IS NOT NULL THEN
    OPEN cs_customer(v_credit_account_id_new); FETCH cs_customer INTO cs_customer_rec; CLOSE cs_customer;
    OPEN cs_md(v_bank_account_verification_id); FETCH cs_md INTO cs_md_rec; CLOSE cs_md;
    OPEN cs_md_status(v_bank_account_verification_id); FETCH cs_md_status INTO cs_md_status_rec; CLOSE cs_md_status;
    OPEN cs_md_ts(v_bank_account_verification_id); FETCH cs_md_ts INTO cs_md_ts_rec; CLOSE cs_md_ts;
    OPEN cs_md_locking(v_bank_account_verification_id); FETCH cs_md_locking INTO cs_md_locking_rec; CLOSE cs_md_locking;
    OPEN cs_md_attempt(cs_md_locking_rec.id); FETCH cs_md_attempt INTO cs_md_attempt_rec; CLOSE cs_md_attempt;
  END IF;

  ---------------------------------------------------------------------------
  -- INSERT branch: Insert/merge into staging table for non-migrated records
  ---------------------------------------------------------------------------
  IF INSERTING AND v_is_migrated != 2 THEN
    MERGE INTO ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT tgt
    USING DUAL
    ON (
      tgt.legacy_customer_id = cs_customer_rec.customer_id AND
      tgt.account_id         = v_bank_account_id_new AND
      tgt.credit_account_id  = v_credit_account_id_new AND
      tgt.routing_number     = :NEW.routing_number AND
      tgt.account_number     = :NEW.account_number AND
      tgt.account_type       = CASE :NEW.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END
    )
    WHEN MATCHED THEN
      UPDATE SET
        tgt.result                = v_result_new,
        tgt.agent                 = v_agent_new,
        tgt.channel               = v_channel_new,
        tgt.verification_type     = v_verification_type_new,
        tgt.verified_date         = v_verified_date_new,
        tgt.mdamount1             = cs_md_rec.mdamount1,
        tgt.mdamount2             = cs_md_rec.mdamount2,
        tgt.mdamount3             = cs_md_rec.mdamount3,
        tgt.md1_status            = cs_md_status_rec.md1_status,
        tgt.md2_status            = cs_md_status_rec.md2_status,
        tgt.md3_status            = cs_md_status_rec.md3_status,
        tgt.md1_created_timestamp = cs_md_ts_rec.md1_created_timestamp,
        tgt.md2_created_timestamp = cs_md_ts_rec.md2_created_timestamp,
        tgt.md3_created_timestamp = cs_md_ts_rec.md3_created_timestamp,
        tgt.locked                = CASE WHEN cs_md_locking_rec.id IS NOT NULL THEN 'Y' ELSE 'N' END,
        tgt.verification_attempts = 0,
        tgt.change_timestamp      = v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6),
        tgt.updated_by            = v_agent_new,
        tgt.updated_timestamp     = v_Change_timestamp_pst,
        tgt.is_primary            = :NEW.is_primary
    WHEN NOT MATCHED THEN
      INSERT (
        legacy_customer_id, account_id, credit_account_id, routing_number, account_number,
        account_type, result, agent, channel, verification_type, verified_date,
        mdamount1, mdamount2, mdamount3,
        md1_status, md2_status, md3_status,
        md1_created_timestamp, md2_created_timestamp, md3_created_timestamp,
        locked, verification_attempts, change_timestamp,
        created_by, created_timestamp, updated_by, updated_timestamp, is_primary
      )
      VALUES (
        cs_customer_rec.customer_id, v_bank_account_id_new, v_credit_account_id_new,
        :NEW.routing_number, :NEW.account_number,
        CASE :NEW.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END,
        v_result_new, v_agent_new, v_channel_new, v_verification_type_new, v_verified_date_new,
        cs_md_rec.mdamount1, cs_md_rec.mdamount2, cs_md_rec.mdamount3,
        cs_md_status_rec.md1_status, cs_md_status_rec.md2_status, cs_md_status_rec.md3_status,
        cs_md_ts_rec.md1_created_timestamp, cs_md_ts_rec.md2_created_timestamp, cs_md_ts_rec.md3_created_timestamp,
        CASE WHEN cs_md_locking_rec.id IS NOT NULL THEN 'Y' ELSE 'N' END,
        0, v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6),
        v_agent_new, v_created_date_new, v_agent_new, v_Change_timestamp_pst, :NEW.is_primary
      );
  
  ---------------------------------------------------------------------------
  -- UPDATE branch: Update is_primary and account type (legacy or migrated)
  ---------------------------------------------------------------------------
  ELSIF UPDATING AND (:OLD.is_primary != :NEW.is_primary OR :OLD.account_type_id != :NEW.account_type_id) THEN
    IF v_is_migrated != 2 THEN
      -- Legacy update
      UPDATE ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT
      SET is_primary        = :NEW.is_primary,
          Account_Type      = CASE :NEW.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END,
          updated_timestamp = v_Change_timestamp_pst,
          change_timestamp  = v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6)
      WHERE legacy_customer_id = :NEW.customer_id
        AND account_number     = :NEW.account_number
        AND routing_number     = :NEW.routing_number
        AND account_type       = CASE :OLD.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END;
    ELSE
      -- Migrated update, only run if not MOD-originated
      If v_Is_Change_From_MOD = 'False' THEN 
        MERGE INTO ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT tgt
        USING DUAL
        ON (
          tgt.legacy_customer_id = cs_customer_rec.customer_id AND
          tgt.account_id         = v_bank_account_id_new AND
          tgt.credit_account_id  = v_credit_account_id_new
        )
        WHEN MATCHED THEN
          UPDATE SET
            tgt.routing_number        = :NEW.routing_number,
            tgt.account_number        = :NEW.account_number,
            tgt.account_type          = CASE :NEW.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END,
            tgt.result                = v_result_new,
            tgt.agent                 = v_agent_new,
            tgt.channel               = v_channel_new,
            tgt.verification_type     = v_verification_type_new,
            tgt.verified_date         = v_verified_date_new,
            tgt.mdamount1             = cs_md_rec.mdamount1,
            tgt.mdamount2             = cs_md_rec.mdamount2,
            tgt.mdamount3             = cs_md_rec.mdamount3,
            tgt.md1_status            = cs_md_status_rec.md1_status,
            tgt.md2_status            = cs_md_status_rec.md2_status,
            tgt.md3_status            = cs_md_status_rec.md3_status,
            tgt.md1_created_timestamp = cs_md_ts_rec.md1_created_timestamp,
            tgt.md2_created_timestamp = cs_md_ts_rec.md2_created_timestamp,
            tgt.md3_created_timestamp = cs_md_ts_rec.md3_created_timestamp,
            tgt.locked                = CASE WHEN cs_md_locking_rec.id IS NOT NULL THEN 'Y' ELSE 'N' END,
            tgt.verification_attempts = 0,
            tgt.change_timestamp      = v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6),
            tgt.updated_by            = v_agent_new,
            tgt.updated_timestamp     = v_Change_timestamp_pst,
            tgt.is_primary            = :NEW.is_primary
        WHEN NOT MATCHED THEN
          INSERT (
            legacy_customer_id, account_id, credit_account_id, routing_number, account_number,
            account_type, result, agent, channel, verification_type, verified_date,
            mdamount1, mdamount2, mdamount3,
            md1_status, md2_status, md3_status,
            md1_created_timestamp, md2_created_timestamp, md3_created_timestamp,
            locked, verification_attempts, change_timestamp,
            created_by, created_timestamp, updated_by, updated_timestamp, is_primary
          )
          VALUES (
            cs_customer_rec.customer_id, v_bank_account_id_new, v_credit_account_id_new,
            :NEW.routing_number, :NEW.account_number,
            CASE :NEW.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END,
            v_result_new, v_agent_new, v_channel_new, v_verification_type_new, v_verified_date_new,
            cs_md_rec.mdamount1, cs_md_rec.mdamount2, cs_md_rec.mdamount3,
            cs_md_status_rec.md1_status, cs_md_status_rec.md2_status, cs_md_status_rec.md3_status,
            cs_md_ts_rec.md1_created_timestamp, cs_md_ts_rec.md2_created_timestamp, cs_md_ts_rec.md3_created_timestamp,
            CASE WHEN cs_md_locking_rec.id IS NOT NULL THEN 'Y' ELSE 'N' END,
            0, v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6),
            v_agent_new, v_created_date_new, v_agent_new, v_Change_timestamp_pst, :NEW.is_primary
          );
      END IF;
    END IF;

  ---------------------------------------------------------------------------
  -- DELETE branch: Mark result INACTIVE in staging
  ---------------------------------------------------------------------------
  ELSIF DELETING THEN
    OPEN cs_bav_bank_verification_del;
    FETCH cs_bav_bank_verification_del INTO 
      v_bank_account_verification_id,
      v_verification_type,
      v_verified_date,
      v_result,
      v_channel,
      v_agent,
      v_created_date,
      v_credit_account_id,
      v_bank_account_id,
      v_is_migrated;
    CLOSE cs_bav_bank_verification_del;

    IF v_is_migrated != 2 THEN
      -- Legacy: Set result as INACTIVE
      UPDATE ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT
      SET result = 'INACTIVE',
          updated_timestamp = v_Change_timestamp_pst,
          change_timestamp  = v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6)
      WHERE legacy_customer_id = :OLD.customer_id
        AND account_number     = :OLD.account_number
        AND routing_number     = :OLD.routing_number
        AND account_type       = CASE :OLD.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END;
    ELSE
      -- Migrated: update or insert (if not found) as INACTIVE
      OPEN cs_customer(v_credit_account_id); FETCH cs_customer INTO cs_customer_rec; CLOSE cs_customer;
      If v_Is_Change_From_MOD = 'False' THEN 
        MERGE INTO ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT tgt
        USING DUAL
        ON (
          tgt.legacy_customer_id = cs_customer_rec.customer_id AND
          tgt.account_id         = v_bank_account_id AND
          tgt.credit_account_id  = v_credit_account_id
        )
        WHEN MATCHED THEN
          UPDATE SET
            tgt.result            = 'INACTIVE',
            tgt.updated_by        = v_agent,
            tgt.updated_timestamp = v_Change_timestamp_pst,
            tgt.change_timestamp  = v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6)
        WHEN NOT MATCHED THEN
          INSERT (
            legacy_customer_id, account_id, credit_account_id, routing_number, account_number,
            account_type, result, agent, channel, verification_type, verified_date,
            change_timestamp, created_by, created_timestamp, updated_by, updated_timestamp, is_primary
          )
          VALUES (
            cs_customer_rec.customer_id, v_bank_account_id, v_credit_account_id,
            :OLD.routing_number, :OLD.account_number,
            CASE :OLD.account_type_id WHEN 1 THEN 'CHK' ELSE 'SAV' END,
            'INACTIVE', v_agent, v_channel, v_verification_type, v_verified_date,
            v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6),
            v_agent, v_created_date, v_agent, v_Change_timestamp_pst, :OLD.is_primary
          );
      END IF;
    END IF;
  END IF;

EXCEPTION
  -- On error, log full error to trigger_log for traceability
  WHEN OTHERS THEN
    INSERT INTO dbproc.trigger_log (
      trigger_name, table_name, table_id, source_system, destination_system,
      status, failure_reason, failure_count, created_by, created_timestamp,
      updated_by, updated_timestamp, domain, trigger_input
    ) VALUES (
      'TRIG_INS_UPD_DEL_T_CUSTOMER_BANK_ACCOUNT', 'T_CUSTOMER_BANK_ACCOUNT', 'customer_bank_account_id:' || TO_CHAR(:NEW.customer_bank_account_id),
      'LEGACY', 'MODERN', 'FAILED',
      DBMS_UTILITY.format_error_stack || DBMS_UTILITY.format_error_backtrace,
      0, 'TRIGGER', SYSTIMESTAMP, '', '', 'EXTERNAL_ACCOUNTS', ''
    );
END;
