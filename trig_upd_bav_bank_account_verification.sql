CREATE OR REPLACE TRIGGER ODS_STG.trig_upd_bav_bank_account_verification
  AFTER UPDATE OF verified_date -- Trigger fires after verified_date is updated
  ON ach.t_bav_bank_account_verification
  REFERENCING NEW AS NEW
  FOR EACH ROW
DECLARE
  PRAGMA autonomous_transaction;

  -- Fetch legacy customer_id for credit account
  CURSOR cs_customer IS
    SELECT customer_id
    FROM t_credit_account
    WHERE credit_account_id = :NEW.Credit_Account_Id;
  cs_customer_rec cs_customer%ROWTYPE;

  -- Fetch routing/account info and normalized account_type
  CURSOR cs_bav_bank_account IS
    SELECT id, aba_number, account_number,
           CASE TO_CHAR(account_type) WHEN 'Checking' THEN 'CHK' ELSE 'SAV' END AS account_type
    FROM ach.T_BAV_BANK_ACCOUNT tbba
    WHERE id = :NEW.Bank_Account_Id;
  cs_bav_bank_account_rec cs_bav_bank_account%ROWTYPE;

  -- Fetch most recent three statuses for this micro-deposit verification
  CURSOR cs_md_status IS
    SELECT * FROM (
      SELECT
        bank_account_Verification_id,
        status,
        row_number() OVER (PARTITION BY bank_account_Verification_id ORDER BY created_date asc) rnum
      FROM ach.T_BAV_MD_VERIFICATION_TRANSACTION
      WHERE bank_account_Verification_id = :NEW.Bank_Account_Verification_Id
    ) PIVOT (
      max(status) FOR RNUM IN (1 AS md1_status, 2 AS md2_status, 3 AS md3_status)
    );
  cs_md_status_rec cs_md_status%ROWTYPE;

  -- Check if a locking record exists for this verification (used for attempts, etc)
  CURSOR cs_md_locking IS
    SELECT ID
    FROM ach.T_BAV_MD_VERIFICATION_LOCKING
    WHERE bank_account_verification_id = :NEW.Bank_Account_Verification_Id;
  cs_md_locking_rec cs_md_locking%ROWTYPE;

  -- Count the number of verification attempts for this lock
  CURSOR cs_md_attempt(p_locking_id NUMBER) IS
    SELECT count(*) AS attempts
    FROM ach.T_BAV_MD_VERIFICATION_ATTEMPT
    WHERE verification_locking_id = p_locking_id
    GROUP BY verification_locking_id;
  cs_md_attempt_rec cs_md_attempt%ROWTYPE;

  -- Fetch micro-deposit amounts for up to 3 transactions
  CURSOR cs_md(p_bav_id NUMBER) IS
    SELECT * FROM (
      SELECT bank_account_verification_id, amount,
             ROW_NUMBER() OVER (PARTITION BY bank_account_verification_id ORDER BY created_date) rnum
      FROM ach.T_BAV_MD_VERIFICATION_TRANSACTION
      WHERE bank_account_verification_id = p_bav_id
    ) PIVOT (
      SUM(amount) FOR rnum IN (1 AS mdamount1, 2 AS mdamount2, 3 AS mdamount3)
    );
  cs_md_rec cs_md%ROWTYPE;

  -- Fetch timestamps for each micro-deposit transaction (up to 3)
  CURSOR cs_md_ts(p_bav_id NUMBER) IS
    SELECT * FROM (
      SELECT bank_account_verification_id, created_date,
             ROW_NUMBER() OVER (PARTITION BY bank_account_verification_id ORDER BY created_date) rnum
      FROM ach.T_BAV_MD_VERIFICATION_TRANSACTION
      WHERE bank_account_verification_id = p_bav_id
    ) PIVOT (
      MAX(created_date) FOR rnum IN (1 AS md1_created_timestamp, 2 AS md2_created_timestamp, 3 AS md3_created_timestamp)
    );
  cs_md_ts_rec cs_md_ts%ROWTYPE;

  -- Verification record fields (current event)
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

  -- Staging table fields (transformed values)
  v_result_new ach.t_bav_bank_account_verification.result%TYPE;
  v_verification_type_new ach.t_bav_bank_account_verification.verification_type%TYPE;
  v_verified_date_new ach.t_bav_bank_account_verification.verified_date%TYPE;
  v_channel_new ach.t_bav_bank_account_verification.channel%TYPE;
  v_agent_new ach.t_bav_bank_account_verification.agent%TYPE;
  v_created_date_new ach.t_bav_bank_account_verification.created_date%TYPE;
  v_credit_account_id_new ach.t_bav_bank_account_verification.credit_account_id%TYPE;
  v_bank_account_id_new ach.t_bav_bank_account.ID%TYPE;
  v_Is_Primary number;
  V_Status_From_Staging ods_stg.stage_ach_modern_ext_bank_account.result%TYPE;

  -- For audit/update timestamps
  v_Change_timestamp_pst ods_stg.stage_ach_ext_bank_account.change_timestamp%TYPE;
  v_Is_Change_From_MOD varchar2(10):=CASE WHEN ods_stg.trig_ctx_pkg.is_from_trig_MBA THEN 'True' Else 'False' END;

BEGIN
  -- Fetch key attributes for this account/verification
  OPEN cs_customer;
  FETCH cs_customer INTO cs_customer_rec;
  CLOSE cs_customer;

  OPEN cs_bav_bank_account;
  FETCH cs_bav_bank_account INTO cs_bav_bank_account_rec;
  CLOSE cs_bav_bank_account;

  OPEN cs_md_status;
  FETCH cs_md_status INTO cs_md_status_rec;
  CLOSE cs_md_status;

  OPEN cs_md_locking;
  FETCH cs_md_locking INTO cs_md_locking_rec;
  CLOSE cs_md_locking;

  OPEN cs_md_attempt(cs_md_locking_rec.ID);
  FETCH cs_md_attempt INTO cs_md_attempt_rec;
  CLOSE cs_md_attempt;

  SELECT SYSTIMESTAMP AT TIME ZONE 'PST' INTO v_Change_timestamp_pst FROM dual;

  -- Assign current row’s values to local variables
  v_bank_account_verification_id := :new.bank_account_verification_id;
  v_verification_type := :new.verification_type;
  v_verified_date := :new.verified_date;
  v_result := :new.result;
  v_channel := :new.channel;
  v_agent := :new.agent;
  v_created_date := :new.created_date;
  v_credit_account_id := :new.credit_account_id;
  v_bank_account_id := :new.bank_account_id;
  v_is_migrated := :new.is_migrated;

  -- Set staging/normalized result and type based on logic
  IF (v_verification_type in ('Initial', 'Ews') AND v_result = 'Pass') THEN
    v_result_new := 'ACTIVE';
    v_verification_type_new := 'EWS';
    v_verified_date_new := v_verified_date;
  ELSIF ((v_verification_type = 'MicroDeposit' AND NVL(v_result, 'X') = 'Pass') or  (v_verification_type = 'MicroDeposit' AND :New.Verified_Date is not null)) THEN
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

  -- Assign other transformed fields
  v_channel_new := v_channel;
  v_agent_new := v_agent;
  v_created_date_new := v_created_date;
  v_credit_account_id_new := v_credit_account_id;
  v_bank_account_id_new := v_bank_account_id;

  -- Fetch extra MD/TS info if verification is valid
  IF v_bank_account_verification_id IS NOT NULL THEN
    OPEN cs_customer;
    FETCH cs_customer INTO cs_customer_rec;
    CLOSE cs_customer;

    OPEN cs_md(v_bank_account_verification_id);
    FETCH cs_md INTO cs_md_rec;
    CLOSE cs_md;

    OPEN cs_md_status;
    FETCH cs_md_status INTO cs_md_status_rec;
    CLOSE cs_md_status;

    OPEN cs_md_ts(v_bank_account_verification_id);
    FETCH cs_md_ts INTO cs_md_ts_rec;
    CLOSE cs_md_ts;
  END IF;

  -- Get IS_PRIMARY status for this customer/account/routing (if exists)
  BEGIN
    SELECT IS_PRIMARY INTO v_Is_Primary
      FROM account.t_customer_bank_account
      WHERE Customer_id = cs_customer_rec.customer_id
        AND ROUTING_NUMBER = cs_bav_bank_account_rec.aba_number
        AND ACCOUNT_NUMBER = cs_bav_bank_account_rec.account_number;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      v_Is_Primary := NULL;
  END;

  -- Only update if an UPDATE statement (required for AFTER UPDATE trigger)
  IF UPDATING THEN
    -- Legacy/Non-migrated account, update the legacy staging table directly
    IF :NEW.Is_Migrated != 2 AND :NEW.VERIFICATION_TYPE = 'MicroDeposit' THEN
      UPDATE ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT
      SET result = CASE
                     WHEN :NEW.Verification_Type = 'MicroDeposit' AND :NEW.VERIFIED_DATE IS NOT NULL THEN 'ACTIVE'
                     ELSE TO_CHAR(:OLD.RESULT)
                   END,
          agent = :NEW.Agent,
          channel = :NEW.Channel,
          verification_type = UPPER(:NEW.VERIFICATION_TYPE),
          verified_date = :NEW.VERIFIED_DATE,
          md1_status = cs_md_status_rec.md1_status,
          md2_status = cs_md_status_rec.md2_status,
          md3_status = cs_md_status_rec.md3_status,
          locked = CASE WHEN cs_md_locking_rec.ID IS NOT NULL THEN 'Y' ELSE 'N' END,
          verification_attempts = cs_md_attempt_rec.attempts,
          change_timestamp = SYSDATE,
          UPDATED_BY = :NEW.Agent,
          UPDATED_TIMESTAMP = SYSDATE
      WHERE legacy_customer_id = cs_customer_rec.customer_id
        AND ROUTING_NUMBER = cs_bav_bank_account_rec.aba_number
        AND account_number = cs_bav_bank_account_rec.account_number;
      commit;

    ELSE
      -- For migrated/modern, check the modern staging table for current status
      BEGIN
        SELECT  RESULT INTO V_Status_From_Staging
        FROM ods_stg.stage_ach_modern_ext_bank_account
        WHERE LEGACY_CUSTOMER_ID = cs_customer_rec.customer_id
          AND ROUTING_NUMBER = cs_bav_bank_account_rec.aba_number
          AND ACCOUNT_NUMBER = cs_bav_bank_account_rec.account_number
          AND ACCOUNT_TYPE = cs_bav_bank_account_rec.account_type;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_Status_From_Staging := NULL;
      END;

      -- Audit log for debugging (inserted every update for MODERN)
      INSERT INTO dbproc.trigger_log(
        trigger_name, table_name, table_id, source_system, destination_system, status,
        failure_reason, failure_count, created_by, created_timestamp,
        updated_by, updated_timestamp, domain, trigger_input
      ) VALUES (
        'TRIG_UPD_BAV_BANK_ACCOUNT_VERIFICATION',
        'T_BAV_BANK_ACCOUNT_VERIFICATION',
        'Bank_Account_Verification_Id:' || TO_CHAR(:NEW.Bank_Account_Verification_Id),
        'LEGACY',
        'MODERN',
        'FAILED',
        ' | ABA: ' || TO_CHAR(cs_bav_bank_account_rec.aba_number) ||
        ' | Account: ' || TO_CHAR(cs_bav_bank_account_rec.account_number) ||
        ' | Credit Card No: ' || TO_CHAR(v_credit_account_id_new) || 'account type ' || TO_CHAR(cs_bav_bank_account_rec.account_type || 'Staging ' || V_Status_From_Staging),
        1,
        'TRIGGER',
        SYSTIMESTAMP,
        '', '', 'EXTERNAL_ACCOUNTS', ''
      );
      commit;

      -- Only update modern staging table if change originated from legacy (not MOD)
      IF v_Is_Change_From_MOD = 'False' AND :NEW.Is_Migrated = 2 THEN
        MERGE INTO ods_stg.STAGE_ACH_EXT_BANK_ACCOUNT tgt
        USING (
          SELECT
            cs_customer_rec.customer_id AS legacy_customer_id,
            v_bank_account_id_new AS account_id,
            v_credit_account_id_new AS credit_account_id,
            cs_bav_bank_account_rec.aba_number AS ROUTING_NUMBER,
            cs_bav_bank_account_rec.account_number AS ACCOUNT_NUMBER,
            cs_bav_bank_account_rec.account_type AS ACCOUNT_TYPE,
            v_result_new AS result,
            v_agent_new AS agent,
            v_channel_new AS channel,
            v_verification_type_new AS verification_type,
            v_verified_date_new AS verified_date,
            cs_md_rec.mdamount1 AS mdamount1,
            cs_md_rec.mdamount2 AS mdamount2,
            cs_md_rec.mdamount3 AS mdamount3,
            cs_md_status_rec.md1_status AS md1_status,
            cs_md_status_rec.md2_status AS md2_status,
            cs_md_status_rec.md3_status AS md3_status,
            cs_md_ts_rec.md1_created_timestamp AS md1_created_timestamp,
            cs_md_ts_rec.md2_created_timestamp AS md2_created_timestamp,
            cs_md_ts_rec.md3_created_timestamp AS md3_created_timestamp,
            CASE WHEN cs_md_attempt_rec.attempts = 3 THEN 'Y' ELSE 'N' END AS locked,
            NVL(cs_md_attempt_rec.attempts, 0) AS verification_attempts,
            v_Change_timestamp_pst + INTERVAL '0 00:00:00.000011' DAY TO SECOND(6) AS change_timestamp,
            v_agent_new AS created_by,
            v_created_date_new AS created_timestamp,
            v_agent_new AS updated_by,
            v_Change_timestamp_pst AS updated_timestamp,
            v_Is_Primary as IS_PRIMARY
          FROM dual
        ) src
        ON (
          tgt.legacy_customer_id = src.legacy_customer_id and
          tgt.ROUTING_NUMBER = src.ROUTING_NUMBER and
          tgt.ACCOUNT_NUMBER = src.ACCOUNT_NUMBER
        )
        WHEN MATCHED THEN
          UPDATE SET
            tgt.credit_account_id = src.credit_account_id,
            tgt.account_id = src.account_id,
            tgt.ACCOUNT_type = src.ACCOUNT_type,
            tgt.result = src.result,
            tgt.agent = src.agent,
            tgt.channel = src.channel,
            tgt.verification_type = src.verification_type,
            tgt.verified_date = src.verified_date,
            tgt.mdamount1 = src.mdamount1,
            tgt.mdamount2 = src.mdamount2,
            tgt.mdamount3 = src.mdamount3,
            tgt.md1_status = src.md1_status,
            tgt.md2_status = src.md2_status,
            tgt.md3_status = src.md3_status,
            tgt.md1_created_timestamp = src.md1_created_timestamp,
            tgt.md2_created_timestamp = src.md2_created_timestamp,
            tgt.md3_created_timestamp = src.md3_created_timestamp,
            tgt.locked = src.locked,
            tgt.verification_attempts = src.verification_attempts,
            tgt.change_timestamp = src.change_timestamp,
            tgt.updated_by = src.updated_by,
            tgt.updated_timestamp = src.updated_timestamp,
            tgt.IS_PRIMARY = src.IS_PRIMARY
        WHEN NOT MATCHED THEN
          INSERT (
            legacy_customer_id, account_id, credit_account_id, ROUTING_NUMBER, ACCOUNT_NUMBER,account_type,
            result, agent, channel, verification_type, verified_date,
            mdamount1, mdamount2, mdamount3,
            md1_status, md2_status, md3_status,
            md1_created_timestamp, md2_created_timestamp,md3_created_timestamp,
            locked, verification_attempts, change_timestamp,
            created_by, created_timestamp, updated_by, updated_timestamp,IS_PRIMARY
          )
          VALUES (
            src.legacy_customer_id, src.account_id, src.credit_account_id, src.ROUTING_NUMBER, src.ACCOUNT_NUMBER,src.account_type,
            src.result, src.agent, src.channel, src.verification_type, src.verified_date,
            src.mdamount1, src.mdamount2, src.mdamount3,
            src.md1_status, src.md2_status, src.md3_status,
            src.md1_created_timestamp, src.md2_created_timestamp, src.md3_created_timestamp,
            src.locked, src.verification_attempts, src.change_timestamp,
            src.created_by, src.created_timestamp, src.updated_by, src.updated_timestamp,src.IS_PRIMARY
          );
        commit;
      END IF;
    END IF;
    commit;
  END IF;

EXCEPTION
  -- Log all unhandled exceptions for troubleshooting
  WHEN OTHERS THEN
    INSERT INTO dbproc.trigger_log(
      trigger_name, table_name, table_id, source_system, destination_system, status,
      failure_reason, failure_count, created_by, created_timestamp,
      updated_by, updated_timestamp, domain, trigger_input
    ) VALUES (
      'TRIG_UPD_BAV_BANK_ACCOUNT_VERIFICATION',
      'T_BAV_BANK_ACCOUNT_VERIFICATION',
      'Bank_Account_Verification_Id:' || TO_CHAR(:NEW.Bank_Account_Verification_Id),
      'LEGACY',
      'MODERN',
      'FAILED',
      DBMS_UTILITY.format_error_stack || DBMS_UTILITY.format_error_backtrace,
      1,
      'TRIGGER',
      SYSTIMESTAMP,
      '', '', 'EXTERNAL_ACCOUNTS', ''
    );
END;
