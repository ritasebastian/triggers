CREATE OR REPLACE TRIGGER ACH."TRIG_INS_STG_MDRN_EXT_BNK_ACCT"
    AFTER INSERT OR UPDATE
    ON ods_Stg.STAGE_ACH_MODERN_EXT_BANK_ACCOUNT
    REFERENCING NEW AS NEW OLD AS OLD
    FOR EACH ROW
    
DECLARE

    CURSOR cs_bav_user IS
      SELECT ID, individual_name 
      FROM ach.t_bav_user
      WHERE individual_identifier=:NEW.LEGACY_CUSTOMER_ID
      AND identifier_type=2;
             
      cs_bav_user_rec cs_bav_user%ROWTYPE;
     
    CURSOR cs_customer_full_name IS
      SELECT n.name
      FROM t_primary_chd chd
      JOIN t_name n ON n.name_id=chd.name_id
      WHERE chd.customer_id=:NEW.LEGACY_CUSTOMER_ID
      AND chd.credit_account_id=:NEW.CREDIT_ACCOUNT_ID;
         
      cs_customer_full_name_rec cs_customer_full_name%ROWTYPE;
    
    CURSOR cs_bank IS
      SELECT ID
      FROM ach.T_BAV_BANK_ACCOUNT tbba
      WHERE ABA_NUMBER = :NEW.routing_number
      AND account_Number = :NEW.account_number
      AND account_Type = CASE WHEN :NEW.account_type = 'CHK' THEN 'Checking'  ELSE 'Savings' END;
    
    CURSOR cs_bank_name IS
      SELECT RT_INSTIT as bank_name
      FROM( 
             SELECT RT_INSTIT FROM ach.t_rt_thompson_01
             WHERE RT_MICR = :NEW.ROUTING_NUMBER
      )WHERE rownum=1;
                   
      cs_bank_name_rec     cs_bank_name%ROWTYPE;           
    
    CURSOR cs_card IS
      SELECT CARD_ID
      FROM account.t_Card
      WHERE     credit_Account_id = :NEW.credit_account_id
      AND current_Card = 1;
          
      v_card_id NUMBER;
    

    CURSOR cs_existing_account IS 
      SELECT *
      FROM account.t_customer_bank_account
      WHERE Customer_id = :NEW.LEGACY_CUSTOMER_ID
      AND ROUTING_NUMBER = :NEW.ROUTING_NUMBER
      AND ACCOUNT_NUMBER = :NEW.ACCOUNT_NUMBER
      AND ACCOUNT_TYPE_ID = (CASE WHEN :OLD.ACCOUNT_TYPE = 'CHK' THEN '1' ELSE '2' END);
                
      cs_existing_account_rec cs_existing_account%ROWTYPE;
    
    bank_rec              cs_bank%ROWTYPE;
    bank_rec2              cs_bank%ROWTYPE;
    v_bank_Account_type   VARCHAR2 (20);
    v_generated_id NUMBER;
    v_bank_account_verification_id NUMBER;
    --v_verification_type VARCHAR2(50);
    v_verified_date       TIMESTAMP(7);
    v_customer_id NUMBER;
    v_customer_bank_account_id NUMBER;

    v_uc_id             NUMBER;
    v_uc_id_existing NUMBER;
    v_uc_audit_id    NUMBER;
    v_ref_num VARCHAR2(30);
    v_is_existing_account BOOLEAN := FALSE;

    v_account_last_four VARCHAR2(4):= CASE WHEN LENGTH(:NEW.ACCOUNT_NUMBER)<=4 THEN :NEW.ACCOUNT_NUMBER ELSE SUBSTR(:NEW.ACCOUNT_NUMBER,-4) END;
  
BEGIN
  
    SELECT fn_ref_num_seq() INTO v_ref_num FROM DUAL;

    OPEN cs_bav_user;
    FETCH cs_bav_user INTO cs_bav_user_rec;
    CLOSE cs_bav_user;
    
    IF cs_bav_user_rec.ID IS NULL THEN 
                  
        OPEN cs_customer_full_name;
        FETCH cs_customer_full_name INTO cs_customer_full_name_rec;
        CLOSE cs_customer_full_name;
                
        INSERT INTO ach.t_bav_user
               (individual_identifier,
               identifier_type,
               individual_name,
               id_old)
        VALUES(
               :NEW.LEGACY_CUSTOMER_ID,
               2,
               cs_customer_full_name_rec.name,
               NULL
        ) RETURNING ID, individual_name INTO cs_bav_user_rec.ID, cs_bav_user_rec.individual_name;
    END IF;
    
    
    OPEN cs_bank;
    FETCH cs_bank INTO bank_rec;
    CLOSE cs_bank;

    IF :NEW.account_type = 'CHK' THEN
        v_bank_Account_Type := 'Checking';
    ELSE
        v_bank_Account_Type := 'Savings';
    END IF;

    IF bank_rec.id IS NULL THEN
        INSERT INTO ACH.T_BAV_BANK_ACCOUNT 
               (aba_number,
               account_number,
               account_Type)
        VALUES (:NEW.routing_number,
               :NEW.account_number,
               v_bank_Account_Type);
    ELSE
        UPDATE ACH.T_BAV_BANK_ACCOUNT
        SET ACCOUNT_TYPE = v_bank_account_Type
        WHERE id = bank_rec.id;
    END IF;

    OPEN cs_card;
    FETCH cs_card INTO v_card_id;    
    CLOSE cs_card;
        
    OPEN cs_existing_account;
    FETCH cs_existing_account INTO cs_existing_account_rec;
            IF cs_existing_account%FOUND THEN
                v_is_existing_account := TRUE;
            END IF;
    CLOSE cs_existing_account;


    IF INSERTING AND :NEW.Single_Use != '1' AND v_is_existing_account = FALSE THEN
      
        --Check if operation_type='ADD'
        IF :NEW.operation_type='ADD' THEN
              OPEN cs_bank;
              FETCH cs_bank INTO bank_rec2;
              CLOSE cs_bank;

              IF (:NEW.Verification_Type IS NULL AND :NEW.Result='PENDING_ACH_VERIFICATION')
                  OR (:NEW.Verification_Type ='AGENT' AND :NEW.Result='ACTIVE') THEN
                  
                      SELECT fnbm_seq.NEXTVAL INTO v_uc_id FROM DUAL;
                       
                      SELECT fnbm_seq.NEXTVAL INTO v_uc_audit_id FROM DUAL;

                      -- ADD WCH for bank accunt verification 
                      INSERT INTO t_uc
                             (status_id,
                             type_id,
                             ref_num,
                             open_date,
                             open_agent_id,
                             status_date,
                             status_agent_id,
                             uc_id,
                             credit_account_id,
                             card_id)
                      VALUES
                             (1,
                             CASE WHEN :NEW.Result ='ACTIVE' THEN 415471 WHEN :NEW.Result ='PENDING_ACH_VERIFICATION' THEN 415472 END,
                             v_ref_num, 
                             :NEW.CREATED_TIMESTAMP,
                             :NEW.CREATED_BY,
                             :NEW.UPDATED_TIMESTAMP,
                             :NEW.UPDATED_BY,
                             v_uc_id,
                             :NEW.CREDIT_ACCOUNT_ID,
                             v_card_id);
                                               
                      -- Add Audit steps for bank account verification work case created 
                      INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                      VALUES  (v_uc_audit_id, 1,'WORK CASE CREATED, STATUS OPEN', :NEW.UPDATED_TIMESTAMP,v_uc_id );
                      
                      INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                      VALUES  (v_uc_audit_id, 1,'Verification on account ending in: '|| v_account_last_four, :NEW.UPDATED_TIMESTAMP,v_uc_id );
                      
                      INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                      VALUES  (v_uc_audit_id, 1,'Verification Method: Ews', :NEW.UPDATED_TIMESTAMP,v_uc_id );
                      
                      IF (:NEW.Result='ACTIVE') THEN
                          INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                          VALUES  (v_uc_audit_id, 1,'Verification Status: Successful', :NEW.UPDATED_TIMESTAMP,v_uc_id );
                      ELSE
                          INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                          VALUES  (v_uc_audit_id, 1,'Verification Status:  Failed', :NEW.UPDATED_TIMESTAMP,v_uc_id );
                      END IF;
                      
                      INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                      VALUES  (v_uc_audit_id, 1,'STATUS CHANGED TO RCLOSED', :NEW.UPDATED_TIMESTAMP,v_uc_id );                           
               
                      INSERT INTO ACH.T_BAV_BANK_ACCOUNT_VERIFICATION(
                              verification_type,
                               created_date,
                               verified_date,
                               work_case_id,
                               agent,
                               channel,
                               RESULT,
                               credit_account_id,
                               is_migrated,
                               bank_account_id,
                               user_id)                              
                      VALUES  ('Initial',
                              :NEW.created_timestamp,
                              :NEW.verified_date,
                              NULL,--v_uc_id,
                              :NEW.created_by,
                              CASE WHEN :NEW.channel='AGENT' THEN 'CASH' 
                                  WHEN :NEW.channel='CAS' THEN 'CAS'
                                  WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                  WHEN :NEW.channel='WEB' Then 'Web'
                              END,
                              'StepUp',
                              :NEW.credit_account_id,
                              2,
                              nvl(bank_rec.id,bank_rec2.id),
                              cs_bav_user_rec.ID
                              );
                              
              END IF;


              INSERT INTO ACH.T_BAV_BANK_ACCOUNT_VERIFICATION(
                    verification_type,
                    created_date,
                    verified_date,
                    work_case_id,
                    agent,
                    channel,
                    RESULT,
                    credit_account_id,
                    is_migrated,
                    bank_account_id,
                    user_id)              
              VALUES(
                    CASE WHEN :NEW.Verification_Type IS NULL AND :NEW.Result='NEW' THEN 'Initial'
                    WHEN :NEW.Verification_Type IS NULL AND :NEW.Result='PENDING_ACH_VERIFICATION' THEN 'Ews'
                    WHEN :NEW.Verification_Type ='AGENT' AND :NEW.Result='ACTIVE' THEN 'Ews'
                    WHEN :NEW.Verification_Type='EWS' THEN 'Ews'
                    WHEN :NEW.Verification_Type='YODLEE' THEN 'Yodlee'
                    ELSE 'UnKnown'
                    END,
                    :NEW.created_timestamp + interval '0 00:00:00.000011' day to second(6),
                    CASE WHEN :NEW.Verification_Type ='AGENT' AND :NEW.Result='ACTIVE' 
                    THEN :NEW.created_timestamp
                    ELSE :NEW.verified_date 
                    END,
                    v_uc_id,
                    :NEW.created_by,
                    CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                    WHEN :NEW.channel='CAS' THEN 'CAS'
                    WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                    WHEN :NEW.channel='WEB' Then 'Web'
                    END,
                    CASE WHEN :NEW.Verification_Type IS NULL 
                    AND :NEW.Result IN ('NEW','PENDING_ACH_VERIFICATION') 
                    THEN 'StepUp'
                    ELSE 'Pass' 
                    END,
                    :NEW.credit_account_id,
                    2,
                    nvl(bank_rec.id,bank_rec2.id),
                    cs_bav_user_rec.ID
                    );

              --To ensure to add bank account only if it is ACTIVE / PENDING_ACH_VALIDATION)
              IF (:NEW.Result='ACTIVE') THEN 
                   
          ods_stg.trig_ctx_pkg.set_from_trig_MBA;
                  PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_account(:NEW.legacy_customer_id,
                    :NEW.routing_number,                                        
                    :NEW.account_number,
                    CASE WHEN :NEW.account_type='SAV' Then 2
                         WHEN :NEW.account_type='CHK' Then 1 END,
                    v_generated_id
                   );
                   ods_stg.trig_ctx_pkg.clear_from_trig_MBA;
           
                  PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_acct_hist(:NEW.legacy_customer_id,
                                            1,
                                            :NEW.account_number,
                                            :NEW.routing_number,
                                            CASE WHEN :NEW.account_type='SAV' Then 2
                                                 WHEN :NEW.account_type='CHK' Then 1
                                            END,
                                            CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                                                 WHEN :NEW.channel='CAS' THEN 'CAS'
                                                 WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                                 WHEN :NEW.channel='WEB' Then 'Web'
                                            END
                                            );                                                                                                                            

                  SELECT fnbm_seq.NEXTVAL INTO v_uc_id FROM DUAL;                         
                  SELECT fnbm_seq.NEXTVAL  INTO v_uc_audit_id FROM DUAL;
                  
                  -- Add work case of bank account registration                        
                  INSERT INTO t_uc(
                         status_id,
                         type_id,
                         ref_num,
                         open_date,
                         open_agent_id,
                         status_date,
                         status_agent_id,
                         uc_id,
                         credit_account_id,
                         card_id)
                  VALUES
                         (1,
                         325470, -- ACH Registration
                         v_ref_num, 
                         :NEW.CREATED_TIMESTAMP,
                         :NEW.CREATED_BY,
                         :NEW.UPDATED_TIMESTAMP,
                         :NEW.UPDATED_BY,
                         v_uc_id,
                         :NEW.CREDIT_ACCOUNT_ID,
                         v_card_id);
                    
                  -- Insert audit steps for ACH Registration
                  OPEN cs_bank_name;
                  FETCH cs_bank_name INTO cs_bank_name_rec;
                  CLOSE cs_bank_name;
                                          
                  INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                  VALUES  (v_uc_audit_id, 1,'USE CASE CREATED, STATUS OPEN', :NEW.UPDATED_TIMESTAMP,v_uc_id );
                                    
                  INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                  VALUES  (v_uc_audit_id, 1,'Bank Information Updated, '||cs_bank_name_rec.bank_name||', '||:NEW.Routing_Number||', '||:NEW.Account_Number||', '||:NEW.ACCOUNT_TYPE, :NEW.UPDATED_TIMESTAMP,v_uc_id );
                  
                  INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                  VALUES  (v_uc_audit_id, 1,'STATUS CHANGED TO RCLOSED', :NEW.UPDATED_TIMESTAMP,v_uc_id );
              END IF;                                                      


              INSERT INTO ODS_STG.LEGACY_MODERN_EXT_ACCT_MAP(
                        MODERN_BANK_ACCOUNT_ID,
                        LEGACY_BANK_ACCOUNT_ID,
                        CREATED_BY,
                        CREATED_TIMESTAMP
                          )
              VALUES(:NEW.MODERN_BANK_ACCOUNT_ID,
                      nvl(bank_rec.id,bank_rec2.id),--v_generated_id,
                      'TRIGGER',
                      SYSTIMESTAMP
                      );
                      
              IF :NEW.Is_Primary=1 THEN
                  --PKG_CUSTOMER_BANK_ACCOUNT.pr_update_is_primary(v_generated_id);
                  BEGIN
                      SELECT customer_id
                      INTO v_customer_id
                      FROM account.t_customer_bank_account cba
                      WHERE cba.customer_bank_account_id = v_generated_id;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                      RETURN;
                  END;
                  
          ods_stg.trig_ctx_pkg.set_from_trig_MBA;                    
                  UPDATE account.t_customer_bank_account cba
                  SET cba.is_primary = 0
                  WHERE cba.customer_id = v_customer_id;
                  ods_stg.trig_ctx_pkg.clear_from_trig_MBA;
          
          ods_stg.trig_ctx_pkg.set_from_trig_MBA; 
                  UPDATE account.t_customer_bank_account cba
                  SET cba.is_primary = 1
                  WHERE cba.customer_bank_account_id = v_generated_id;
                  ods_stg.trig_ctx_pkg.clear_from_trig_MBA;
          
                  INSERT INTO account.t_customer_bank_account_sync
                         (customer_bank_account_id,
                         customer_id,
                         change_type)
                  VALUES
                         (v_generated_id,
                         v_customer_id,
                         3);                                   
              END IF;
                       
        END IF;
    END IF;

    IF UPDATING AND :NEW.Single_Use != '1' THEN              
          -- To ensure to add bank account only if it is ACTIVE / PENDING_ACH_VALIDATION)
          IF :NEW.operation_type='UPDATE' AND :OLD.Result='PENDING_ACH_VERIFICATION' AND :NEW.Result='PENDING_ACH_VALIDATION' 
          and v_is_existing_account = FALSE THEN
        ods_stg.trig_ctx_pkg.set_from_trig_MBA;
                PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_account(:NEW.legacy_customer_id,
                                                                      :NEW.routing_number,                                        
                                                                      :NEW.account_number,
                                                                      CASE WHEN :NEW.account_type='SAV' Then 2
                                                                           WHEN :NEW.account_type='CHK' Then 1 END,
                                                                      v_generated_id);
                ods_stg.trig_ctx_pkg.clear_from_trig_MBA;                      
                PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_acct_hist(:NEW.legacy_customer_id,
                                                                      1,
                                                                      :NEW.account_number,
                                                                      :NEW.routing_number,
                                                                      CASE WHEN :NEW.account_type='SAV' Then 2
                                                                           WHEN :NEW.account_type='CHK' Then 1
                                                                           END,
                                                                      CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                                                                           WHEN :NEW.channel='CAS' THEN 'CAS'
                                                                           WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                                                           WHEN :NEW.channel='WEB' Then 'Web'
                                                                           END);
                                                                           
                --Add work case history when account gets added based on MD verification
                SELECT fnbm_seq.NEXTVAL INTO v_uc_id FROM DUAL;
                     
                SELECT fnbm_seq.NEXTVAL INTO v_uc_audit_id FROM DUAL;
                                      
                INSERT INTO t_uc(
                       status_id,
                       type_id,
                       ref_num,
                       open_date,
                       open_agent_id,
                       status_date,
                       status_agent_id,
                       uc_id,
                       credit_account_id,
                       card_id)
                VALUES
                       (1,
                       325470, -- ACH Registration
                       v_ref_num, 
                       :NEW.UPDATED_TIMESTAMP, --:NEW.CREATED_TIMESTAMP,
                       :NEW.UPDATED_BY, --:NEW.CREATED_BY,
                       :NEW.UPDATED_TIMESTAMP,
                       :NEW.UPDATED_BY,
                       v_uc_id,
                       :NEW.CREDIT_ACCOUNT_ID,
                       v_card_id);
   
                -- Insert audit steps for ACH Registration                                      
                OPEN cs_bank_name;
                FETCH cs_bank_name INTO cs_bank_name_rec;
                CLOSE cs_bank_name;
                                      
                INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1,'USE CASE CREATED, STATUS OPEN', :NEW.UPDATED_TIMESTAMP,v_uc_id );
                
                INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1,'Bank Information Updated, '||cs_bank_name_rec.bank_name||', '||:NEW.Routing_Number||', '||:NEW.Account_Number||', '||:NEW.ACCOUNT_TYPE, :NEW.UPDATED_TIMESTAMP,v_uc_id );
                
                INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                VALUES  (v_uc_audit_id, 1,'STATUS CHANGED TO RCLOSED', :NEW.UPDATED_TIMESTAMP,v_uc_id );
                                  
          END IF; 
                       
          IF :NEW.operation_type='UPDATE' AND :OLD.Result='PENDING_ACH_VALIDATION' AND :NEW.Result='ACTIVE' THEN
                          
                SELECT work_case_id, bav.bank_account_verification_id, verified_date into v_uc_id_existing, v_bank_account_verification_id, v_verified_date
                FROM ach.t_bav_bank_account_verification bav
                Inner join ach.t_bav_bank_account ba on ba.id = bav.bank_account_id
                Where ba.aba_number = :NEW.routing_number
                and ba.account_number = :NEW.account_number
                and  ba.ACCOUNT_TYPE =  CASE :NEW.ACCOUNT_TYPE WHEN 'CHK' THEN 'Checking' WHEN 'SAV' THEN 'Savings' END
                and trunc(bav.created_date) = TRUNC(:NEW.CREATED_TIMESTAMP)
                -- removing the result from filter, as it is now update in MD trigger and current filter should identify the correct record WCH 
                -- and (verification_type = 'MicroDeposit' and result is null )
                and (verification_type = 'MicroDeposit')            
                and is_migrated = 2;
                               
                If (v_verified_date is null) THEN 
                      -- Update MD result as pass since it is moving to active status 
                      UPDATE ACH.T_BAV_BANK_ACCOUNT_VERIFICATION
                      SET verified_date=:NEW.Updated_Timestamp, result='Pass'
                      WHERE BANK_ACCOUNT_VERIFICATION_ID = v_bank_account_verification_id;
                                                    
                      INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                      VALUES  (v_uc_audit_id, 1,'Verification Status Update: Successful', :NEW.UPDATED_TIMESTAMP,v_uc_id_existing );                 
                      INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                      VALUES  (v_uc_audit_id, 1,'Verification Platform: UCRM', :NEW.UPDATED_TIMESTAMP,v_uc_id_existing );                
                      INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
                      VALUES  (v_uc_audit_id, 1,'STATUS CHANGED TO RCLOSED ', :NEW.UPDATED_TIMESTAMP,v_uc_id_existing );
                END IF;
                         
          END IF;

          IF (:NEW.operation_type='UPDATE' OR :NEW.operation_type='PRIMARY_UPDATE') 
            AND :OLD.Is_Primary=0 AND :NEW.Is_Primary=1
            AND v_is_existing_account = TRUE AND cs_existing_account_rec.IS_Primary <> :NEW.Is_Primary THEN
            --PKG_CUSTOMER_BANK_ACCOUNT.pr_update_is_primary(:NEW.legacy_customer_id);
              BEGIN
                      SELECT customer_bank_account_id
                      INTO v_customer_bank_account_id
                      FROM account.t_customer_bank_account cba
                      WHERE cba.customer_id = :NEW.legacy_customer_id
                      AND cba.routing_number=:NEW.routing_number
                      AND cba.account_number=:NEW.ACCOUNT_NUMBER
                      AND cba.account_type_id=CASE WHEN :NEW.ACCOUNT_TYPE='CHK' THEN 1 WHEN :NEW.ACCOUNT_TYPE='SAV' THEN 2 END;
                  EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                  RETURN;
              END;
      
        ods_stg.trig_ctx_pkg.set_from_trig_MBA;
              UPDATE account.t_customer_bank_account cba
              SET cba.is_primary = 0
              WHERE cba.customer_id = :NEW.legacy_customer_id;
        ods_stg.trig_ctx_pkg.clear_from_trig_MBA;
                                       
              ods_stg.trig_ctx_pkg.set_from_trig_MBA;
        UPDATE account.t_customer_bank_account cba
              SET cba.is_primary = 1
              WHERE cba.customer_bank_account_id = v_customer_bank_account_id;
        ods_stg.trig_ctx_pkg.clear_from_trig_MBA;
                                       
              INSERT INTO account.t_customer_bank_account_sync
                     (customer_bank_account_id,
                     customer_id,
                     change_type)
              VALUES
                     (v_customer_bank_account_id,
                     :NEW.legacy_customer_id,
                     3);

              PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_acct_hist(:NEW.legacy_customer_id,
                                        3,
                                        :NEW.account_number,
                                        :NEW.routing_number,
                                        CASE WHEN :NEW.account_type='SAV' Then 2
                                             WHEN :NEW.account_type='CHK' Then 1
                                        END,
                                        CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                                             WHEN :NEW.channel='CAS' THEN 'CAS'
                                             WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                             WHEN :NEW.channel='WEB' Then 'Web'
                                        END
                                        );

          END IF;

          IF :NEW.operation_type='UPDATE' AND (:OLD.ROUTING_NUMBER <> :NEW.ROUTING_NUMBER OR :OLD.ACCOUNT_NUMBER <> :NEW.ACCOUNT_NUMBER) THEN
              INSERT INTO dbproc.trigger_log(trigger_name,
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
                     trigger_input)
              VALUES (
                     'TRIG_INS_STG_MDRN_EXT_BNK_ACCT',
                     'STAGE_ACH_MODERN_EXT_BANK_ACCOUNT',
                     :NEW.STAGE_SEQUENCE_ID,
                     'MODERN',
                     'LEGACY',
                     'INFO',
                     'Account number or routing number change in update flow',
                     0,
                     'TRIGGER',
                     SYSTIMESTAMP,
                     '', 
                     '', 
                     'EXTERNAL_ACCOUNTS',
                     '' );
                              
                    /*INSERT INTO ACH.T_BAV_BANK_ACCOUNT_VERIFICATION(verification_type,
                             created_date,
                             verified_date,
                             work_case_id,
                             agent,
                             channel,
                             RESULT,
                             credit_account_id,
                             is_migrated,
                             bank_account_id,
                             user_id
                             )
                    VALUES(
                             CASE WHEN :OLD.Result='NEW' AND :NEW.Result='ACTIVE'
                                  THEN CASE 
                                  WHEN :NEW.Verification_Type='EWS' THEN 'Ews'
                                  WHEN :NEW.Verification_Type IN ('AGENT','INSTANT') THEN 'Initial'
                                  ELSE :NEW.Verification_Type
                                  END
                                  WHEN :OLD.Result='NEW' AND :NEW.Result='PENDING_ACH_VERIFICATION'
                                  THEN 'Ews'
                                  WHEN :OLD.Result='PENDING_ACH_VERIFICATION' AND :NEW.Result='PENDING_ACH_VALIDATION'
                                  THEN 'MicroDeposit'
                                  --WHEN :OLD.Result='PENDING_ACH_VALIDATION' AND :NEW.Result IN('ACTIVE','FAILED')
                                  --   THEN 'MicroDeposit'
                                  ELSE 'UnKnown02'
                                  END,
                                  :NEW.created_timestamp,
                                  :NEW.verified_date,
                                  null,
                                  :NEW.created_by,
                                  CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                                       WHEN :NEW.channel='CAS' THEN 'CAS'
                                       WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                       WHEN :NEW.channel='WEB' Then 'Web'
                                       END,
                                       CASE WHEN :OLD.Result='NEW' AND :NEW.Result='ACTIVE'
                                       THEN 'Pass'
                                       WHEN :OLD.Result='NEW' AND :NEW.Result='PENDING_ACH_VERIFICATION'
                                       THEN 'StepUp'
                                       HEN :OLD.Result='PENDING_ACH_VERIFICATION' AND :NEW.Result='PENDING_ACH_VALIDATION'
                                       THEN NULL
                                       --WHEN :OLD.Result='PENDING_ACH_VALIDATION' AND :NEW.Result ='ACTIVE' 
                                       --   THEN 'Pass' 
                                       --WHEN :OLD.Result='PENDING_ACH_VALIDATION' AND :NEW.Result ='FAILED' 
                                       --   THEN NULL
                                       END,
                                 :NEW.credit_account_id,
                                 2,
                                 nvl(bank_rec.id,bank_rec2.id),
                                 cs_bav_user_rec.ID
                                 )
                                 RETURNING bank_account_verification_id, verification_type
                                 INTO v_bank_account_verification_id, v_verification_type;
                                                    
                                                    
                    PKG_CUSTOMER_BANK_ACCOUNT.pr_update_cust_bank_account(:NEW.legacy_customer_id,
                                                        :NEW.routing_number,
                                                        :NEW.account_number,                                        
                                                        CASE WHEN :NEW.account_type='SAV' Then 2
                                                        WHEN :NEW.account_type='CHK' Then 1 END
                                                        );

                    PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_acct_hist(:NEW.legacy_customer_id,
                                                        3,
                                                        NULL,
                                                        :NEW.account_number,
                                                        :NEW.routing_number,
                                                        CASE WHEN :NEW.account_type='SAV' Then 2
                                                        WHEN :NEW.account_type='CHK' Then 1
                                                        END,
                                                        CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                                                        WHEN :NEW.channel='CAS' THEN 'CAS'
                                                        WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                                        WHEN :NEW.channel='WEB' Then 'Web'
                                                        END
                                                        );*/
          END IF;
                       
          --To handle when account type change happens from UCRM 
          IF (:NEW.operation_type='UPDATE' OR :NEW.operation_type='PRIMARY_UPDATE') 
          AND  :OLD.ROUTING_NUMBER = :NEW.ROUTING_NUMBER AND :OLD.ACCOUNT_NUMBER = :NEW.ACCOUNT_NUMBER  AND :OLD.ACCOUNT_TYPE <> :NEW.ACCOUNT_TYPE
          AND v_is_existing_account = TRUE AND  cs_existing_account_rec.ACCOUNT_TYPE_id <>  (CASE WHEN :New.ACCOUNT_TYPE='CHK' THEN 1
                                                                                                   WHEN :new.ACCOUNT_TYPE='SAV' THEN 2 END ) THEN 
                          
              -- Update the t_customer_bank_account table and child tables 
              BEGIN
                  SELECT customer_bank_account_id
                  INTO v_customer_bank_account_id
                  FROM account.t_customer_bank_account cba
                  WHERE cba.customer_id = :NEW.legacy_customer_id
                  AND cba.routing_number=:NEW.routing_number
                  AND cba.account_number=:NEW.ACCOUNT_NUMBER
                  AND cba.account_type_id=CASE WHEN :OLD.ACCOUNT_TYPE='CHK' THEN 1
                                  WHEN :OLD.ACCOUNT_TYPE='SAV' THEN 2 END;
              EXCEPTION
                  WHEN NO_DATA_FOUND THEN
                  RETURN;
              END;
                                  
              ods_stg.trig_ctx_pkg.set_from_trig_MBA;
        UPDATE account.t_customer_bank_account cba
              SET cba.ACCOUNT_TYPE_ID = CASE WHEN :NEW.ACCOUNT_TYPE='CHK' THEN 1 
                                             WHEN :NEW.ACCOUNT_TYPE='SAV' THEN 2
                                             END
              WHERE cba.customer_bank_account_id = v_customer_bank_account_id;  
        ods_stg.trig_ctx_pkg.clear_from_trig_MBA;
                                       
              INSERT INTO account.t_customer_bank_account_sync
                     (customer_bank_account_id,
                     customer_id,
                     change_type)
              VALUES
                     (v_customer_bank_account_id,
                     :NEW.legacy_customer_id,
                     0);

              PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_acct_hist(:NEW.legacy_customer_id,
                                        3,
                                        :NEW.account_number,
                                        :NEW.routing_number,
                                        CASE WHEN :NEW.account_type='SAV' Then 2
                                             WHEN :NEW.account_type='CHK' Then 1
                                        END,
                                        CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                                             WHEN :NEW.channel='CAS' THEN 'CAS'
                                             WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                             WHEN :NEW.channel='WEB' Then 'Web'
                                        END
                                        );              
                                
                                
              -- ADD WCH for bank account updated with new UC_ID, audit id and ref_num 
              SELECT fnbm_seq.NEXTVAL INTO v_uc_id FROM DUAL;
              SELECT fnbm_seq.NEXTVAL INTO v_uc_audit_id FROM DUAL;                                
                                
              -- Add use case 
              INSERT INTO t_uc
                     (status_id,
                     type_id,
                     ref_num,
                     open_date,
                     open_agent_id,
                     status_date,
                     status_agent_id,
                     uc_id,
                     credit_account_id,
                     card_id)
              VALUES
                     (1,
                     325470, -- ACH Registration
                     v_ref_num, 
                     :NEW.UPDATED_TIMESTAMP, --:NEW.CREATED_TIMESTAMP,
                     :NEW.UPDATED_BY, --:NEW.CREATED_BY,
                     :NEW.UPDATED_TIMESTAMP,
                     :NEW.UPDATED_BY,
                     v_uc_id,
                     :NEW.CREDIT_ACCOUNT_ID,
                     v_card_id);
                                
              OPEN cs_bank_name;
              FETCH cs_bank_name INTO cs_bank_name_rec;
              CLOSE cs_bank_name;
              
              -- Add audit Steps 
              INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
              VALUES  (v_uc_audit_id, 1,'USE CASE CREATED, STATUS OPEN', :NEW.UPDATED_TIMESTAMP,v_uc_id );  
              
              INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
              VALUES  (v_uc_audit_id, 1,'Bank Information Updated, '||cs_bank_name_rec.bank_name||', '||:NEW.Routing_Number||', '||:NEW.Account_Number||', '||:NEW.ACCOUNT_TYPE, :NEW.UPDATED_TIMESTAMP,v_uc_id );
              
              INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
              VALUES  (v_uc_audit_id, 1,'STATUS CHANGED TO RCLOSED', :NEW.UPDATED_TIMESTAMP,v_uc_id );              
          END IF;       

    IF :NEW.operation_type='DELETE'  THEN
      begin
        SELECT customer_bank_account_id INTO v_customer_bank_account_id
        FROM account.t_customer_bank_account
        WHERE customer_id=:NEW.Legacy_Customer_Id
        AND routing_number=:NEW.ROUTING_NUMBER
        AND account_number=:NEW.ACCOUNT_NUMBER
        AND account_type_id=CASE :old.ACCOUNT_TYPE WHEN 'CHK' THEN 1 ELSE 2 END;
              INSERT INTO dbproc.trigger_log(trigger_name,
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
               trigger_input)
        VALUES (
               'TRIG_INS_STG_MDRN_EXT_BNK_ACCT',
               'STAGE_ACH_MODERN_EXT_BANK_ACCOUNT',
               'STAGE_SEQUENCE_ID:' || TO_CHAR(:NEW.STAGE_SEQUENCE_ID),
               'MODERN',
               'LEGACY',
               'FAILED',
               'v_customer_bank_account_id:'|| to_char(v_customer_bank_account_id),
               0,
               'Before exception',
               SYSTIMESTAMP,
               '', 
               '', 
               'EXTERNAL_ACCOUNTS',
               ''
               );
       EXCEPTION
  WHEN NO_DATA_FOUND THEN
      INSERT INTO dbproc.trigger_log(trigger_name,
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
               trigger_input)
        VALUES (
               'TRIG_INS_STG_MDRN_EXT_BNK_ACCT',
               'STAGE_ACH_MODERN_EXT_BANK_ACCOUNT',
               'STAGE_SEQUENCE_ID:' || TO_CHAR(:NEW.STAGE_SEQUENCE_ID),
               'MODERN',
               'LEGACY',
               'FAILED',
               'v_customer_bank_account_id:'|| to_char(v_customer_bank_account_id),
               0,
               'TRIGGER',
               SYSTIMESTAMP,
               '', 
               '', 
               'EXTERNAL_ACCOUNTS',
               ''
               );
           end;
     
        IF v_customer_bank_account_id is not null THEN  
               ods_stg.trig_ctx_pkg.set_from_trig_MBA;                                
            PKG_CUSTOMER_BANK_ACCOUNT.pr_delete_cust_bank_account(v_customer_bank_account_id);
            ods_stg.trig_ctx_pkg.clear_from_trig_MBA;
            
            PKG_CUSTOMER_BANK_ACCOUNT.pr_insert_cust_bank_acct_hist(:NEW.legacy_customer_id,
                                      2,
                                      NULL,
                                      :NEW.account_number,
                                      :NEW.routing_number,
                                      CASE WHEN :NEW.account_type='SAV' Then 2
                                           WHEN :NEW.account_type='CHK' Then 1
                                      END,
                                      CASE WHEN :NEW.channel='AGENT' THEN 'CASH'
                                           WHEN :NEW.channel='CAS' THEN 'CAS'
                                           WHEN :NEW.channel='MOBILE' THEN 'Mobile_App'
                                           WHEN :NEW.channel='WEB' Then 'Web'
                                      END
                                      );

            -- ADD WCH for bank account deleted  
            -- Get new UC_ID, audit id and ref_num 
            SELECT fnbm_seq.NEXTVAL INTO v_uc_id FROM DUAL;
            SELECT fnbm_seq.NEXTVAL INTO v_uc_audit_id FROM DUAL;                          
                          
            -- Add use case 
            INSERT INTO t_uc
                   (status_id,
                   type_id,
                   ref_num,
                   open_date,
                   open_agent_id,
                   status_date,
                   status_agent_id,
                   uc_id,
                   credit_account_id,
                   card_id)
            VALUES
                   (1,
                   325470, -- ACH Registration
                   v_ref_num, 
                   :NEW.UPDATED_TIMESTAMP, --:NEW.CREATED_TIMESTAMP,
                   :NEW.UPDATED_BY, --:NEW.CREATED_BY,
                   :NEW.UPDATED_TIMESTAMP,
                   :NEW.UPDATED_BY,
                   v_uc_id,
                   :NEW.CREDIT_ACCOUNT_ID,
                   v_card_id);
                          
            OPEN cs_bank_name;
            FETCH cs_bank_name INTO cs_bank_name_rec;
            CLOSE cs_bank_name;
            
            -- Add audit Steps 
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
            VALUES  (v_uc_audit_id, 1,'USE CASE CREATED, STATUS OPEN', :NEW.UPDATED_TIMESTAMP,v_uc_id );  
            
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
            VALUES  (v_uc_audit_id, 1,'Bank Information Updated, '||cs_bank_name_rec.bank_name||', '||:NEW.Routing_Number||', '||:NEW.Account_Number||', '||:NEW.ACCOUNT_TYPE, :NEW.UPDATED_TIMESTAMP,v_uc_id );
            
            INSERT INTO t_uc_audit (uc_audit_id, agent_id, DATA, audit_date, uc_id)
            VALUES  (v_uc_audit_id, 1,'STATUS CHANGED TO RCLOSED', :NEW.UPDATED_TIMESTAMP,v_uc_id );                                                               
        END IF;               
    END IF;

    END IF;

EXCEPTION
WHEN OTHERS THEN
        INSERT INTO dbproc.trigger_log(trigger_name,
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
               trigger_input)
        VALUES (
               'TRIG_INS_STG_MDRN_EXT_BNK_ACCT',
               'STAGE_ACH_MODERN_EXT_BANK_ACCOUNT',
               'STAGE_SEQUENCE_ID:' || TO_CHAR(:NEW.STAGE_SEQUENCE_ID),
               'MODERN',
               'LEGACY',
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
-- RAISE_APPLICATION_ERROR(-20001,DBMS_UTILITY.format_error_stack
--|| DBMS_UTILITY.format_error_backtrace);                                
    
END TRIG_INS_STG_MDRN_EXT_BNK_ACCT;
