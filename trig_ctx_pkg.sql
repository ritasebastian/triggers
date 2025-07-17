CREATE OR REPLACE PACKAGE ods_stg.trig_ctx_pkg IS 
       PROCEDURE set_from_trig_MBA;
       FUNCTION is_from_trig_MBA RETURN BOOLEAN;
       PROCEDURE clear_from_trig_MBA;
END;
/
CREATE OR REPLACE PACKAGE BODY ods_stg.trig_ctx_pkg IS 
--------------------------------------------------------
-- Procedure to set the flag  --
--------------------------------------------------------
PROCEDURE set_from_trig_MBA IS 
  BEGIN 
    DBMS_SESSION.set_context ('ods_stg_trig_ctx', 'from_MBA_Trig','Y');
  END;
--------------------------------------------------------
-- Function to check  --
--------------------------------------------------------
FUNCTION is_from_trig_MBA RETURN BOOLEAN IS 
  BEGIN 
    RETURN SYS_CONTEXT ('ods_stg_trig_ctx','from_MBA_Trig') = 'Y';
  END;
--------------------------------------------------------
-- Procedure to Clear flag --
--------------------------------------------------------
PROCEDURE clear_from_trig_MBA IS 
  BEGIN
    Dbms_Session.clear_context('ods_stg_trig_ctx','from_MBA_Trig');
  END;
END;
/