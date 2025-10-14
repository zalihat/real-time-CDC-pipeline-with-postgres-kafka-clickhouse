-- create test database

CREATE TABLE public.sales_test (
  id SERIAL PRIMARY KEY,
  customer_name TEXT,
  item TEXT,
  amount NUMERIC(10,2),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
-- Include the entire old row in the WAL for updates and deletes

ALTER TABLE public.sales_test REPLICA IDENTITY FULL;


-- create audit schema in the selected database and a table for tracking changes
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.data_changes (
  audit_id     bigserial PRIMARY KEY,
  table_name   text NOT NULL,
  record_pk    text,
  op_type      text NOT NULL,
  changed_by   text,
  app_name     text,
  client_ip    inet,
  changed_at   timestamptz DEFAULT now(),
  before_row   jsonb,
  after_row    jsonb,
  diff         jsonb
);

-- create the function that logs changes
CREATE OR REPLACE FUNCTION audit.log_row_change() RETURNS trigger AS $$
DECLARE
  pk_val text;
  before_json jsonb;
  after_json jsonb;
  computed_diff jsonb := '{}'::jsonb;
BEGIN
  IF TG_OP = 'DELETE' THEN
    before_json := row_to_json(OLD)::jsonb;
    after_json := NULL;
    pk_val := OLD.id::text;
  ELSIF TG_OP = 'INSERT' THEN
    before_json := NULL;
    after_json := row_to_json(NEW)::jsonb;
    pk_val := NEW.id::text;
  ELSE
    before_json := row_to_json(OLD)::jsonb;
    after_json := row_to_json(NEW)::jsonb;
    pk_val := NEW.id::text;
  END IF;

  IF before_json IS NOT NULL AND after_json IS NOT NULL THEN
    computed_diff := (
      SELECT jsonb_object_agg(k, jsonb_build_object('old', before_json->k, 'new', after_json->k))
      FROM (
        SELECT jsonb_object_keys(before_json) AS k
        UNION
        SELECT jsonb_object_keys(after_json) AS k
      ) s
      WHERE (before_json->s.k) IS DISTINCT FROM (after_json->s.k)
    );
  END IF;

  INSERT INTO audit.data_changes(
    table_name, record_pk, op_type, changed_by, app_name, client_ip, before_row, after_row, diff
  )
  VALUES (
    TG_TABLE_NAME,
    pk_val,
    TG_OP,
    current_user,
    current_setting('application_name', true),
    inet_client_addr(),
    before_json,
    after_json,
    computed_diff
  );

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--create a trigger
CREATE TRIGGER trg_audit_sales_test
AFTER INSERT OR UPDATE OR DELETE
ON public.sales_test
FOR EACH ROW EXECUTE FUNCTION audit.log_row_change();

--insert a new record into the test table
INSERT INTO public.sales_test (customer_name, item, amount)
VALUES ('Alice', 'Laptop', 1200.00);

--update record in the table
UPDATE public.sales_test
SET amount = 1800.00
WHERE customer_name = 'Alice';

-- check audit_logs.data_changes table to verify changes log
SELECT 
  * 
FROM audit.data_changes