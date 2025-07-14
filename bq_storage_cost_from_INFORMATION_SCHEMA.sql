  -- The costs of logical and physical storage.    This is for US multiregion
DECLARE
  active_logical_gib_price FLOAT64 DEFAULT 0.02;
DECLARE
  long_term_logical_gib_price FLOAT64 DEFAULT 0.01;
DECLARE
  active_physical_gib_price FLOAT64 DEFAULT 0.04;
DECLARE
  long_term_physical_gib_price FLOAT64 DEFAULT 0.02;

INSERT INTO bq_storage_costs.bq_table_snapshots (
    observed_at,
    region,
    gcp_project,
    dataset,
    table_name,
    active_logical_gib,
    long_term_logical_gib,
    active_physical_gib,
    long_term_physical_gib,
    time_travel_physical_gib,
    fail_safe_physical_gib,
    active_no_tt_physical_gib,
    projected_monthly_cost,
    expiration_timestamp,
    partition_expiration_days,
    storage_billing_model,
    last_accessed,
    labels_array_string)

WITH

  -- this gets the billing model for a dataset. A dataset has a billing model, either logical or physical.
  billing_model AS (
  SELECT
    catalog_name,
    schema_name,
    option_value AS storage_billing_model
  FROM
    INFORMATION_SCHEMA.SCHEMATA_OPTIONS
  WHERE
    option_name = 'storage_billing_model' ),
  -- this gets the expiration policies set on all the tables, and shapes it so it can be joined by the dataset and table. it could do something smarter than STRING_AGG
  table_expirations AS (
  SELECT
    *
  FROM (
    SELECT
      table_catalog,
      table_schema,
      table_name,
      option_name,
      option_value
    FROM
      region-US.INFORMATION_SCHEMA.TABLE_OPTIONS)
  PIVOT
    (STRING_AGG(option_value) FOR option_name IN ('expiration_timestamp',
        'partition_expiration_days')) ),
  -- this gets the labels on the tables. The column option_value in TABLE_OPTIONS is a string representation of an array of structs.
  table_labels AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    option_value AS labels_array_string
  FROM
    region-US.INFORMATION_SCHEMA.TABLE_OPTIONS
  WHERE
    option_name = "labels" ),
  -- this gets the queries reading from the tables and allows them to be joined by dataset and table they reference, to give last access time for the table.
  usage AS (
  SELECT
    t.project_id,
    t.dataset_id,
    t.table_id,
    DATE(MAX(end_time)) AS last_accessed
  FROM
    region-US.INFORMATION_SCHEMA.JOBS,
    UNNEST(referenced_tables) AS t
  GROUP BY
    t.project_id,
    t.dataset_id,
    t.table_id ),
  -- this gets the sizes of the different kinds of storage usage that we can be billed for
  storage AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    -- Logical
  IF
    (deleted=FALSE, active_logical_bytes, 0) / POWER(1024, 3) AS active_logical_gib,
  IF
    (deleted=FALSE, long_term_logical_bytes, 0) / POWER(1024, 3) AS long_term_logical_gib,
    -- Physical
    active_physical_bytes / POWER(1024, 3) AS active_physical_gib,
    (active_physical_bytes - time_travel_physical_bytes) / POWER(1024, 3) AS active_no_tt_physical_gib,
    long_term_physical_bytes / POWER(1024, 3) AS long_term_physical_gib,
    -- Restorable previously deleted physical
    time_travel_physical_bytes / POWER(1024, 3) AS time_travel_physical_gib,
    fail_safe_physical_bytes / POWER(1024, 3) AS fail_safe_physical_gib,
  FROM
    region-US.INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
  WHERE
    total_physical_bytes > 0
    AND table_type = "BASE TABLE" ),
  -- This converts the sizes into costs.   this is the projected monthly cost for the table, based on the the snapshot of table storage usage.
  --  Cost is calculated under both billing model options (logical or physical).  the bill is based on what billing option is set for the dataset
  storage_costs AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
    active_logical_gib,
    long_term_logical_gib,
    active_physical_gib,
    active_no_tt_physical_gib,
    long_term_physical_gib,
    time_travel_physical_gib,
    fail_safe_physical_gib,
    -- Compression ratio
    ROUND(SAFE_DIVIDE(active_logical_gib, active_no_tt_physical_gib), 2) AS active_compression_ratio,
    ROUND(SAFE_DIVIDE(long_term_logical_gib, long_term_physical_gib), 2) AS long_term_compression_ratio,
    -- Projected monthly costs logical
    ROUND(active_logical_gib * active_logical_gib_price, 2) AS projected_active_logical_cost,
    ROUND(long_term_logical_gib * long_term_logical_gib_price, 2) AS projected_long_term_logical_cost,
    -- Projected monthly costs physical
    ROUND((active_no_tt_physical_gib + time_travel_physical_gib + fail_safe_physical_gib) * active_physical_gib_price, 2) AS projected_active_physical_cost,
    ROUND(long_term_physical_gib * long_term_physical_gib_price, 2) AS projected_long_term_physical_cost,
  FROM
    storage ) 
  -- This is the join of all the above
SELECT
  -- timestamp of the snapshot so we can build up a timeseries for trend reporting
  CURRENT_TIMESTAMP AS observed_at,
  "US" as region,
  -- change the nomenclature from the INFORMATION_SCHEMA names to more common names that billing admins and BQ users will be familiar with
  storage_costs.table_catalog AS gcp_project,
  storage_costs.table_schema AS dataset,
  storage_costs.table_name,
  -- Logical
  ROUND(active_logical_gib, 2) AS active_logical_gib,
  ROUND(long_term_logical_gib, 2) AS long_term_logical_gib,
  -- Physical
  ROUND(active_physical_gib, 2) AS active_physical_gib,
  ROUND(long_term_physical_gib, 2) AS long_term_physical_gib,
  ROUND(time_travel_physical_gib, 2) AS time_travel_physical_gib,
  ROUND(fail_safe_physical_gib, 2) AS fail_safe_physical_gib,
  ROUND(active_no_tt_physical_gib, 2) AS active_no_tt_physical_gib,
  -- The cost is based on billing model selected, either PHYSICAL or LOGICAL
IF
  (storage_billing_model = 'PHYSICAL', projected_active_physical_cost + projected_long_term_physical_cost, projected_active_logical_cost+projected_long_term_logical_cost) AS projected_monthly_cost,
  table_expirations.expiration_timestamp,
  table_expirations.partition_expiration_days,
  billing_model.storage_billing_model,
  usage.last_accessed,
  table_labels.labels_array_string
FROM
  storage_costs
LEFT OUTER JOIN
  table_expirations
ON
  ((storage_costs.table_catalog =table_expirations.table_catalog)
    AND (storage_costs.table_schema = table_expirations.table_schema)
    AND (storage_costs.table_name = table_expirations.table_name))
LEFT OUTER JOIN
  billing_model
ON
  ((storage_costs.table_catalog = billing_model.catalog_name)
    AND (storage_costs.table_schema = billing_model.schema_name))
LEFT OUTER JOIN
  usage
ON
  ((storage_costs.table_catalog =usage.project_id )
    AND (storage_costs.table_schema =usage.dataset_id )
    AND (storage_costs.table_name = usage.table_id))
LEFT OUTER JOIN
  table_labels
ON
  ((storage_costs.table_catalog = table_labels.table_catalog)
    AND (storage_costs.table_schema = table_labels.table_schema)
    AND (storage_costs.table_name = table_labels.table_name))
ORDER BY
  projected_monthly_cost DESC
