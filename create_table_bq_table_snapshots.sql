CREATE TABLE IF NOT EXISTS
  bq_storage_costs.bq_table_snapshots ( 
    observed_at TIMESTAMP,
    region STRING,
    gcp_project STRING,
    dataset STRING,
    table_name STRING,
    active_logical_gib FLOAT64,
    long_term_logical_gib FLOAT64,
    active_physical_gib FLOAT64,
    long_term_physical_gib FLOAT64,
    time_travel_physical_gib FLOAT64,
    fail_safe_physical_gib FLOAT64,
    active_no_tt_physical_gib FLOAT64,
    projected_monthly_cost FLOAT64,
    expiration_timestamp STRING,
    partition_expiration_days STRING,
    storage_billing_model STRING,
    last_accessed DATE,
    labels_array_string STRING )
    PARTITION BY TIMESTAMP_TRUNC(observed_at, DAY)
