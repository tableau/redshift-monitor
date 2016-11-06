-- Table: public.redshift_results

-- DROP TABLE public.redshift_results;

CREATE TABLE public.redshift_results
(
    id integer NOT NULL DEFAULT nextval('test_id'::regclass),
    guid uuid NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    current_is_vacuum_in_process integer,
    current_queries_running integer,
    current_transaction_count integer,
    current_ungranted_locks_on_resources integer,
    lasthour_internal_broadcasting integer,
    lasthour_perf_alert_count integer,
    lasthour_queries_spilled_to_disk_count integer,
    lasthour_avgqueryexecutiontime_sec double precision,
    lasthour_userqueryexecutiontime_sec double precision,
    lasthour_wlm_queuetime double precision,
    lasthour_maxqueryexecutiontime_sec double precision,
    longest_last_run_date timestamp without time zone,
    longest_max_execution_time numeric,
    longest_max_query_id integer,
    longest_min_execution_time numeric,
    longest_query_count integer,
    longest_query_text character varying(100) COLLATE "default".pg_catalog,
    longest_aborted integer,
    longest_average_execution_time numeric,
    longest_total_execution_time numeric,
    longest_database_name character varying(100) COLLATE "default".pg_catalog,
    longest_event character varying(100) COLLATE "default".pg_catalog,
    solution character varying(100) COLLATE "default".pg_catalog,
    filters_table_name character varying(100) COLLATE "default".pg_catalog,
    filters_filter character varying(100) COLLATE "default".pg_catalog,
    filters_secs integer,
    filters_num integer,
    filters_query integer,
    missingstats_plan_node character varying(100) COLLATE "default".pg_catalog,
    skew_schemaname character varying(100) COLLATE "default".pg_catalog,
    skew_tablename character varying(100) COLLATE "default".pg_catalog,
    skew_tableid integer,
    skew_size_in_mb integer,
    skew_has_dist_key integer,
    skew_has_sort_key integer,
    skew_has_col_encoding integer,
    skew_ratio_across_slices double precision,
    skew_pct_slices_populated integer,
    alerted_table character varying(100) COLLATE "default".pg_catalog,
    alerted_event character varying(100) COLLATE "default".pg_catalog,
    alerted_rows integer,
    alerted_sample_query character varying(100) COLLATE "default".pg_catalog,
    vacuum_elapsedtime_sec double precision,
    vacuum_table character varying(100) COLLATE "default".pg_catalog,
    vacuum_starttime timestamp without time zone,
    vacuum_endtime timestamp without time zone,
    vacuum_xid integer,
    lasthour_wlm_total_queuetime_by_queue double precision DEFAULT 0,
    wlm_queue_id integer,
    lashour_wlm_queries_spilled_to_disk_count integer DEFAULT 0,
    lasthour_wlm_maxqueryexecutiontime_sec double precision,
    lasthour_wlm_avgqueryexecutiontime_sec double precision,
    lasthour_wlm_query_count integer,
    CONSTRAINT redshift_results_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.redshift_results
    OWNER to someUser;

-- Index: redshift_results_idx

-- DROP INDEX public.redshift_results_idx;

CREATE INDEX redshift_results_idx
    ON public.redshift_results USING btree
    (timestamp)
    TABLESPACE pg_default;

-- Index: redshift_results_idx2

-- DROP INDEX public.redshift_results_idx2;

CREATE INDEX redshift_results_idx2
    ON public.redshift_results USING btree
    (guid)
    TABLESPACE pg_default;