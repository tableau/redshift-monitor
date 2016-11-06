var pg = require('pg'); //postgress
var asynclib = require('async'); //helper library for async work
var starttime = new Date();


var showDebug = false;
// Metrics which aggregate last seven days or last day of data run longer and
// don't need to be collected hourly. Collect them once a day, at this hour UTC time
var hourToCollectLastSevenDaysMetrics = 15;



var redshiftConfig = {
    user: 'root',
    database: 'foo',
    password: 'bar',
    port: 5439,
    host: 'foo.bar.ap-southeast-1.redshift.amazonaws.com'
};

var pgConfig = {
    user: 'root',
    database: 'redshift-data',
    password: 'foo',
    port: 5432,
    host: 'foo.bar.us-east-1.rds.amazonaws.com'
};


var redshiftClient = new pg.Client(redshiftConfig);
var pgClient = new pg.Client(pgConfig);

// Queries to be executed by function to gather data from Redshift

// (Current) How many queries are being run right now?
var query = [{
    metric: "c_queriesRunning",
    sql: "select count(*) as current_Queries_Running from stv_recents where status = 'Running' and userid > 1;"
}];
// (Last Hour) What is the average query runtime over the last hour?
query.push({
    metric: "l_queryRuntime",
    sql: "select max(datediff(ms, starttime, endtime)) / 1000.0 as lastHour_maxQueryExecutionTime_sec, avg(datediff(ms, starttime, endtime)) / 1000.0 as lastHour_AvgQueryExecutionTime_sec from stl_query where starttime >= GETDATE() - INTERVAL '1 hour' and userid <>1"
});
// (Last Hour) What is the total queue time for all queries which have waited in any WLM queue before executinng over the last hour?
query.push({
    metric: "l_WLMwaittime",
    sql: "SELECT SUM(w.total_queue_time) / 1000000.0 as LastHour_WLM_QueueTime FROM stl_wlm_query w WHERE w.queue_start_time >= GETDATE() - INTERVAL '1 hour' AND w.total_queue_time > 0"
});
// (Current) Are our queries waiting because they can't lock resources they need to answer a question?
query.push({
    metric: "c_ungrantedLocks",
    sql: "select count(*) as current_ungranted_locks_on_resources from svv_transactions t WHERE t.granted = 'f' and t.pid != pg_backend_pid()"
});
// (Current) Is a vacuum (expensive!) going on right now?
query.push({
    metric: "c_vacuum",
    sql: "select case when count(*)  > 0 then 1 else 0 end as current_is_vacuum_in_process from svv_transactions where xid = (select max(xid) from stl_vacuum)"
});
// (Current) Current number of transactions
query.push({
        metric: "c_currentTransactions",
        sql: "select count(*) as current_transaction_count from svv_transactions t WHERE t.lockable_object_type = 'transactionid' and pid != pg_backend_pid()"
    })
    // (Last Hour) In the last hour, has here been lots of cross-node broadcasting going on while queries are being executed?
query.push({
    metric: "l_broadcasting",
    sql: "select sum(total) as LastHour_internal_broadcasting from (select count(query) total from stl_dist where starttime >= GETDATE() - INTERVAL '1 hour' group by query having sum(packets) > 1000000)"
});
// (Last Hour) How many performance alerts related to query execution were issued by Redshift during the last hour?
query.push({
    metric: "l_perfalerts",
    sql: "select count(distinct l.query) as LastHour_perf_alert_count from stl_alert_event_log as l where l.userid > 1 and l.event_time >= GETDATE() - INTERVAL '1 hour'"
});
// (Last Hour) In the last hour, how many queries had to spill to disk during execution vs. staying completely in RAM?
query.push({
    metric: "l_diskspill",
    sql: "SELECT count(distinct query) LastHour_queries_spilled_to_disk_count FROM svl_query_report WHERE is_diskbased='t' AND (LABEL LIKE 'hash%%' OR LABEL LIKE 'sort%%' OR LABEL LIKE 'aggr%%') AND userid > 1 AND start_time >= GETDATE() - INTERVAL '1 hour'"
});
// (Current) Tables currently missing statistics
query.push({
    metric: "c_missingStats",
    sql: "SELECT substring(trim(plannode),1,100) AS missingstats_plan_node FROM stl_explain WHERE plannode LIKE '%missing statistics%' AND plannode NOT LIKE '%redshift_auto_health_check_%' GROUP BY plannode ORDER BY 1 DESC;"
});
// (Last Hour) On a per-queue basis, what was the total wait time for queries in a WLM queue before they begin executing over the last hour?
query.push({
    metric: "l_WLMwaittime_per_queue",
    sql: "SELECT s.service_class as wlm_queue_id, SUM(w.total_queue_time) / 1000000.0 as lastHour_wlm_total_queuetime_by_queue FROM stl_wlm_query w left join STV_WLM_SERVICE_CLASS_CONFIG s ON w.service_class = s.service_class WHERE w.queue_start_time >= GETDATE() - INTERVAL '1 hour' AND w.total_queue_time > 0 AND s.service_class is null or s.service_class> 4 group by s.service_class;"
});
// (Last Hour) On a per-queue basis, what was the total wait time for queries in a WLM queue before they begin executing over the last hour
query.push({
    metric: "l_wlm_diskspill",
    sql: "SELECT  w.service_class wlm_queue_id, count(distinct r.query) lashour_wlm_queries_spilled_to_disk_count FROM svl_query_report r INNER JOIN stl_wlm_query w ON r.query = w.query WHERE r.is_diskbased='t' AND (r.LABEL LIKE 'hash%%' OR r.LABEL LIKE 'sort%%' OR r.LABEL LIKE 'aggr%%') AND r.userid > 1 AND r.start_time >= GETDATE() - INTERVAL '1 hour' group by w.service_class;"	
});
// (Last Hour) On a per-queue basis, what was the average and max query execution time over the last hour
query.push({
    metric: "l_wlm_queryRuntime",
    sql: "select service_class as wlm_queue_id , max(datediff(ms, queue_start_time, exec_end_time)) / 1000.0 as lasthour_wlm_maxqueryexecutiontime_sec, avg(datediff(ms, queue_start_time, exec_end_time)) / 1000.0 as lastHour_wlm_avgqueryexecutiontime_sec from stl_wlm_query  where queue_start_time >= GETDATE() - INTERVAL '1 hour' and userid <>1 GROUP BY service_class;"
});
// (Last Hour) On a per-queue basis, how many queries executed over the last hour
query.push({
    metric: "l_wlm_queriesRunning",
    sql: "select service_class as wlm_queue_id , count (distinct query) as lastHour_wlm_query_count FROM stl_wlm_query  where queue_start_time >= GETDATE() - INTERVAL '1 hour' and userid <>1 GROUP BY service_class;"
});

//  Only execute these once a day
if (hourToCollectLastSevenDaysMetrics == starttime.getHours()) {
    // (Last Seven Days) List Performance Alerts for the last week
    query.push({
        metric: "ls_listPerformanceAlerts",
        sql: "select coalesce(trim(s.perm_table_name), 'NA') as alerted_table , coalesce(sum(coalesce(b.rows,d.rows,s.rows)), 0) as alerted_rows, trim(split_part(l.event,':',1)) as alerted_event,  substring(trim(l.solution),1,60) as solution , max(l.query) as alerted_sample_query from stl_alert_event_log as l left join stl_scan as s on s.query = l.query and s.slice = l.slice and s.segment = l.segment left join stl_dist as d on d.query = l.query and d.slice = l.slice and d.segment = l.segment left join stl_bcast as b on b.query = l.query and b.slice = l.slice and b.segment = l.segment where l.userid >1 and  l.event_time >=  dateadd(day, -7, current_Date) group by 1,3,4 order by 2 desc,5 desc;"
    });
    // (Last Seven Days) Enumerate all filters (WHERE Clauses) over the past 7 days. These might make good targets for sortkeys
    query.push({
        metric: "ls_filters",
        sql: "select trim(s.perm_Table_name) as filters_table_name , substring(trim(info),1,100) as filters_filter, sum(datediff(seconds,starttime,case when starttime > endtime then starttime else endtime end)) as filters_secs, count(distinct i.query) as filters_num, max(i.query) as filters_query from stl_explain p join stl_plan_info i on ( i.userid=p.userid and i.query=p.query and i.nodeid=p.nodeid  ) join stl_scan s on (s.userid=i.userid and s.query=i.query and s.segment=i.segment and s.step=i.step) where s.starttime > dateadd(day, -7, current_Date) and s.perm_table_name not like 'Internal Worktable%' and p.info like 'Filter:%'  and p.nodeid > 0 and s.perm_table_name like '%' group by 1,2 order by 1, 3 desc , 4 desc;"
    });
    // (Last Seven Days) What are 50 longest running queries in the past 7 days?
    query.push({
        metric: "ls_fiftyLongestRunning",
        sql: "select trim(database) as longest_database_name, count(query) as longest_query_count, max(substring (qrytext,1,80)) as longest_query_text, min(run_seconds) as longest_min_execution_time , max(run_seconds) as longest_max_execution_time, avg(run_seconds) as longest_average_execution_time, sum(run_seconds) as longest_total_execution_time,  max(query) as longest_max_query_id, max(starttime)::datetime as longest_last_run_date,aborted as longest_aborted, event as longest_event from (select userid, label, stl_query.query, trim(database) as database, trim(querytxt) as qrytext, md5(trim(querytxt)) as qry_md5, starttime, endtime, datediff(seconds, starttime,endtime)::numeric(12,2) as run_seconds, aborted, decode(alrt.event,'Very selective query filter','Filter','Scanned a large number of deleted rows','Deleted','Nested Loop Join in the query plan','Nested Loop','Distributed a large number of rows across the network','Distributed','Broadcasted a large number of rows across the network','Broadcast','Missing query planner statistics','Stats',alrt.event) as event from stl_query left outer join ( select query, trim(split_part(event,':',1)) as event from STL_ALERT_EVENT_LOG where event_time >=  dateadd(day, -7, current_Date)  group by query, trim(split_part(event,':',1)) ) as alrt on alrt.query = stl_query.query where userid <> 1 and starttime >=  dateadd(day, -7, current_Date)) group by database, label, qry_md5, aborted, event order by longest_total_execution_time desc limit 50"
    });
    // (Last Day) Show current skew for tables in the database named in the Redshift conection string
    query.push({
        metric: "d_tableSkew",
        sql: "SELECT SCHEMA skew_schemaname, \"table\" as skew_tablename, table_id skew_tableid, size skew_size_in_mb,  CASE  WHEN diststyle NOT IN ('EVEN','ALL') THEN 1 ELSE 0 END skew_has_dist_key,  CASE WHEN sortkey1 IS NOT NULL THEN 1 ELSE 0 END skew_has_sort_key, CASE WHEN encoded = 'Y' THEN 1 ELSE 0  END skew_has_col_encoding,  CAST(max_blocks_per_slice - min_blocks_per_slice AS FLOAT) / GREATEST(NVL (min_blocks_per_slice,0)::int,1) skew_ratio_across_slices,  CAST(100*dist_slice AS FLOAT) /(SELECT COUNT(DISTINCT slice) FROM stv_slices) skew_pct_slices_populated FROM svv_table_info ti  JOIN (SELECT tbl, MIN(c) min_blocks_per_slice,  MAX(c) max_blocks_per_slice,COUNT(DISTINCT slice) dist_slice  FROM (SELECT b.tbl,  b.slice, COUNT(*) AS c FROM STV_BLOCKLIST b GROUP BY b.tbl,b.slice)  WHERE tbl IN (SELECT table_id FROM svv_table_info)GROUP BY tbl) iq ON iq.tbl = ti.table_id"
      });
    // (Last Day) Capture all Vacuum events in the last day
    query.push({
        metric: "d_vaccumEvents",
        sql: "select v.elapsed_time / 1000000.00 as vacuum_elapsedtime_sec, v.table_name as vacuum_table, v.xid as vacuum_xid,  min(q.starttime) as vacuum_starttime ,  max(q.endtime) as vacuum_endtime   FROM svv_vacuum_summary v INNER JOIN stl_query q ON v.xid = q.xid WHERE q.starttime >= GETDATE() - INTERVAL '1 day'   GROUP BY 1,2,3"
      });
    }

function executeSelectQuery(clientInfo, query) {
    var queryResult = [];
    var dataClient = new pg.Client(clientInfo);
    // Using a single connection, fire a set of queries.
    dataClient.connect(function(err) {
        if (err) {
            console.log("Unable to connect to Redshift", err, "Exiting.")
            process.exit();
        }
        console.log('Reading from Redshift');
        asynclib.each(query, function(i, callback) {


                dataClient.query(i.sql, function(err, result) {
                    if (err) {
                        console.log("Error executing SELECT statement: ", i.sql, err);
                    }

                    // Use the same timestamp for ALL rows retrieved in the same query result. This'll make it easier
                    // for users to correlate data later
                    var ts = new Date().toISOString().slice(0, 19).replace('T', ' ');
                    // Generate a GUID to which can be used to group multiple rows together later... Not sure
                    // if I'll need this.
                    myGuid = notReallyaGuid();

                    // Add a 'metric' property, ts, guid
                    queryResult.push({
                        metric: i.metric
                    });
                    queryResult[queryResult.length - 1].ts = ts;
                    queryResult[queryResult.length - 1].guid = myGuid;
                    // Add a 'result' property which is an array of result rows
                    queryResult[queryResult.length - 1].result = result.rows;
                    if (showDebug) console.log(JSON.stringify(queryResult));
                    callback();
                });
            },
            // callback
            function(err) {
                if (err) throw err;
                // cleanup
                console.log('Closing Redshift connection...');
                dataClient.end(function(err) {
                    if (err) {
                        console.log("Error closing connection to Redshift", err);
                    }
                    console.log('Closed');
                    console.log(queryResult.length, 'resultsets gathered.');
                    executeInsert(pgConfig, queryResult);
                });
            });
    });
}

function executeInsert(clientInfo, metrics) {

    var redshiftMetrics = metrics
    var dataClient = new pg.Client(clientInfo);
    var sqlINSERTs = []; // Array which will hold INSERT statements we're about to generate

    // Cycle thru each resultset building up a collection of INSERTs
    for (var i in redshiftMetrics) {
        if (redshiftMetrics.hasOwnProperty(i)) {
            var resultSet = redshiftMetrics[i];
            if (showDebug) console.log(resultSet.metric) //Which metric is this?
                // Deal with each row of this resultSet
            for (var i in resultSet.result) {
                if (resultSet.result.hasOwnProperty(i)) {
                    var row = resultSet.result[i];
                    if (showDebug) console.log(JSON.stringify(row));
                    // Start building your INSERT statement
                    var insert = "INSERT INTO redshift_results (guid, timestamp ";
                    var values = " VALUES('" + resultSet.guid + "','" + resultSet.ts + "' ";
                    //Populate the column and VALUES portions by grabbing each key/value in your row...
                    Object.keys(row).forEach(function(key) {
                        //  console.log(key + ': ' + row[key]);
                        insert = insert + ", " + key;
                        //  Feh, the pg module appears to return all results as a string.
                        //  Looks like I need to test to see if it is a number...

                        var test = filterFloat(parseFloat(row[key]));
                        if (isNaN(test) == true) {
                            if (row[key] === null) {
                                values = values + ", 0 ";
                            } else {
                                //Strip:
                                //  Timzeone related stuff if it exists. It'll break the INSERT
                                //  Single quotes, replace with doubles.
                                values = values + ", '" + row[key].toString().replace(/ GMT.*/gi, "").replace(/\'/gi, "\"") + "' ";
                            }
                        } else {
                            values = values + "," + test;
                        }

                    });



                    sql = insert + ") " + values + ")";
                    if (showDebug) console.log(sql);
                    sqlINSERTs.push(sql);
                }
            }
        }
    }

    // Execute each INSERT

    // Using a single connection, insert a bunch of rows.
    console.log("Opening connection to pgsql")
    pgClient.connect(function(err) {
            if (err) {
                console.log("Unable to connect to pgsql", err, "Exiting.")
                process.exit();
                }
                console.log("Inserting", sqlINSERTs.length, "rows");
                asynclib.each(sqlINSERTs, function(i, callback)
                    //  iteratee
                    {
                        pgClient.query(i, function(err, result) {
                            if (err) {
                                console.log("Error executing statement", i, err);
                            }
                            callback();
                        });

                    },
                    //callback
                    function(err) {
                        if (err) {
                            console.log("Unhandled Exception has fallen to INSERT Callback", err);
                        }
                        // cleanup
                        console.log('Closing pgsql connection...');
                        pgClient.end(function(err) {
                            if (err) {
                                console.log("Error closing connection to pgsql", err);
                            }
                            console.log('Closed');
                            finishtime = Date();
                            console.log("Began", starttime, "Finished", finishtime)
                        });
                    });
            });


    }

    //*********************Helpers************/

    var filterFloat = function(value) {
        if (/^(\-|\+)?([0-9]+(\.[0-9]+)?|Infinity)$/
            .test(value))
            return Number(value);
        return NaN;
    }

    // http://stackoverflow.com/questions/105034/create-guid-uuid-in-javascript
    function notReallyaGuid() {
        function s4() {
            return Math.floor((1 + Math.random()) * 0x10000)
                .toString(16)
                .substring(1);
        }
        return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
            s4() + '-' + s4() + s4() + s4();
    }

    // Get keys of an object
    function getKeys(obj) {
        var keys = [];
        for (var key in obj) {
            keys.push(key);

        }
        return keys;
    }



    executeSelectQuery(redshiftConfig, query);
