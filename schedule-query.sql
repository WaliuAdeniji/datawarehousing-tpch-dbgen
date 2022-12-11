with sales_checks as
(
    SELECT COUNT(s_orderkey_linenumber) AS s_orderkey_linenumber_count,
      COUNT(DISTINCT(CASE WHEN s_orderkey_linenumber is null THEN s_orderkey_linenumber END)) AS s_orderkey_linenumber_null
      FROM tpch-dbgen-367914.tpch_dbgen_367914_all_data.fact_sales 
)
, customer_checks as(
      SELECT COUNT(c_custkey) AS c_custkey_count,
        COUNT(DISTINCT c_custkey) AS c_custkey_unique,
        COUNT(DISTINCT(CASE WHEN c_custkey is null THEN c_custkey END)) AS c_custkey_null
        FROM tpch-dbgen-367914.tpch_dbgen_367914_all_data.dim_customer    
)
, supplier_checks as (
    SELECT COUNT(supp_suppkey) AS supp_suppkey_count,
      COUNT(DISTINCT supp_suppkey) AS supp_suppkey_unique,
      COUNT(DISTINCT(CASE WHEN supp_suppkey is null THEN supp_suppkey END)) AS supp_suppkey_null
      FROM tpch-dbgen-367914.tpch_dbgen_367914_all_data.dim_supplier 
)
, part_checks as (
    SELECT COUNT(p_partkey_suppkey) AS p_partkey_suppkey_count,
      COUNT(DISTINCT(CASE WHEN p_partkey_suppkey is null THEN p_partkey_suppkey END)) AS p_partkey_suppkey_null
      FROM  tpch-dbgen-367914.tpch_dbgen_367914_all_data.dim_part 
)
, row_conditions as (
    select if(s_orderkey_linenumber_count = 0,'Sales data missing; ', NULL)  as alerts from sales_checks
union all
    select if(s_orderkey_linenumber_null != 0, cast(s_orderkey_linenumber_null as string )||' NULL sales primary key found', NULL) from sales_checks
union all
    select if(c_custkey_count = 0,'Customer data missing; ', NULL) from customer_checks
union all
    select if(c_custkey_count != c_custkey_unique,'Duplicate customer primary key found; ', NULL) from customer_checks
union all
    select if(c_custkey_null != 0, cast(c_custkey_null as string )||' NULL customer primary key found', NULL) from customer_checks
union all
    select if(supp_suppkey_count = 0,'Supplier data missing; ', NULL) from supplier_checks
union all
    select if(supp_suppkey_count != supp_suppkey_unique,'Duplicate supplier primary key found; ', NULL) from supplier_checks
union all
    select if(supp_suppkey_null != 0, cast(supp_suppkey_null as string )||' NULL supplier primary key found', NULL) from supplier_checks
union all
    select if(p_partkey_suppkey_count = 0,'Part data missing; ', NULL) from part_checks
union all
    select if(p_partkey_suppkey_null != 0, cast(p_partkey_suppkey_null as string )||' NULL part primary key found', NULL) from part_checks

)

, alerts as (
select
        array_to_string(
            array_agg(alerts IGNORE NULLS) 
        ,'.; ')                                         as stringify_alert_list
    
    ,   array_length(array_agg(alerts IGNORE NULLS))     as issues_found
from
    row_conditions
)

select
    alerts.issues_found,
    if(alerts.issues_found is null, 'all good'
        , ERROR(FORMAT('ATTENTION: tpch-dbgen-367914.tpch_dbgen_367914_all_data. has potential data quality issues for yesterday: %t. for more info.'
        , stringify_alert_list)))
from
    alerts
;