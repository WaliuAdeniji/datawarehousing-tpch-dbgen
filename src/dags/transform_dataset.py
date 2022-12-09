import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import avg, when, year, date_format, to_date, concat

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

customer_schema = types.StructType(
    [
        types.StructField("c_custkey", types.IntegerType(), True),
        types.StructField("c_name", types.StringType(), True),
        types.StructField("c_address", types.StringType(), True),
        types.StructField("c_nationkey", types.IntegerType(), True),
        types.StructField("c_phone", types.StringType(), True),
        types.StructField("c_acctbal", types.DoubleType(), True),
        types.StructField("c_mktsegment", types.StringType(), True),
        types.StructField("c_comment", types.StringType(), True),
        types.StructField("c_Unnamed", types.DoubleType(), True),
    ]
)

supplier_schema = types.StructType(
    [
        types.StructField("s_suppkey", types.IntegerType(), True),
        types.StructField("s_name", types.StringType(), True),
        types.StructField("s_address", types.StringType(), True),
        types.StructField("s_nationkey", types.IntegerType(), True),
        types.StructField("s_phone", types.StringType(), True),
        types.StructField("s_acctbal", types.DoubleType(), True),
        types.StructField("s_comment", types.StringType(), True),
        types.StructField("s_Unnamed", types.DoubleType(), True),
    ]
)

nation_schema = types.StructType(
    [
        types.StructField("n_nationkey", types.IntegerType(), True),
        types.StructField("n_name", types.StringType(), True),
        types.StructField("n_regionkey", types.IntegerType(), True),
        types.StructField("n_comment", types.StringType(), True),
        types.StructField("n_Unnamed", types.DoubleType(), True),
    ]
)

region_schema = types.StructType(
    [
        types.StructField("r_regionkey", types.IntegerType(), True),
        types.StructField("r_name", types.StringType(), True),
        types.StructField("r_comment", types.StringType(), True),
        types.StructField("r_Unnamed", types.DoubleType(), True),
    ]
)

part_schema = types.StructType(
    [
        types.StructField("p_partkey", types.IntegerType(), True),
        types.StructField("p_name", types.StringType(), True),
        types.StructField("p_mfgr", types.StringType(), True),
        types.StructField("p_brand", types.StringType(), True),
        types.StructField("p_type", types.StringType(), True),
        types.StructField("p_size", types.IntegerType(), True),
        types.StructField("p_container", types.StringType(), True),
        types.StructField("p_retailprice", types.DoubleType(), True),
        types.StructField("p_comment", types.StringType(), True),
        types.StructField("p_Unnamed", types.DoubleType(), True),
    ]
)


partsupp_schema = types.StructType(
    [
        types.StructField("ps_partkey", types.IntegerType(), True),
        types.StructField("ps_suppkey", types.IntegerType(), True),
        types.StructField("ps_availqty", types.IntegerType(), True),
        types.StructField("ps_supplycost", types.DoubleType(), True),
        types.StructField("ps_comment", types.StringType(), True),
        types.StructField("ps_Unnamed", types.DoubleType(), True),
    ]
)

order_schema = types.StructType(
    [
        types.StructField("o_orderkey", types.IntegerType(), True),
        types.StructField("o_custkey", types.IntegerType(), True),
        types.StructField("o_orderstatus", types.StringType(), True),
        types.StructField("o_totalprice", types.DoubleType(), True),
        types.StructField("o_orderdate", types.TimestampType(), True),
        types.StructField("o_orderpriority", types.StringType(), True),
        types.StructField("o_clerk", types.StringType(), True),
        types.StructField("o_shippriority", types.IntegerType(), True),
        types.StructField("o_comment", types.StringType(), True),
        types.StructField("o_Unnamed", types.DoubleType(), True),
    ]
)

lineitem_schema = types.StructType(
    [
        types.StructField("l_id", types.IntegerType(), True),
        types.StructField("l_orderkey", types.IntegerType(), True),
        types.StructField("l_ps_id", types.IntegerType(), True),
        types.StructField("l_linenumber", types.LongType(), True),
        types.StructField("l_quantity", types.IntegerType(), True),
        types.StructField("l_extendedprice", types.DoubleType(), True),
        types.StructField("l_discount", types.DoubleType(), True),
        types.StructField("l_tax", types.DoubleType(), True),
        types.StructField("l_returnflag", types.StringType(), True),
        types.StructField("l_linestatus", types.StringType(), True),
        types.StructField("l_shipdate", types.TimestampType(), True),
        types.StructField("l_commitdate", types.TimestampType(), True),
        types.StructField("l_receiptdate", types.TimestampType(), True),
        types.StructField("l_shipinstruct", types.StringType(), True),
        types.StructField("l_shipmode", types.StringType(), True),
        types.StructField("l_comment", types.StringType(), True),
        types.StructField("l_Unnamed", types.DoubleType(), True),
    ]
)


def initialize_spark():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(f"{PROJECT_ID}-spark")
        .getOrCreate()
    )

    # task_instance = kwargs['ti']
    # task_instance.xcom_push(key="spark_session", value=spark)
    # logging.info("XCOM variables spark_session is successfully pushed..")
    return spark


def stop_spark(spark: SparkSession):
    spark.stop()


def extract_n_transform_customer(spark: SparkSession):
    # Read Local Data and
    try:
        df_customer = (
            spark.read.schema(customer_schema)
            .option("sep", "|")
            .csv(f"{AIRFLOW_HOME}/customer.tbl")
        )

        df_customer = df_customer.drop("c_Unnamed")

        # find average value of customer account balance
        avg_acctbal_val = df_customer.select(avg("c_acctbal")).collect()
        avg_acctbal_split = str(avg_acctbal_val).split("=")
        avg_acctbal = float(avg_acctbal_split[1].strip(")]"))

        # classifying the customer account balance into 3 groups
        df_customer = df_customer.withColumn(
            "c_acctbal_class",
            when(df_customer.c_acctbal < 0, "negative")
            .when(df_customer.c_acctbal < avg_acctbal, "below_average")
            .otherwise("above_average")
            .cast("String"),
        ).select(
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_acctbal_class",
            "c_mktsegment",
            "c_comment",
        )

        return df_customer

    except OSError as error:
        print(error.strerror)
        exit


def extract_n_transform_supplier(spark: SparkSession):
    # Read Local Data and
    try:
        df_supplier = (
            spark.read.schema(supplier_schema)
            .option("sep", "|")
            .csv(f"{AIRFLOW_HOME}/supplier.tbl")
        )

        df_supplier = df_supplier.drop("s_Unnamed")

        return df_supplier

    except OSError as error:
        print(error.strerror)
        exit


def extract_n_transform_nation(spark: SparkSession):
    # Read Local Data and
    try:
        df_nation = (
            spark.read.schema(nation_schema)
            .option("sep", "|")
            .csv(f"{AIRFLOW_HOME}/nation.tbl")
        )

        df_nation = df_nation.drop("n_Unnamed")

        return df_nation

    except OSError as error:
        print(error.strerror)
        exit


def extract_n_transform_region(spark: SparkSession):
    # Read Local Data and
    try:
        df_region = (
            spark.read.option("sep", "|")
            .schema(region_schema)
            .csv(f"{AIRFLOW_HOME}/region.tbl")
        )

        df_region = df_region.drop("r_Unnamed")

        return df_region

    except OSError as error:
        print(error.strerror)
        exit


def extract_n_transform_part(spark: SparkSession):
    # Read Local Data and
    try:
        df_part = (
            spark.read.schema(part_schema)
            .option("sep", "|")
            .csv(f"{AIRFLOW_HOME}/part.tbl")
        )

        df_part = df_part.drop("p_Unnamed")

        return df_part

    except OSError as error:
        print(error.strerror)
        exit


def extract_n_transform_partsupp(spark: SparkSession):
    # Read Local Data and
    try:
        df_partsupp = (
            spark.read.schema(partsupp_schema)
            .option("sep", "|")
            .csv(f"{AIRFLOW_HOME}/partsupp.tbl")
        )

        df_partsupp = (
            df_partsupp.drop("ps_Unnamed")
            .withColumn("ps_id", df_partsupp.ps_suppkey)
            .select(
                "ps_id",
                "ps_partkey",
                "ps_suppkey",
                "ps_availqty",
                "ps_supplycost",
                "ps_comment",
            )
        )

        return df_partsupp

    except OSError as error:
        print(error.strerror)
        exit


def extract_n_transform_orders(spark: SparkSession):
    # Read Local Data and
    try:
        df_orders = (
            spark.read.option("sep", "|")
            .schema(order_schema)
            .csv(f"{AIRFLOW_HOME}/orders.tbl")
        )
        df_orders = df_orders.drop("o_Unnamed")

        # convert the dates to be distributed over the last 2 years

        df_orders = (
            df_orders.withColumn("o_year", year(df_orders.o_orderdate))
            .withColumn(
                "o_month",
                date_format(to_date(df_orders.o_orderdate, "MM"), "MMM"),
            )
            .select(
                "o_orderkey",
                "o_custkey",
                "o_orderstatus",
                "o_totalprice",
                "o_orderdate",
                "o_year",
                "o_month",
                "o_orderpriority",
                "o_clerk",
                "o_shippriority",
                "o_comment",
            )
        )

        return df_orders

    except OSError as error:
        print(error.strerror)
        exit


def revenue(x, y, z):
    return x - y - z


def extract_n_transform_lineitem(spark: SparkSession):
    # Read Local Data and
    try:
        df_lineitem = (
            spark.read.option("sep", "|")
            .schema(lineitem_schema)
            .csv(f"{AIRFLOW_HOME}/lineitem.tbl")
        )

        df_lineitem = df_lineitem.drop("l_Unnamed")

        # add revenue per line item
        df_lineitem = df_lineitem.withColumn(
            "l_revenue",
            revenue(
                df_lineitem.l_extendedprice,
                df_lineitem.l_discount,
                df_lineitem.l_tax,
            ),
        ).select(
            "l_id",
            "l_orderkey",
            "l_ps_id",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_revenue",
            "l_returnflag",
            "l_linestatus",
            "l_commitdate",
            "l_shipdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment",
        )

        return df_lineitem

    except OSError as error:
        print(error.strerror)
        exit


def format_dataset_and_save_locally():
    spark_session = initialize_spark()
    # extract and transform
    df_customer = extract_n_transform_customer(spark_session)
    df_supplier = extract_n_transform_supplier(spark_session)
    df_nation = extract_n_transform_nation(spark_session)
    df_region = extract_n_transform_region(spark_session)
    df_part = extract_n_transform_part(spark_session)
    df_partsupp = extract_n_transform_partsupp(spark_session)
    df_orders = extract_n_transform_orders(spark_session)
    df_lineitem = extract_n_transform_lineitem(spark_session)

    # saving locally
    df_customer.toPandas().to_parquet(
        f"{AIRFLOW_HOME}/df_customer.parquet", index=False
    )
    df_supplier.toPandas().to_parquet(
        f"{AIRFLOW_HOME}/df_supplier.parquet", index=False
    )
    df_nation.toPandas().to_parquet(f"{AIRFLOW_HOME}/df_nation.parquet", index=False)
    df_region.toPandas().to_parquet(f"{AIRFLOW_HOME}/df_region.parquet", index=False)
    df_part.toPandas().to_parquet(f"{AIRFLOW_HOME}/df_part.parquet", index=False)
    df_partsupp.toPandas().to_parquet(
        f"{AIRFLOW_HOME}/df_partsupp.parquet", index=False
    )
    df_orders.toPandas().to_parquet(f"{AIRFLOW_HOME}/df_orders.parquet", index=False)
    df_lineitem.toPandas().to_parquet(
        f"{AIRFLOW_HOME}/df_lineitem.parquet", index=False
    )

    stop_spark(spark_session)


# create sales facts table
def sales_facts_table(spark: SparkSession):

    try:
        df_orders = spark.read.parquet(f"{AIRFLOW_HOME}/df_orders.parquet")
        df_lineitem = spark.read.parquet(f"{AIRFLOW_HOME}/df_lineitem.parquet")
        sales_order_columns = [
            columns.replace("o_", "s_") for columns in df_orders.columns
        ]
        sales_order = df_orders.toDF(*sales_order_columns).withColumnRenamed(
            "s_comment", "s_o_comment"
        )
        sales_lineitem_columns = [
            columns.replace("l_", "s_") for columns in df_lineitem.columns
        ]
        sales_lineitem = df_lineitem.toDF(*sales_lineitem_columns).withColumnRenamed(
            "s_comment", "s_l_comment"
        )
        sales = sales_order.join(sales_lineitem, on="s_orderkey", how="outer")
        sales_columns = [
            "s_orderkey",
            "s_custkey",
            "s_ps_id",
            "s_orderstatus",
            "s_totalprice",
            "s_orderdate",
            "s_year",
            "s_month",
            "s_orderpriority",
            "s_clerk",
            "s_shippriority",
            "s_o_comment",
            "s_id",
            "s_linenumber",
            "s_quantity",
            "s_extendedprice",
            "s_discount",
            "s_tax",
            "s_revenue",
            "s_returnflag",
            "s_linestatus",
            "s_commitdate",
            "s_shipdate",
            "s_receiptdate",
            "s_shipinstruct",
            "s_shipmode",
            "s_l_comment",
        ]
        sales = sales.select(*sales_columns).withColumnRenamed("s_ps_id", "s_suppkey")
        sales = sales.select(
            concat(sales.s_orderkey, sales.s_linenumber).alias("s_orderkey_linenumber"),
            "s_orderkey",
            "s_custkey",
            "s_suppkey",
            "s_orderstatus",
            "s_totalprice",
            "s_orderdate",
            "s_year",
            "s_month",
            "s_orderpriority",
            "s_clerk",
            "s_shippriority",
            "s_o_comment",
            "s_id",
            "s_linenumber",
            "s_quantity",
            "s_extendedprice",
            "s_discount",
            "s_tax",
            "s_revenue",
            "s_returnflag",
            "s_linestatus",
            "s_commitdate",
            "s_shipdate",
            "s_receiptdate",
            "s_shipinstruct",
            "s_shipmode",
            "s_l_comment",
        )

        return sales

    except OSError as error:
        print(error.strerror)
        exit


# create customer star table
def customer_dimension_table(spark: SparkSession):

    try:
        df_customer = spark.read.parquet(f"{AIRFLOW_HOME}/df_customer.parquet")
        df_nation = spark.read.parquet(f"{AIRFLOW_HOME}/df_nation.parquet")
        df_region = spark.read.parquet(f"{AIRFLOW_HOME}/df_region.parquet")
        customer_nation_columns = [
            "c_nationkey",
            "c_n_name",
            "c_regionkey",
            "c_n_comment",
        ]
        customer_nation = df_nation.toDF(*customer_nation_columns)
        customer_region_columns = ["c_regionkey", "c_r_name", "c_r_comment"]
        customer_region = df_region.toDF(*customer_region_columns)
        customer_nation_region = customer_nation.join(
            customer_region, on="c_regionkey", how="outer"
        )
        customer = df_customer.join(
            customer_nation_region, on="c_nationkey", how="outer"
        )
        customer_columns = [
            "c_custkey",
            "c_name",
            "c_address",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
            "c_nationkey",
            "c_n_name",
            "c_n_comment",
            "c_regionkey",
            "c_r_name",
            "c_r_comment",
        ]
        customer = customer.select(*customer_columns)

        return customer

    except OSError as error:
        print(error.strerror)
        exit


# create supplier dimension table
def supplier_dimension_table(spark: SparkSession):
    try:
        df_supplier = spark.read.parquet(f"{AIRFLOW_HOME}/df_supplier.parquet")
        df_nation = spark.read.parquet(f"{AIRFLOW_HOME}/df_nation.parquet")
        df_region = spark.read.parquet(f"{AIRFLOW_HOME}/df_region.parquet")
        df_supplier_columns = [
            column.replace("s_", "supp_") for column in df_supplier.columns
        ]
        df_supplier = df_supplier.toDF(*df_supplier_columns)
        supplier_nation_columns = [
            "supp_nationkey",
            "supp_n_name",
            "supp_regionkey",
            "supp_n_comment",
        ]
        supplier_nation = df_nation.toDF(*supplier_nation_columns)
        supplier_region_columns = [
            "supp_regionkey",
            "supp_r_name",
            "supp_r_comment",
        ]
        supplier_region = df_region.toDF(*supplier_region_columns)
        supplier_nation_region = supplier_nation.join(
            supplier_region, on="supp_regionkey", how="outer"
        )
        supplier = df_supplier.join(
            supplier_nation_region, on="supp_nationkey", how="outer"
        )
        supplier_columns = [
            "supp_suppkey",
            "supp_name",
            "supp_address",
            "supp_phone",
            "supp_acctbal",
            "supp_comment",
            "supp_nationkey",
            "supp_n_name",
            "supp_n_comment",
            "supp_regionkey",
            "supp_r_name",
            "supp_r_comment",
        ]
        supplier = supplier.select(*supplier_columns)

        return supplier

    except OSError as error:
        print(error.strerror)
        exit


# create part diemnsion table
def part_dimension_table(spark: SparkSession):

    try:
        df_partsupp = spark.read.parquet(f"{AIRFLOW_HOME}/df_partsupp.parquet")
        df_partsupp = df_partsupp.drop("ps_id").withColumnRenamed(
            "ps_partkey", "p_partkey"
        )
        df_part = spark.read.parquet(f"{AIRFLOW_HOME}/df_part.parquet")
        part = df_part.join(df_partsupp, on="p_partkey", how="outer")
        part = part.select(
            concat(part.p_partkey, part.ps_suppkey).alias("p_partkey_suppkey"),
            "p_partkey",
            "p_name",
            "p_mfgr",
            "p_brand",
            "p_type",
            "p_size",
            "p_container",
            "p_retailprice",
            "p_comment",
            "ps_suppkey",
            "ps_availqty",
            "ps_supplycost",
            "ps_comment",
        )

        return part

    except OSError as error:
        print(error.strerror)
        exit


def generate_facts_and_dimension_table():
    spark_session = initialize_spark()

    sales = sales_facts_table(spark_session)
    customer = customer_dimension_table(spark_session)
    supplier = supplier_dimension_table(spark_session)
    part = part_dimension_table(spark_session)

    sales.toPandas().to_parquet(f"{AIRFLOW_HOME}/sales.parquet", index=False)
    customer.toPandas().to_parquet(f"{AIRFLOW_HOME}/customer.parquet", index=False)
    supplier.toPandas().to_parquet(f"{AIRFLOW_HOME}/supplier.parquet", index=False)
    part.toPandas().to_parquet(f"{AIRFLOW_HOME}/part.parquet", index=False)

    stop_spark(spark_session)
