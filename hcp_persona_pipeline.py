# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/default/hcp_volumes"))

# COMMAND ----------

BASE = "/Volumes/workspace/default/hcp_volumes"

ROSTER_CSV = f"{BASE}/hcp_demographics.csv"
SALES_CSV  = f"{BASE}/sales_weekly.csv"
EMAIL_CSV  = f"{BASE}/email_activity.csv"
CALLS_CSV  = f"{BASE}/calls.csv"
KOL_CSV    = f"{BASE}/kol_events.csv"

# COMMAND ----------

display(dbutils.fs.ls(BASE))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "400")

# COMMAND ----------

# Roster schema
roster_schema = StructType([
    StructField("HCP_ID", StringType(), False),
    StructField("Name", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Specialty", StringType(), True),
    StructField("Employment_Type", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Join_Date", DateType(), True)
])

# Weekly sales schema
sales_schema = StructType([
    StructField("HCP_ID", StringType(), False),
    StructField("Week_Start", DateType(), True),
    StructField("Our_Brand_Sales", DoubleType(), True),
    StructField("Competitor_Brand_Sales", DoubleType(), True),
    StructField("Channel", StringType(), True)
])

# Email activity schema
email_schema = StructType([
    StructField("HCP_ID", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("event_date", TimestampType(), True),
    StructField("activity_status", StringType(), True),
    StructField("campaign_id", StringType(), True)
])

# Calls schema
calls_schema = StructType([
    StructField("HCP_ID", StringType(), False),
    StructField("Date", DateType(), True),
    StructField("Record_Type", StringType(), True),
    StructField("Planned_Calls", FloatType(), True),
    StructField("Actual_Calls", FloatType(), True),
    StructField("Call_Purpose", StringType(), True),
    StructField("Quarter", StringType(), True)
])

# KOL events schema
kol_schema = StructType([
    StructField("HCP_ID", StringType(), False),
    StructField("Event_ID", StringType(), False),
    StructField("Event_Type", StringType(), True),
    StructField("Event_Date", DateType(), True),
    StructField("Title", StringType(), True),
    StructField("Venue", StringType(), True)
])

# COMMAND ----------

roster = spark.read.csv(ROSTER_CSV, header=True, schema=roster_schema)
sales_weekly = spark.read.csv(SALES_CSV, header=True, schema=sales_schema)
email_activity = spark.read.csv(EMAIL_CSV, header=True, schema=email_schema)
calls = spark.read.csv(CALLS_CSV, header=True, schema=calls_schema)
kol_events = spark.read.csv(KOL_CSV, header=True, schema=kol_schema)

display(roster)

# COMMAND ----------

display(sales_weekly)
display(email_activity)
display(calls)
display(kol_events)

# COMMAND ----------

# 16 - quick counts for sanity
print("roster rows:", roster.count())
print("sales rows:", sales_weekly.count())
print("email rows:", email_activity.count())
print("calls rows:", calls.count())
print("kol rows:", kol_events.count())

# COMMAND ----------

# 17 - persist larger DataFrames to avoid re-reading / re-computing
sales_weekly = sales_weekly.repartition("HCP_ID")
calls = calls.repartition("HCP_ID")

# COMMAND ----------

# 18 - normalize text fields and build flags
r = roster.withColumn("Employment_Type", F.initcap(F.col("Employment_Type"))) \
          .withColumn("Status", F.initcap(F.col("Status")))

# COMMAND ----------


agg = r.groupBy("HCP_ID").agg(
    F.max(F.when(F.col("Employment_Type") == "Permanent", 1).otherwise(0)).alias("has_permanent"),
    F.max(F.when(F.col("Status") == "Active", 1).otherwise(0)).alias("has_active"),
    F.min("Join_Date").alias("Join_Date"),
    F.first("Name").alias("Name"),
    F.first("City").alias("City"),
    F.first("State").alias("State"),
    F.first("Specialty").alias("Specialty")
)

# COMMAND ----------

agg.show()
agg.count()

# COMMAND ----------

# 19 - derive canonical fields
canonical = agg.withColumn("Employment_Type",
                           F.when(F.col("has_permanent") == 1, F.lit("Permanent")).otherwise(F.lit("Temporary"))) \
               .withColumn("Status",
                           F.when(F.col("has_active") == 1, F.lit("Active")).otherwise(F.lit("Terminated"))) \
               .select("HCP_ID","Name","City","State","Specialty","Employment_Type","Status","Join_Date")

# COMMAND ----------

canonical.show()
canonical.count()

# COMMAND ----------

# 20 - optionally broadcast for downstream joins (small)
from pyspark.sql.functions import broadcast
canonical_b = broadcast(canonical)
display(canonical_b)
canonical_b.count()

# COMMAND ----------

# 21 - remove exact duplicates
emails_dedup = email_activity.dropDuplicates(["HCP_ID","transaction_id","event_date","activity_status","campaign_id"])

# COMMAND ----------


email_activity.count()
#emails_dedup.count()

# COMMAND ----------

#emails_dedup = email_activity.dropDuplicates(["HCP_ID","transaction_id","activity_status"])

# COMMAND ----------

emails_dedup.count()

# COMMAND ----------

# 22 - aggregate statuses per transaction
trans_status = emails_dedup.groupBy("HCP_ID","transaction_id").agg(
    F.collect_set("activity_status").alias("statuses"),
    F.min("event_date").alias("first_event"),
    F.max("event_date").alias("last_event"),
    F.count("*").alias("events_count")
)

# COMMAND ----------

trans_status.show()
trans_status.count()

# COMMAND ----------

# 23 - flag inconsistent sequences (Opened without Delivered)
trans_status = trans_status.withColumn("opened_but_not_delivered",
                                       (F.array_contains(F.col("statuses"), "Opened")) &
                                       (~F.array_contains(F.col("statuses"), "Delivered")))

# COMMAND ----------

display(trans_status.limit(50))
trans_status.count()

# COMMAND ----------

# 24 - active roster used for skeleton (only HCPs we consider active)
active_roster = canonical.filter(F.col("Status") == "Active").select("HCP_ID","Join_Date")

# COMMAND ----------

# 25 - calendar range from sales data
min_week = sales_weekly.agg(F.min("Week_Start")).first()[0]
max_week = sales_weekly.agg(F.max("Week_Start")).first()[0]

# COMMAND ----------

# 26 - build list of week_start dates (Python loop safe for moderate ranges)
weeks = []
d = min_week
while d <= max_week:
    weeks.append((d,))
    d += datetime.timedelta(days=7)

weeks_df = spark.createDataFrame(weeks, ["Week_Start"])

# COMMAND ----------

weeks_df.show()

# COMMAND ----------

# 27 - cross join skeleton and fill missing sales with zeros
skeleton = active_roster.crossJoin(weeks_df)
sales_filled = skeleton.join(sales_weekly, ["HCP_ID","Week_Start"], "left") \
    .withColumn("Our_Brand_Sales", F.coalesce(F.col("Our_Brand_Sales"), F.lit(0.0))) \
    .withColumn("Competitor_Brand_Sales", F.coalesce(F.col("Competitor_Brand_Sales"), F.lit(0.0))) \
    .withColumn("in_tenure", F.when(F.col("Week_Start") >= F.col("Join_Date"), F.lit(True)).otherwise(F.lit(False)))

display(sales_filled.limit(20))

# COMMAND ----------

# 28 - planned vs actual per quarter
planned = calls.filter(F.col("Record_Type") == "Planned").groupBy("HCP_ID","Quarter") \
    .agg(F.sum(F.coalesce("Planned_Calls", F.lit(0))).alias("Planned_Calls_Quarter"))

actual = calls.filter(F.col("Record_Type") == "Actual").groupBy("HCP_ID","Quarter") \
    .agg(F.sum(F.coalesce("Actual_Calls", F.lit(0))).alias("Actual_Calls_Quarter"))

# COMMAND ----------

planned.show()
planned.count()

# COMMAND ----------

actual.show()
actual.count()

# COMMAND ----------

# 29 - join and compute attainment percentage and flag
calls_quarter = planned.join(actual, ["HCP_ID","Quarter"], "full_outer") \
    .withColumn("Planned_Calls_Quarter", F.coalesce(F.col("Planned_Calls_Quarter"), F.lit(0))) \
    .withColumn("Actual_Calls_Quarter", F.coalesce(F.col("Actual_Calls_Quarter"), F.lit(0))) \
    .withColumn("call_attainment_pct",
                F.when(F.col("Planned_Calls_Quarter") == 0,
                       F.when(F.col("Actual_Calls_Quarter") > 0, F.lit(100.0)).otherwise(F.lit(0.0)))
                 .otherwise(100.0 * F.col("Actual_Calls_Quarter") / F.col("Planned_Calls_Quarter"))
               ) \
    .withColumn("flag_below_70pct", F.col("Actual_Calls_Quarter") < 0.7 * F.col("Planned_Calls_Quarter"))

display(calls_quarter.limit(50))
calls_quarter.count()

# COMMAND ----------

# 30 - reference date
max_sales_date = sales_weekly.agg(F.max("Week_Start")).first()[0]
max_kol_date = kol_events.agg(F.max("Event_Date")).first()[0]
REF_DATE = max([d for d in [max_sales_date, max_kol_date] if d is not None]) if max_sales_date is not None else datetime.date.today()

# 31 - boundaries
START_24M = REF_DATE - relativedelta(months=24)
START_12M = REF_DATE - relativedelta(months=12)


# COMMAND ----------

# 32 - sales aggregates (24 months)
sales_24 = sales_weekly.filter(F.col("Week_Start") >= START_24M) \
    .groupBy("HCP_ID").agg(
        F.sum("Our_Brand_Sales").alias("Total_Our_Sales_24m"),
        F.sum("Competitor_Brand_Sales").alias("Total_Competitor_Sales_24m"),
        F.avg("Our_Brand_Sales").alias("Avg_Weekly_Our_Sales_24m"),
        F.stddev("Our_Brand_Sales").alias("Sales_Volatility_24m"),
        F.max("Week_Start").alias("Last_Sale_Date")
    )

# COMMAND ----------

sales_24.show()

# COMMAND ----------

# 33 - email counts and engagement score (24m)
email_24_counts = email_activity.filter(F.col("event_date") >= START_24M) \
    .groupBy("HCP_ID").pivot("activity_status", ["Sent","Delivered","Opened"]).count().na.fill(0)

# COMMAND ----------

email_24_counts.show()
email_24_counts.count()

# COMMAND ----------

email_24 = email_24_counts.withColumn("Opened", F.coalesce(F.col("Opened"), F.lit(0))) \
    .withColumn("Delivered", F.coalesce(F.col("Delivered"), F.lit(0))) \
    .withColumn("Email_Engagement_Score",
                F.when((F.col("Delivered") == 0) & (F.col("Opened") == 0), F.lit(0.0))
                 .when((F.col("Delivered") == 0) & (F.col("Opened") > 0), F.lit(100.0))
                 .otherwise(100.0 * F.col("Opened") / F.col("Delivered"))
               )

email_24.show()
email_24.count()

# COMMAND ----------

# 34 - last opened per HCP
last_open = email_activity.filter(F.col("activity_status") == "Opened").groupBy("HCP_ID").agg(F.max("event_date").alias("Last_Email_Open"))

# COMMAND ----------

last_open.show()
last_open.count()

# COMMAND ----------

# 35 - KOL counts (24m)
kol_24 = kol_events.filter(F.col("Event_Date") >= START_24M).groupBy("HCP_ID").agg(F.count("*").alias("KOL_Count_24m"), F.max("Event_Date").alias("Last_KOL_Event_Date"))

# COMMAND ----------

kol_24.show()
kol_24.count()

# COMMAND ----------

# 36 - calls last 12 months (planned & actual)
planned_12 = calls.filter((F.col("Record_Type") == "Planned") & (F.col("Date") >= START_12M)).groupBy("HCP_ID").agg(F.sum(F.coalesce("Planned_Calls", F.lit(0))).alias("Planned_Calls_12m"))
actual_12 = calls.filter((F.col("Record_Type") == "Actual") & (F.col("Date") >= START_12M)).groupBy("HCP_ID").agg(F.sum(F.coalesce("Actual_Calls", F.lit(0))).alias("Actual_Calls_12m"))

call_coverage = planned_12.join(actual_12, on="HCP_ID", how="full_outer") \
    .withColumn("Planned_Calls_12m", F.coalesce(F.col("Planned_Calls_12m"), F.lit(0))) \
    .withColumn("Actual_Calls_12m", F.coalesce(F.col("Actual_Calls_12m"), F.lit(0))) \
    .withColumn("Call_Coverage_12m",
                F.when(F.col("Planned_Calls_12m") == 0,
                       F.when(F.col("Actual_Calls_12m") > 0, F.lit(1.0)).otherwise(F.lit(0.0)))
                 .otherwise(F.col("Actual_Calls_12m") / F.col("Planned_Calls_12m"))
               )
display(call_coverage.limit(20))

# COMMAND ----------

# 37 - join features to canonical roster (broadcast small roster)
features = canonical_b.select("HCP_ID","Name","Specialty","Join_Date","Status") \
    .join(sales_24, on="HCP_ID", how="left") \
    .join(email_24.select("HCP_ID","Email_Engagement_Score"), on="HCP_ID", how="left") \
    .join(last_open, on="HCP_ID", how="left") \
    .join(kol_24, on="HCP_ID", how="left") \
    .join(call_coverage.select("HCP_ID","Actual_Calls_12m","Planned_Calls_12m","Call_Coverage_12m"), on="HCP_ID", how="left") \
    .na.fill({
        "Total_Our_Sales_24m": 0.0,
        "Total_Competitor_Sales_24m": 0.0,
        "Avg_Weekly_Our_Sales_24m": 0.0,
        "Sales_Volatility_24m": 0.0,
        "Email_Engagement_Score": 0.0,
        "KOL_Count_24m": 0,
        "Actual_Calls_12m": 0,
        "Planned_Calls_12m": 0,
        "Call_Coverage_12m": 0.0
    })

display(features.limit(50))

# COMMAND ----------

features.count()

# COMMAND ----------

# 38 - Sales percentile buckets via approxQuantile
quantiles = features.approxQuantile("Total_Our_Sales_24m", [0.2,0.4,0.6,0.8], 0.01)
q20,q40,q60,q80 = quantiles if len(quantiles)==4 else (0.0,0.0,0.0,0.0)

# COMMAND ----------

# 39 - Sales score buckets
features = features.withColumn("Sales_Score",
    F.when(F.col("Total_Our_Sales_24m") < F.lit(q20), F.lit(20))
     .when(F.col("Total_Our_Sales_24m") < F.lit(q40), F.lit(40))
     .when(F.col("Total_Our_Sales_24m") < F.lit(q60), F.lit(60))
     .when(F.col("Total_Our_Sales_24m") < F.lit(q80), F.lit(80))
     .otherwise(F.lit(100))
)

# COMMAND ----------

features.show()
features.count()

# COMMAND ----------

# 40 - KOL score mapping
features = features.withColumn("KOL_Score",
    F.when(F.col("KOL_Count_24m") == 0, F.lit(0))
     .when(F.col("KOL_Count_24m") == 1, F.lit(40))
     .when(F.col("KOL_Count_24m") == 2, F.lit(60))
     .when(F.col("KOL_Count_24m") == 3, F.lit(80))
     .otherwise(F.lit(100))
)

# COMMAND ----------

features.show()
features.count()

# COMMAND ----------

# 41 - Call attainment %
features = features.withColumn("Call_Attainment_Score",
    F.when((F.col("Planned_Calls_12m") == 0) & (F.col("Actual_Calls_12m") == 0), F.lit(0.0))
     .when((F.col("Planned_Calls_12m") == 0) & (F.col("Actual_Calls_12m") > 0), F.lit(100.0))
     .otherwise(100.0 * F.col("Actual_Calls_12m") / F.col("Planned_Calls_12m"))
)

# COMMAND ----------

features.show()
features.count()

# COMMAND ----------

# 42 - tenure and low-flag sum
features = features.withColumn("tenure_days", F.datediff(F.lit(REF_DATE), F.col("Join_Date")))
low_sales = (F.col("Sales_Score") <= 40)
low_email = (F.col("Email_Engagement_Score") <= 30)
low_call = (F.col("Call_Attainment_Score") <= 40)
kol_zero = (F.col("KOL_Score") == 0)

features = features.withColumn("low_flags_sum",
    (F.when(low_sales,1).otherwise(0) + F.when(low_email,1).otherwise(0) + F.when(low_call,1).otherwise(0) + F.when(kol_zero,1).otherwise(0))
)

# COMMAND ----------

features.show()

# COMMAND ----------

features.count()

# COMMAND ----------

# 43 - persona assignment rules
features = features.withColumn("persona",
    F.when(F.col("tenure_days") < 180, F.lit("Unknown")) \
     .when((F.col("Sales_Score") >= 80) & (F.col("Email_Engagement_Score") >= 60) & (F.col("Call_Attainment_Score") >= 80) & (F.col("KOL_Score") >= 60), F.lit("High Performer")) \
     .when((F.col("Sales_Score").between(50,79)) & (F.col("Email_Engagement_Score").between(40,79)) & (F.col("Call_Attainment_Score").between(50,79)) & (F.col("KOL_Score").between(20,79)), F.lit("Growth Potential")) \
     .when(F.col("low_flags_sum") >= 2, F.lit("At Risk")) \
     .when((F.col("Sales_Score") <= 40) & (F.col("Email_Engagement_Score") <= 20) & (F.col("Call_Attainment_Score") <= 30) & (F.col("KOL_Score") <= 20), F.lit("Low Engagement Stable")) \
     .otherwise(F.lit("Unknown"))
)

# COMMAND ----------

features.show()
features.count()

# COMMAND ----------

# 44 - select final columns and write outputs
out_cols = ["HCP_ID","Name","Specialty","Status","Join_Date","Total_Our_Sales_24m","Sales_Score","Email_Engagement_Score",
            "Call_Attainment_Score","KOL_Count_24m","KOL_Score","tenure_days","persona"]

out_df = features.select(*[c for c in out_cols if c in features.columns])

# write parquet and csv parts (do not coalesce(1) for performance)
out_df.write.mode("overwrite").parquet(f"{BASE}/hcp_persona_features.parquet")
out_df.write.mode("overwrite").option("header", True).csv(f"{BASE}/hcp_persona_features_csv")

display(out_df.limit(100))
print("Wrote outputs to:", f"{BASE}/hcp_persona_features.parquet", f"{BASE}/hcp_persona_features_csv")

# COMMAND ----------

