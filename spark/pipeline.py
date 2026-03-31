# spark/pipeline.py
# Employee Data Cleaning and Transformation Pipeline
# Reads employees_raw.csv, cleans and transforms data, loads to PostgreSQL

import logging
import re
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

# Logging setup 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/opt/spark-apps/pipeline.log')
    ]
)
log = logging.getLogger(__name__)

# Config 
INPUT_PATH  = "/opt/spark-data/employees_raw.csv"
JDBC_URL    = "jdbc:postgresql://postgres:5432/employee_db"
JDBC_TABLE  = "employees_clean"
JDBC_PROPS  = {
    "user":     "spark_user",
    "password": "spark_pass123",
    "driver":   "org.postgresql.Driver"
}

#  Raw schema 
RAW_SCHEMA = StructType([
    StructField("employee_id", StringType(), True),
    StructField("first_name",  StringType(), True),
    StructField("last_name",   StringType(), True),
    StructField("email",       StringType(), True),
    StructField("hire_date",   StringType(), True),
    StructField("job_title",   StringType(), True),
    StructField("department",  StringType(), True),
    StructField("salary",      StringType(), True),
    StructField("manager_id",  StringType(), True),
    StructField("address",     StringType(), True),
    StructField("city",        StringType(), True),
    StructField("state",       StringType(), True),
    StructField("zip_code",    StringType(), True),
    StructField("birth_date",  StringType(), True),
    StructField("status",      StringType(), True),
])


def create_spark_session():
    log.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("EmployeePipeline") \
        .config("spark.jars", "/opt/spark/jars/extra/postgresql-42.7.3.jar") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created successfully")
    return spark


# ══════════════════════════════════════════════════════════════════
# TASK 3.1 — DATA QUALITY CHECKS
# ══════════════════════════════════════════════════════════════════

def run_quality_checks(df, stage="raw"):
    """Log row counts and null counts per column."""
    total = df.count()
    log.info(f"[Quality Check - {stage}] Total rows: {total}")
    for col in df.columns:
        null_count = df.filter(
            F.col(col).isNull() | (F.trim(F.col(col)) == "")
        ).count()
        if null_count > 0:
            log.info(f"  Column '{col}': {null_count} nulls/blanks ({round(null_count/total*100,1)}%)")
    return total


def remove_duplicates(df):
    """Remove duplicate rows based on employee_id keeping first occurrence."""
    before = df.count()
    df = df.dropDuplicates(["employee_id"])
    after = df.count()
    log.info(f"[Dedup] Removed {before - after} duplicate employee_id rows. Remaining: {after}")
    return df


def filter_invalid_hire_dates(df):
    """Remove rows where hire_date is in the future."""
    today = date.today().strftime("%Y-%m-%d")
    before = df.count()
    df = df.filter(
        F.col("hire_date").isNull() |
        (F.col("hire_date") <= today)
    )
    after = df.count()
    log.info(f"[HireDate] Removed {before - after} future hire date rows. Remaining: {after}")
    return df


def tag_invalid_emails(df):
    """Flag invalid emails — used for logging only, not dropped."""
    email_regex = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    invalid_count = df.filter(
        ~F.col("email").rlike(email_regex) |
        F.col("email").isNull()
    ).count()
    log.info(f"[Email] Found {invalid_count} invalid email formats")
    return df


# ══════════════════════════════════════════════════════════════════
# TASK 3.2 — DATA TRANSFORMATIONS
# ══════════════════════════════════════════════════════════════════

def clean_names(df):
    """Convert first_name and last_name to proper case."""
    df = df.withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    df = df.withColumn("last_name",  F.initcap(F.trim(F.col("last_name"))))
    log.info("[Transform] Name standardization applied (initcap + trim)")
    return df


def clean_emails(df):
    """Lowercase all emails and null out invalid ones."""
    email_regex = r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    df = df.withColumn(
        "email",
        F.when(F.col("email").rlike(email_regex), F.col("email"))
         .otherwise(None)
    )
    log.info("[Transform] Email cleanup applied (lowercase + invalid nulled)")
    return df


def clean_salary(df):
    """Strip $, commas, quotes from salary and cast to decimal."""
    df = df.withColumn(
        "salary",
        F.regexp_replace(F.col("salary"), r'[\$,\"]', '')
    )
    df = df.withColumn(
        "salary",
        F.when(F.trim(F.col("salary")) == "", None)
         .otherwise(F.col("salary").cast("decimal(10,2)"))
    )
    log.info("[Transform] Salary cleaned (symbols removed, cast to decimal)")
    return df


def clean_dates(df):
    """Cast hire_date and birth_date strings to DateType."""
    df = df.withColumn("hire_date",  F.to_date(F.col("hire_date"),  "yyyy-MM-dd"))
    df = df.withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
    log.info("[Transform] Dates cast to DateType")
    return df


def calculate_age(df):
    """Calculate employee age in years from birth_date."""
    df = df.withColumn(
        "age",
        F.when(
            F.col("birth_date").isNotNull(),
            F.floor(
                F.datediff(F.current_date(), F.col("birth_date")) / 365.25
            ).cast(IntegerType())
        ).otherwise(None)
    )
    log.info("[Transform] Age calculated from birth_date")
    return df


def calculate_tenure(df):
    """Calculate years of service from hire_date."""
    df = df.withColumn(
        "tenure_years",
        F.when(
            F.col("hire_date").isNotNull(),
            F.round(
                F.datediff(F.current_date(), F.col("hire_date")) / 365.25,
                1
            ).cast("decimal(3,1)")
        ).otherwise(None)
    )
    log.info("[Transform] Tenure years calculated from hire_date")
    return df


def apply_salary_bands(df):
    """Assign salary band: Junior < 50k, Mid 50k-80k, Senior > 80k."""
    df = df.withColumn(
        "salary_band",
        F.when(F.col("salary").isNull(), None)
         .when(F.col("salary") < 50000,  "Junior")
         .when(F.col("salary") <= 80000, "Mid")
         .otherwise("Senior")
    )
    log.info("[Transform] Salary bands applied (Junior/Mid/Senior)")
    return df


def standardize_status(df):
    """Normalize status to proper case Active/Inactive/Terminated."""
    df = df.withColumn(
        "status",
        F.when(F.upper(F.col("status")) == "ACTIVE",     "Active")
         .when(F.upper(F.col("status")) == "INACTIVE",   "Inactive")
         .when(F.upper(F.col("status")) == "TERMINATED", "Terminated")
         .otherwise("Active")
    )
    log.info("[Transform] Status standardized")
    return df


def standardize_department(df):
    """Normalize department to proper case, with acronym corrections."""
    df = df.withColumn("department", F.initcap(F.trim(F.col("department"))))
    df = df.withColumn(
        "department",
        F.when(F.upper(F.col("department")) == "IT", "IT")
         .when(F.upper(F.col("department")) == "HR", "HR")
         .otherwise(F.col("department"))
    )
    log.info("[Transform] Department standardized (with acronym corrections)")
    return df


# ══════════════════════════════════════════════════════════════════
# TASK 3.3 — DATA ENRICHMENT
# ══════════════════════════════════════════════════════════════════

def add_full_name(df):
    """Add full_name = first_name + ' ' + last_name."""
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )
    log.info("[Enrich] full_name column added")
    return df


def add_email_domain(df):
    """Extract domain from email address."""
    df = df.withColumn(
        "email_domain",
        F.when(
            F.col("email").isNotNull(),
            F.split(F.col("email"), "@").getItem(1)
        ).otherwise(None)
    )
    log.info("[Enrich] email_domain column extracted")
    return df


# ══════════════════════════════════════════════════════════════════
# TASK 4.2 — LOAD TO POSTGRESQL
# ══════════════════════════════════════════════════════════════════

def select_final_columns(df):
    """Select and order columns to match the target table schema."""
    return df.select(
        F.col("employee_id").cast(IntegerType()),
        F.col("first_name"),
        F.col("last_name"),
        F.col("full_name"),
        F.col("email"),
        F.col("email_domain"),
        F.col("hire_date"),
        F.col("job_title"),
        F.col("department"),
        F.col("salary"),
        F.col("salary_band"),
        F.col("manager_id").cast(IntegerType()),
        F.col("address"),
        F.col("city"),
        F.col("state"),
        F.col("zip_code"),
        F.col("birth_date"),
        F.col("age"),
        F.col("tenure_years"),
        F.col("status"),
    )


def drop_rows_missing_required(df):
    """Drop rows missing NOT NULL fields: employee_id, first_name, last_name, hire_date, email."""
    before = df.count()
    df = df.filter(
        F.col("employee_id").isNotNull() &
        F.col("first_name").isNotNull()  &
        F.col("last_name").isNotNull()   &
        F.col("hire_date").isNotNull()   &
        F.col("email").isNotNull()
    )
    after = df.count()
    log.info(f"[Filter] Dropped {before - after} rows missing required fields. Remaining: {after}")
    return df


def load_to_postgres(df):
    """Write cleaned DataFrame to PostgreSQL using JDBC."""
    log.info(f"[Load] Writing {df.count()} rows to PostgreSQL table '{JDBC_TABLE}'...")
    df.write \
      .jdbc(
          url=JDBC_URL,
          table=JDBC_TABLE,
          mode="overwrite",
          properties=JDBC_PROPS
      )
    log.info(f"[Load] Done. Data loaded to {JDBC_TABLE} successfully.")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("Employee Data Pipeline Starting")
    log.info("=" * 60)

    spark = create_spark_session()

    #  Read raw CSV 
    log.info(f"[Read] Loading raw data from {INPUT_PATH}")
    df = spark.read \
        .option("header", "true") \
        .option("quote",  '"') \
        .option("escape", '"') \
        .schema(RAW_SCHEMA) \
        .csv(INPUT_PATH)
    run_quality_checks(df, stage="raw")

    #  3.1 Quality checks 
    df = remove_duplicates(df)
    df = filter_invalid_hire_dates(df)
    df = tag_invalid_emails(df)

    #  3.2 Transformations 
    df = clean_names(df)
    df = clean_emails(df)
    df = clean_salary(df)
    df = clean_dates(df)
    df = calculate_age(df)
    df = calculate_tenure(df)
    df = apply_salary_bands(df)
    df = standardize_status(df)
    df = standardize_department(df)

    #  3.3 Enrichment 
    df = add_full_name(df)
    df = add_email_domain(df)

    #  Prepare final output 
    df = add_full_name(df)
    df = add_email_domain(df)
    df = select_final_columns(df)
    df = drop_rows_missing_required(df)
    run_quality_checks(df, stage="cleaned")

    #  Load to Postgres 
    load_to_postgres(df)

    #  Final verification 
    log.info("[Verify] Reading back from PostgreSQL to confirm load...")
    verify_df = spark.read.jdbc(
        url=JDBC_URL,
        table=JDBC_TABLE,
        properties=JDBC_PROPS
    )
    loaded_count = verify_df.count()
    log.info(f"[Verify] Rows in employees_clean: {loaded_count}")

    log.info("=" * 60)
    log.info(f"Pipeline complete. {loaded_count} clean records in PostgreSQL.")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()