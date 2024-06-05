from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year

spark = SparkSession.builder.appName("NH_Data").getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=CORRECTED")

# read without header - (large file ~1.3GB)
df = (
    spark.read.format("csv")
    .option("header", "false")
    .load("/Users/sandeep/Downloads/cms_2567_all_years.csv")
)

# drop Unnamed col - pandas' gift
df = df.drop("_c0")

# take first row which has columns
header = df.first()

# rename columns with the first row
for key, ind in enumerate(header):
    df = df.withColumnRenamed(df.columns[key], ind)


# drop the row that now is the column header
df = df.offset(1)
df = df.dropDuplicates()

# cast in date format
df = df.withColumn(
    "inspect_date",
    to_date(df.inspection_date, "MM/dd/yyyy").cast("date"),  # .alias("date")
)
df = df.drop("inspection_text", "IDR", "IIDR")

# keep non-missing inspection dates
df_with_date = df.filter(df.inspection_date.isNotNull())

# obtain year from inspection date
df_with_date = df_with_date.withColumn("insp_year", year(df_with_date.inspect_date))

# filter on years to subset the data
df_small = df_with_date.filter(df_with_date.insp_year.between(2014, 2023))

df_small = df_small.drop("complaint", "Complaint", "Standard", "standard")

# max_val = df_small.agg({"insp_year": "max"}).collect()[0][0]

# group by year to get a count of
yr_tag_count = df_small.groupBy(["insp_year", "deficiency_tag"]).count()
yr_tag_count_pd = yr_tag_count.toPandas()

yr_tag_count_pd.to_csv("plt_data.csv")

# df_duped = df_duped.drop("Standard")
# df_duped = df_duped.drop("IDR")
# df_duped = df_duped.drop("IIDR")

# write to parquet -
df_small.write.parquet("nh_set_2016-2022.parquet")
