# task3_compare_engagement_levels.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, round as spark_round

def initialize_spark(app_name="Task3_Compare_Engagement_Levels"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def map_engagement_level(df):
    mapping_expr = when(col("EngagementLevel") == "Low", 1)\
                    .when(col("EngagementLevel") == "Medium", 2)\
                    .when(col("EngagementLevel") == "High", 3)\
                    .otherwise(0)
    return df.withColumn("EngagementScore", mapping_expr)

def compare_engagement_levels(df):
    df_mapped = map_engagement_level(df)
    result_df = df_mapped.groupBy("JobTitle")\
                         .agg(spark_round(avg("EngagementScore"), 2).alias("AvgEngagementLevel"))
    return result_df

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    spark = initialize_spark()
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-nikilteja15/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-nikilteja15/outputs/engagement_levels_job_titles.csv"
    df = load_data(spark, input_file)
    result_df = compare_engagement_levels(df)
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
