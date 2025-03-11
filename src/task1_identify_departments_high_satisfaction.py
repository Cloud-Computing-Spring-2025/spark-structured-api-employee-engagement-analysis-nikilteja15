# task1_identify_departments_high_satisfaction.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, format_string

def initialize_spark(app_name="Task1_Identify_Departments"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    # Filter employees based on SatisfactionRating > 4 and EngagementLevel == 'High'
    filtered_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))
    
    # Count qualified employees per department
    department_counts = filtered_df.groupBy("Department").agg(count("*").alias("QualifiedCount"))
    
    # Count total employees per department
    total_counts = df.groupBy("Department").agg(count("*").alias("TotalCount"))

    # Calculate percentages and filter departments with more than 10%
    percentage_df = department_counts.join(total_counts, "Department")\
                                    .withColumn("Percentage", format_string("%.2f%%", (col("QualifiedCount") / col("TotalCount") * 100)))\
                                    .filter(col("Percentage").substr(0, 4) > "1.00")

    return percentage_df.select("Department", "Percentage")

def write_output(result_df, output_path):
    # Ensure the result is written with headers and in a single part file
    result_df.coalesce(1).write.option("header", "true").csv(output_path, mode='overwrite')

def main():
    spark = initialize_spark()
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-nikilteja15/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-nikilteja15/outputs/departments_high_satisfaction.csv"
    df = load_data(spark, input_file)
    result_df = identify_departments_high_satisfaction(df)
    write_output(result_df, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
