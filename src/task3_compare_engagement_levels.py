# task1_identify_departments_high_satisfaction.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task1_Identify_Departments"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    filtered_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))
    print("Filtered DataFrame:")
    filtered_df.show(truncate=False)

    department_counts = filtered_df.groupBy("Department").agg(count("*").alias("QualifiedCount"))
    print("Department Counts:")
    department_counts.show(truncate=False)

    total_counts = df.groupBy("Department").agg(count("*").alias("TotalCount"))
    print("Total Counts:")
    total_counts.show(truncate=False)

    percentage_df = department_counts.join(total_counts, "Department")\
                                     .withColumn("Percentage", (col("QualifiedCount") / col("TotalCount") * 100))\
                                     .filter(col("Percentage") > 50)
    print("Final Percentage DataFrame:")
    percentage_df.show(truncate=False)
    
    return percentage_df.select("Department", "Percentage")

def write_output(result_df, output_path):
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

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
