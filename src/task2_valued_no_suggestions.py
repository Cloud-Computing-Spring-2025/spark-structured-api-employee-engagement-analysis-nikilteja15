# task2_valued_no_suggestions.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Valued_No_Suggestions"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_valued_no_suggestions(df):
    valued_df = df.filter((col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False))
    number = valued_df.count()
    total = df.count()
    proportion = (number / total * 100)
    return number, proportion

def write_output(number, proportion, output_path):
    with open(output_path, 'w') as f:
        f.write(f"Number of Employees Feeling Valued without Suggestions: {number}\n")
        f.write(f"Proportion: {proportion:.2f}%\n")

def main():
    spark = initialize_spark()
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-nikilteja15/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-nikilteja15/outputs/valued_no_suggestions.txt"
    df = load_data(spark, input_file)
    number, proportion = identify_valued_no_suggestions(df)
    write_output(number, proportion, output_file)
    spark.stop()

if __name__ == "__main__":
    main()
