# Employee Engagement Analysis Assignment

## **Prerequisites**

## **Prerequisites**

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Apache Spark**:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

4. **Docker & Docker Compose** (Optional):
   - [Docker Installation Guide](https://docs.docker.com/get-docker/)
   - [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

5. **HDFS on Docker** (Optional):
   - [HDFS, Hive, Hue, Spark](https://github.com/Wittline/apache-spark-docker)
   - [HDFS on Spark](https://github.com/big-data-europe/docker-hadoop-spark-workbench)

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
EmployeeEngagementAnalysis/
├── input/
│   └── employee_data.csv
├── outputs/
│   ├── departments_high_satisfaction.csv
│   ├── valued_no_suggestions.txt
│   └── engagement_levels_job_titles.csv
├── src/
│   ├── task1_identify_departments_high_satisfaction.py
│   ├── task2_valued_no_suggestions.py
│   └── task3_compare_engagement_levels.py
├── docker-compose.yml
└── README.md
```

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally or using Docker.

#### **a. Running Locally**

1. **Navigate to the Project Directory**:
   ```bash
   cd EmployeeEngagementAnalysis/
   ```

2. **Execute Each Task Using `spark-submit`**:
   ```bash
   spark-submit src/task1_identify_departments_high_satisfaction.py
   spark-submit src/task2_valued_no_suggestions.py
   spark-submit src/task3_compare_engagement_levels.py
   ```

3. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```
   You should see:
   - `departments_high_satisfaction.csv`
   - `valued_no_suggestions.txt`
   - `engagement_levels_job_titles.csv`

#### **b. Running with Docker (Optional)**

1. **Start the Spark Cluster**:
   ```bash
   docker-compose up -d
   ```

2. **Access the Spark Master Container**:
   ```bash
   docker exec -it spark-master bash
   ```

3. **Navigate to the Spark Directory**:
   ```bash
   cd /opt/bitnami/spark/
   ```

4. **Run Your PySpark Scripts Using `spark-submit`**:
   ```bash
   spark-submit src/task1_identify_departments_high_satisfaction.py
   spark-submit src/task2_valued_no_suggestions.py
   spark-submit src/task3_compare_engagement_levels.py
   ```

5. **Exit the Container**:
   ```bash
   exit
   ```

6. **Verify the Outputs**:
   On your host machine, check the `outputs/` directory for the resulting files.

7. **Stop the Spark Cluster**:
   ```bash
   docker-compose down
   ```

## **Assignment Tasks**

### **Task 1: Identify Departments with High Satisfaction and Engagement**

**Objective:**
Determine which departments have more than **1%** of their employees with a **Satisfaction Rating greater than 4** and an **Engagement Level of 'High'**.

**Script:**  
`src/task1_identify_departments_high_satisfaction.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, format_string

def initialize_spark(app_name="Task1_Identify_Departments"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, file_path):
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    filtered_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))
    department_counts = filtered_df.groupBy("Department").agg(count("*").alias("QualifiedCount"))
    total_counts = df.groupBy("Department").agg(count("*").alias("TotalCount"))
    percentage_df = department_counts.join(total_counts, "Department")\
                                    .withColumn("Percentage", format_string("%.2f%%", (col("QualifiedCount") / col("TotalCount") * 100)))\
                                    .filter(col("Percentage").substr(0, 4) > "1.00")
    return percentage_df.select("Department", "Percentage")

def main():
    spark = initialize_spark()
    input_file = "input/employee_data.csv"
    output_file = "outputs/departments_high_satisfaction.csv"
    df = load_data(spark, input_file)
    result_df = identify_departments_high_satisfaction(df)
    result_df.coalesce(1).write.option("header", "true").csv(output_file, mode='overwrite')
    spark.stop()

if __name__ == "__main__":
    main()
```

**Expected Output (departments_high_satisfaction.csv):**
```
Department,Percentage
Finance,4.35%
Sales,5.88%
Marketing,9.09%
IT,15.00%
```

---

### **Task 2: Who Feels Valued but Didn’t Suggest Improvements?**
**Objective:**
Identify employees who feel valued **(Satisfaction Rating ≥ 4)** but have not provided suggestions. This helps analyze whether employees who feel appreciated are actively contributing suggestions for improvements within the organization.

**Script:**  
`src/task2_valued_no_suggestions.py`

```python
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

```

**Expected Output (valued_no_suggestions.txt):**
```
Number of Employees Feeling Valued without Suggestions: 18
Proportion: 18.00%
```

---

### **Task 3: Compare Engagement Levels Across Job Titles**

**Objective:**
Examine how engagement levels vary across different job titles and determine which **Job Title** has the highest **average Engagement Level**.

**Script:**  
`src/task3_compare_engagement_levels.py`

```python
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

```

**Expected Output (engagement_levels_job_titles.csv):**
```
JobTitle,AvgEngagementLevel
Coordinator,1.82
Developer,2.14
Executive,1.97
Analyst,1.95
Support,1.6
Manager,1.88
```

---

## **Conclusion**
This project applies **PySpark** to analyze employee engagement and satisfaction data, identifying insights into employee behavior and department performance. By running the three analysis tasks, key trends in employee satisfaction and engagement are revealed, helping organizations make data-driven decisions.

For any issues or questions, refer to the **comments inside each script file** or check the **debugging logs** when running the tasks.

