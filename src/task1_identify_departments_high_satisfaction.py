# task1_identify_departments_high_satisfaction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, coalesce, concat, format_string
from pyspark.sql.functions import col, count, when, round as spark_round


def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): The SparkSession object.
        file_path (str): Path to the employee_data.csv file.

    Returns:
        DataFrame: Spark DataFrame containing employee data.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv(file_path, header=True, schema=schema)

    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments where more than 60% of employees have a Satisfaction Rating > 4 and Engagement Level 'High'.
    """
    # Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'
    high_satisfaction_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == 'High'))
    
    # Count total employees in each department
    total_employees = df.groupBy("Department").count().withColumnRenamed("count", "TotalEmployeesCount")
    
    # Count high satisfaction employees in each department
    high_satisfaction_count = high_satisfaction_df.groupBy("Department").count().withColumnRenamed("count", "HighSatisfactionCount")
    
    # Join the two DataFrames
    department_stats = total_employees.join(high_satisfaction_count, on="Department", how="left")
    
    # Replace NULL values in HighSatisfactionCount with 0
    department_stats = department_stats.withColumn("HighSatisfactionCount", coalesce(col("HighSatisfactionCount"), lit(0)))
    
    # Ensure division is performed correctly
    department_stats = department_stats.withColumn("TotalEmployeesCount", col("TotalEmployeesCount").cast("double"))
    department_stats = department_stats.withColumn("HighSatisfactionCount", col("HighSatisfactionCount").cast("double"))

    # Calculate percentage correctly
    result_df = department_stats.withColumn(
        "HighSatisfactionPercentage",
        (col("HighSatisfactionCount") / col("TotalEmployeesCount")) * 100
    )

    # Round and format with percentage symbol
    result_df = result_df.withColumn(
        "HighSatisfactionPercentageFormatted",
        concat(format_string("%.2f", spark_round(col("HighSatisfactionPercentage"), 1)), lit("%"))
    )

    # Filter based on numeric value (not the formatted string)
    result_df = result_df.filter(col("HighSatisfactionPercentage") > 5)
    
    # Select relevant columns
    result_df = result_df.select("Department", "HighSatisfactionPercentageFormatted")
    
    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.

    Parameters:
        result_df (DataFrame): Spark DataFrame containing the result.
        output_path (str): Path to save the output CSV file.

    Returns:
        None
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-SriLaxmiPrasannaJoginipelli/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-SriLaxmiPrasannaJoginipelli/outputs/task1/departments_high_satisfaction.csv"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
