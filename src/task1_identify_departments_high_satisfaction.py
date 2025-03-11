# task1_identify_departments_high_satisfaction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, coalesce
from pyspark.sql.functions import col, count, when, round as spark_round

# def initialize_spark(app_name="Task1_Identify_Departments"):
#     """
#     Initialize and return a SparkSession.
#     """
#     spark = SparkSession.builder \
#         .appName(app_name) \
#         .getOrCreate()
#     return spark

def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

# def load_data(spark, file_path):
#     """
#     Load the employee data from a CSV file into a Spark DataFrame.

#     Parameters:
#         spark (SparkSession): The SparkSession object.
#         file_path (str): Path to the employee_data.csv file.

#     Returns:
#         DataFrame: Spark DataFrame containing employee data.
#     """
#     schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
#    # df = spark.read.csv(file_path, header=True, schema=schema)
#     df = spark.read.csv('/workspaces/spark-structured-api-employee-engagement-analysis-SriLaxmiPrasannaJoginipelli/employee_data.csv', header=True, schema=schema)

#     return df

# def identify_departments_high_satisfaction(df):
#     """
#     Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.

#     Parameters:
#         df (DataFrame): Spark DataFrame containing employee data.

#     Returns:
#         DataFrame: DataFrame containing departments meeting the criteria with their respective percentages.
#     """
#     # TODO: Implement Task 1
#     # Steps:
#     # 1. Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'.
#     # 2. Calculate the percentage of such employees within each department.
#     # 3. Identify departments where this percentage exceeds 50%.
#     # 4. Return the result DataFrame.

#     pass  # Remove this line after implementing the function

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

# def identify_departments_high_satisfaction(df):
#     """
#     Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.
#     """
#     # Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'
#     high_satisfaction_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == 'High'))
    
#     # Count total employees in each department
#     total_employees = df.groupBy("Department").count().alias("TotalEmployees")
    
#     # Count high satisfaction employees in each department
#     high_satisfaction_count = high_satisfaction_df.groupBy("Department").count().alias("HighSatisfactionCount")
    
#     # Join the two DataFrames
#     department_stats = total_employees.join(high_satisfaction_count, on="Department", how="left")
    
#     # Calculate the percentage of high satisfaction employees in each department
#     result_df = department_stats.withColumn("HighSatisfactionPercentage", 
#                                             (col("HighSatisfactionCount") / col("count")) * 100)
    
#     # Filter departments where the percentage exceeds 50%
#     result_df = result_df.filter(col("HighSatisfactionPercentage") > 50)
    
#     # Select relevant columns and round the percentage to 2 decimal places
#     result_df = result_df.select("Department", spark_round("HighSatisfactionPercentage", 2).alias("HighSatisfactionPercentage"))
    
#     return result_df


def identify_departments_high_satisfaction(df):
    """
    Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.
    """
    # Filter employees with SatisfactionRating > 4 and EngagementLevel == 'High'
    high_satisfaction_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == 'High'))
    high_satisfaction_df.show()  # Debugging print

    # Count total employees in each department
    total_employees = df.groupBy("Department").count().alias("TotalEmployees")
    total_employees.show()  # Debugging print
    
    # Count high satisfaction employees in each department
    high_satisfaction_count = high_satisfaction_df.groupBy("Department").count().alias("HighSatisfactionCount")
    high_satisfaction_count.show()  # Debugging print
    
    # Rename the count columns to avoid ambiguity
    total_employees = total_employees.withColumnRenamed("count", "TotalEmployeesCount")
    high_satisfaction_count = high_satisfaction_count.withColumnRenamed("count", "HighSatisfactionCount")
    
    # Join the two DataFrames
    department_stats = total_employees.join(high_satisfaction_count, on="Department", how="left")
    department_stats.show()  # Debugging print
    
    # Replace NULL values in HighSatisfactionCount with 0 to avoid issues in percentage calculation
    department_stats = department_stats.withColumn("HighSatisfactionCount", coalesce(col("HighSatisfactionCount"), lit(0)))
    
    # Calculate the percentage of high satisfaction employees in each department
    result_df = department_stats.withColumn("HighSatisfactionPercentage", 
                                            (col("HighSatisfactionCount") / col("TotalEmployeesCount")) * 100)
    result_df.show()  # Debugging print
    
    # Filter departments where the percentage exceeds 50%
    result_df = result_df.filter(col("HighSatisfactionPercentage") > 5)
    
    # Select relevant columns and round the percentage to 2 decimal places
    result_df = result_df.select("Department", spark_round("HighSatisfactionPercentage", 2).alias("HighSatisfactionPercentage"))
    
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

# def main():
#     """
#     Main function to execute Task 1.
#     """
#     # Initialize Spark
#     spark = initialize_spark()
    
#     # Define file paths
#     input_file = "/workspaces/Employee_Engagement_Analysis_Spark/input/employee_data.csv"
#     output_file = "/workspaces/Employee_Engagement_Analysis_Spark/outputs/task1/departments_high_satisfaction.csv"
    
#     # Load data
#     df = load_data(spark, input_file)
    
#     # Perform Task 1
#     result_df = identify_departments_high_satisfaction(df)
    
#     # Write the result to CSV
#     write_output(result_df, output_file)
    
#     # Stop Spark Session
#     spark.stop()

# if __name__ == "__main__":
#     main()


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
