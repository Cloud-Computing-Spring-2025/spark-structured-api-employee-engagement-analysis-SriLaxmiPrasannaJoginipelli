from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def initialize_spark(app_name="Task2_Valued_No_Suggestions"):
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
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def identify_valued_no_suggestions(df):
    """
    Find employees who feel valued but have not provided suggestions and calculate their proportion.
    """
    valued_no_suggestions = df.filter((col("SatisfactionRating") >= 4) & (col("ProvidedSuggestions") == False))
    total_employees = df.count()
    valued_no_suggestions_count = valued_no_suggestions.count()
    proportion = (valued_no_suggestions_count / total_employees) * 100
    
    return valued_no_suggestions_count, round(proportion, 2)

def write_output_to_csv(number, proportion, output_path):
    """
    Write the results to a CSV file in outputs/task2/valued_no_suggestions.csv.
    """
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)

    # Convert results into a Spark DataFrame
    spark = SparkSession.builder.getOrCreate()
    result_df = spark.createDataFrame([(number, f"{proportion}%")], ["Number of Employees Feeling Valued without Suggestions", "Proportion"])

    # Save the output as a CSV file
    output_file = os.path.join(output_path, "valued_no_suggestions.csv")
    result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file)

def main():
    """
    Main function to execute Task 2.
    """
    spark = initialize_spark()
    
    # Define paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-SriLaxmiPrasannaJoginipelli/employee_data.csv"
    output_directory = "/workspaces/spark-structured-api-employee-engagement-analysis-SriLaxmiPrasannaJoginipelli/outputs/task2/valued_no_suggestions"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Process data
    number, proportion = identify_valued_no_suggestions(df)  # Unpack values
    
    # Write output as CSV
    write_output_to_csv(number, proportion, output_directory)
    
    # Stop Spark
    spark.stop()

if __name__ == "__main__":
    main()
