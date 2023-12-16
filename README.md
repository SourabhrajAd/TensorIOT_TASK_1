# PySpark Data Analysis and Integration Example

This example demonstrates data analysis using PySpark, focusing on finding information about video game reviews and integrating the results into a SQL Server database.

## Prerequisites

Before running the code, make sure you have the following:
- Install Apache Spark and Necessary Tools
- Install Apache Spark on your local machine. You can follow the official Apache Spark documentation for installation 
  instructions: https://spark.apache.org/docs/latest/.
- Set Up Jupyter Notebook
  Launch Jupyter Notebook and create a new notebook for your project.
- PySpark installed
- A SQL Server instance with the necessary credentials
- A JSON file containing video game reviews (e.g., Video_Games.json)

## Code Overview

### 1. Data Analysis with PySpark

- Import necessary PySpark libraries.
- Create a Spark session.
- Read a JSON file containing video game reviews into a PySpark DataFrame.
- Drop the "style" column due to inconsistent JSON syntax.

#### Ratings Analysis:

- Find the item with the least rating.
- Find the item with the most rating.

#### Review Analysis:

- Add a new column 'review_length' representing the length of each review.
- Find the item with the longest review.

#### Date Formatting:

- Convert 'reviewTime' to a timestamp and format the date as MM-DD-YYYY.
- Show the DataFrame with formatted dates.

### 2. Integration with SQL Server

- Define SQL Server connection properties.
- Specify the SQL Server table name.
- Write the DataFrame to SQL Server.

### 3. Parquet File Creation

- Specify the path to save the Parquet file.
- Write the DataFrame to a Parquet file.

## Usage

1. Replace placeholders (`your_username`, `your_password`, `your_server`, `your_port`, `your_database`, `your_table_name`) in the code with your SQL Server details.
2. Replace the `json_file_path` variable with the path to your JSON file.
3. Run the code.

## Note

- Ensure that the required PySpark libraries are installed.
- Adjust the paths and connection details as needed.

Feel free to explore and modify the code according to your specific requirements.
