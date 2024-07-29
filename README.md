# Data_Transformation_using_Apache_Spark
Small Data Transformation project using Apache Spark on IPL - a cricket tournament dataset 

These are the below operations and the respective functions used in Apache Spark for the transformations.

**1. Importing Required Libraries and Initializing SparkSession:**
The script begins by importing necessary modules and functions from PySpark, such as `StructType`, `StructField`, and data types like `StringType` and `IntegerType`. Additionally, it imports functions for column operations, aggregations, and windowing functions. A SparkSession is then initialized, which serves as the entry point for interacting with Spark.

**2. Defining Schemas for DataFrames:**
The code defines specific schemas for several datasets (Ball_by_Ball, Match, Player, Player_match, and Team) using `StructType` and `StructField`. This helps in setting the correct data types for each column, ensuring that the data is properly structured when loaded into Spark DataFrames.

**3. Loading Data from AWS S3:**
Using the defined schemas, the script reads CSV files from AWS S3 buckets into PySpark DataFrames. This includes loading data related to ball-by-ball details, match specifics, player information, player match data, and team details. The `option("header", "true")` ensures that the first row is treated as header information.

**4. Filtering and Aggregating Data:**
The script filters the Ball_by_Ball DataFrame to exclude extra deliveries like wides and no balls, focusing only on valid deliveries. It then aggregates data to calculate total and average runs scored in each match and inning, providing a summary of the scoring patterns.

**5. Applying Window Functions:**
A window specification is defined to calculate running totals of runs scored within each match and inning, using the `over` function. This helps in analyzing scoring trends over the course of an inning or match.

**6. Adding Derived Columns:**
The script introduces new columns to the Ball_by_Ball DataFrame. For instance, the `high_impact` column identifies deliveries that had a significant impact, such as scoring more than six runs or resulting in a wicket. This is done using conditional logic with the `when` function.

**7. Extracting Date Components:**
From the Match DataFrame, additional columns are derived to extract year, month, and day from the `match_date`. This is useful for time-based analyses, such as examining seasonal trends or daily performance variations.

**8. Data Cleaning and Normalization:**
Player names are normalized by converting them to lowercase and removing any non-alphanumeric characters. Missing values in columns like `batting_hand` and `bowling_skill` are handled by filling them with a default value ('unknown'). Additionally, players are categorized based on their batting hand as 'Left-Handed' or 'Right-Handed'.

**9. Enriching Player Match Data:**
The Player_match DataFrame is enriched with new columns like `veteran_status`, which classifies players as 'Veteran' or 'Non-Veteran' based on their age at the time of the match. Another column, `years_since_debut`, calculates the number of years since a player's debut, providing insights into their experience and longevity.

**10. SQL Queries and Data Visualization:**
The code executes SQL queries to derive insights, such as identifying top-scoring batsmen per season, economical bowlers in powerplay overs, and the impact of winning the toss on match outcomes. It also performs venue analysis, assessing average and maximum scores at different venues. Results are converted to Pandas DataFrames and visualized using Matplotlib and Seaborn, presenting the data in a clear and interpretable format.

**11. Analysis of Toss Impact:**
An additional analysis explores the relationship between winning the toss and winning the match. The script categorizes matches based on whether the toss winner also won the match, allowing for a deeper understanding of the strategic importance of the toss.


