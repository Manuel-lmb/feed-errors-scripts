# Feed Indexation Error Report Scripts
---
This repository contains three Python scripts that work together to generate a report on feed indexation errors. The process involves identifying errors, grouping feeds by their URLs, and then joining the two datasets to produce a final report.

## Prerequisites
Before running the scripts, you need to install the necessary libraries and obtain a database connection string.

* **Python:** Install Python from the [official website](https://www.python.org/downloads/).
* **Libraries:** Install the required Python libraries using `pip`:
    ```bash
    pip install pymongo openpyxl requests
    ```
* **SRV Link:** You'll need an SRV link to access the database. Contact your team lead for this connection string.

## Script 1: `SearchError.py`
This script connects to the database, queries for errors older than a specified number of days, and exports the results to an Excel file.

* **Database Connection:** On line 10, replace the placeholder connection string with your personal SRV link.
    ```python
    client = MongoClient('mongodb+srv://r_persona:link')
    ```
* **Error Age:** On line 13, you can adjust the number of days an error must be older than to be considered for the report. The default is **24 days**.
    ```python
    days_subtract = 24
    ```
* **Output:** The script generates an Excel file containing the feed URL, error type, start date, and the number of days since the error began. The filename is based on the `days_subtract` value and the current date.

## Script 2: `group_feed.py`
This script processes a separate Excel file of feeds and customers, grouping the data by the `feed_url` to create a new file.

* **Input File Path:** On line 43, update the `file_path` variable to the correct location of your "Feeds with customers.xlsx" file.
    ```python
    file_path = 'Feeds with customers.xlsx'
    ```
* **Output Filename:** You can change the name of the resulting file on line 53.
    ```python
    resultName = "Group_feed_url"
    ```

## Script 3: `Join.py`
The final script merges the output from `SearchError.py` and `group_feed.py` to create the final report.

* **File Paths:** You must update the file paths and column names on lines 50-54 to match the names of your generated files.
    ```python
    main_file_path="feed_errors.xlsx"          # File with feed errors
    main_join_column="Feed_URL"
    customer_file_path="Group_feed_url.xlsx"   # File with feeds grouped by URL
    customer_join_column="feed_url"
    output_file_path="final_report.xlsx"       # Name of the final report file
    ```