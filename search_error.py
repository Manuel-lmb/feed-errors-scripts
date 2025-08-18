# import libraries
from pymongo import MongoClient
from datetime import datetime, timedelta
import openpyxl
import requests
from requests.exceptions import ConnectionError, Timeout, RequestException

# --- DATE TO USE ---
# Connecting to the URI
client = MongoClient('mongodb+srv://r_personal:link') # Replace whit your srv link

# Days the error needs to be older than
days_subtract = 24 # you can change the date
now = datetime.now()
error_days_ago = now - timedelta(days=days_subtract)

print(f"Filtering for errorDate (first error after last IDLE) older than: {error_days_ago}")

# Selecting the DB
db = client.feedreader

# --- Web Request Function ---
def feed_error_type(feed_url: str) -> str:
    """
    Try to search the feed URl and determine the error by it HTTP state.
    """
    try:
        # Create a User-Agent to simulate a browser
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(feed_url, timeout=10, allow_redirects=True, headers=headers)

        sCode = response.status_code

        # Check for redirects
        if response.history:
            # The last response in history is the one that redirected to the final URL
            redirect_response = response.history[-1]
            return f"REDIRECTED: {redirect_response.url} -> {response.url}"

        # If the code is 200 it could be html or have some error on the xml
        content_type = response.headers.get("Content-Type", "").lower()
        if sCode == 200:
            if "text/html" in content_type:
                return "HTML_FORMAT"
            return "NOT_VALIDATED"
        else:
            return "ERROR_"+str(sCode) # return the error code

    # Other posible error if we can't reach the feed URL
    except ConnectionError:
        return "CONNECTION_ERROR"
    except Timeout:
        return "TIMEOUT_ERROR"
    except RequestException as e:
        return f"REQUEST_FAILED: {type(e).__name__}"
    except Exception as e:
        return f"UNKNOWN_ERROR: {type(e).__name__}"
    

# --- Extract the URL that have errors later than days_subtract ---
# Selecting the collection feed_status_log
feedsStatusColeccion = db.feed_status_log

# The aggregation pipeline translated from JavaScript to Python dictionary format
aggregation_pipeline = [
    {
        "$group": {
            # Select the feed URL that has at least one READING_ERROR
            "_id": "$feedUrl",
            "count": {"$sum": 1},
            "errors": {
                "$sum": {
                    "$cond": {
                        "if": {"$eq": ["$status", "READING_ERROR"]},
                        "then": 1,
                        "else": 0
                    }
                }
            },
            # This is the date of the oldest "READING_ERROR_DURING_ATTEMPT" found for this feedUrl (overall)
            "earliestReadingErrorDate": {
                "$min": {
                    "$cond": {
                        "if": {"$eq": ["$status", "READING_ERROR_DURING_ATTEMPT"]},
                        "then": "$date",
                        "else": None
                    }
                }
            },
            # Calculate the most recent "IDLE" date for this feedUrl
            "latestIdleDate": {
                "$max": {
                    "$cond": {
                        "if": {"$eq": ["$status", "IDLE"]},
                        "then": "$date",
                        "else": None
                    }
                }
            },
            # Collect all "READING_ERROR_DURING_ATTEMPT" dates into an array
            "allReadingErrorDates": {
                "$push": {
                    "$cond": {
                        "if": {"$eq": ["$status", "READING_ERROR_DURING_ATTEMPT"]},
                        "then": "$date",
                        "else": "$$REMOVE"
                    }
                }
            }
        }
    },
    {
        # Calculate 'errorDate' based on aggregated values from the previous group
        "$addFields": {
            # 'errorDate' will be the ldest READING_ERROR_ATTEMPT date after the latest IDLE,
            # or the oldest overall READING_ERROR_ATTEMPT if no IDLE occurred.
            "errorDate": {
                "$cond": {
                    "if": {"$eq": ["$latestIdleDate", None]}, 
                    "then": "$earliestReadingErrorDate",      
                    "else": {  
                        "$min": {  
                            "$filter": {  
                                "input": "$allReadingErrorDates",
                                "as": "errDate",
                                "cond": {"$gt": ["$$errDate", "$latestIdleDate"]}
                            }
                        }
                    }
                }
            }
        }
    },
    {
        # 
        "$match": {
            "errors": {"$gt": 0},  # Keep groups that have errors
            "errorDate": {"$lte": error_days_ago}  # Filter groups where 'errorDate' is older than 'error_days_ago'
        }
    },
    {
        "$lookup": {
            "from": "feeds",
            "localField": "_id",
            "foreignField": "feedUrl",
            "as": "feedInfo"
        }
    },
    {
        # Ensure there's a matching feedInfo document and its status is not "NOT_AVAILABLE"
        "$match": {
            "feedInfo": {"$ne": []},
            "feedInfo.status": {"$ne": "NOT_AVAILABLE"}
        }
    },
    {
        "$unwind": "$feedInfo"
    },
    {
        "$project": {
            "_id": 1, # The feed URL
            "errorDate": 1, # The calculated "date this error start"
            "days_error": { # the day since this error start (example: 25 days)
                "$dateDiff": {
                    "startDate": "$errorDate",
                    "endDate": "$$NOW",
                    "unit": "day",
                }
            }
        }
    }
]
# Process the search in the DB
errorCursor = feedsStatusColeccion.aggregate(aggregation_pipeline)

# --- Save everything to an Excel file ---
wb = openpyxl.Workbook() # Create a new Excel workbook
ws = wb.active # Select the active sheet
ws.title = "Error_Feeds"

# -- Write the headers
headers = [
    "Feed_URL",
    "Error_Start_Date",
    "Days_Since_Error_Start",
    "Error_type"
]
ws.append(headers)

# -- Write the data to the Excel file
print("\nInitiating web requests for 'Error_type' determination...")

for element in errorCursor:
    feed_url = element["_id"]
    calculated_error_start_date = element.get("errorDate")
    days_since_error_start = element.get("days_error")

    # --- Determine the error_type by making a web request ---
    error_type = feed_error_type(feed_url)
    print(f"  Processing URL: {feed_url} -> Error Type: {error_type}")

    # Add a row with the data to Excel
    ws.append([
        feed_url,
        calculated_error_start_date,
        days_since_error_start,
        error_type
    ])

# --- Save the Excel file ---
fecha_str = now.strftime("%Y%m%d")
excel_file_name = f"{str(days_subtract)}Days_{fecha_str}.xlsx"
wb.save(excel_file_name)

print(f"\nData saved successfully to '{excel_file_name}'")

# Close the connection
client.close()
print("\nConnection to MongoDB closed.")