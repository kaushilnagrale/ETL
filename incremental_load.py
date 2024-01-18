import os
import json
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

# Function to incrementally load data into a CSV file with upsert operation
def incremental_load_csv_upsert(csv_path, new_data, unique_key):
    if os.path.exists(csv_path):
        # If the CSV file exists, load existing data
        existing_data = pd.read_csv(csv_path)
        print(existing_data.head())
        print(new_data.head())
        # Merge existing data with new data based on the unique key
        merged_data = pd.merge(existing_data, new_data, how='outer')
        print(merged_data.head())

        # Use the new data where it exists, otherwise use the existing data
        combined_data = existing_data.combine_first(merged_data)

        # Drop duplicate rows based on the unique key
        combined_data.drop_duplicates(subset=[unique_key], keep='last', inplace=True)
    else:
        # If the CSV file doesn't exist, create a new file with the new data
        combined_data = new_data
    
    # Write the combined data back to the CSV file
    combined_data.to_csv(csv_path, index=False)

# Get the absolute path of the script's directory
try:
    script_path = os.path.abspath(__file__)
    script_directory = os.path.dirname(script_path)
except NameError:
    # If __file__ is not defined (e.g., in an interactive environment), use the current working directory
    script_directory = os.getcwd()


# Replace "amineoumous/50-startups-data" with the desired dataset
dataset_name = "kaushilnagrale/test123"

# Specify the directory where you want to download the dataset
download_path = os.path.join(script_directory, "data")

# Set the current working directory
os.chdir(script_directory)

# Create the Kaggle API object
api = KaggleApi()
api.authenticate()

# Download the dataset
api.dataset_download_files(dataset_name, path=download_path, unzip=True)

# Read the downloaded CSV file into a DataFrame
new_data = pd.read_csv(os.path.join(download_path, "test.csv"))

# Specify the path for the CSV file where you want to incrementally load the data
csv_file_path = os.path.join(script_directory, "combined_data.csv")

# Specify the unique key for upsert operation (replace 'unique_column_name' with the actual column name)
unique_key_column = 'id'

# Incrementally load the new data into the CSV file with upsert operation
incremental_load_csv_upsert(csv_file_path, new_data, unique_key_column)

