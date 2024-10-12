import glob
import os
import sqlite3
from pathlib import Path

import pandas as pd


def parquet_to_sqlite(parquet_folder_str: str, sqlite_db=None):
    # Prompt the user for the database file name if not provided
    if sqlite_db is None:
        db_name = input(
            "Enter the name for the SQLite database file (without extension): "
        )
        folder = os.getcwd()
        sqlite_db = os.path.join(folder, f"{db_name}.db")

    # Connect to SQLite database (it will create the database file if it doesn't exist)
    conn = sqlite3.connect(sqlite_db)

    try:
        # List all parquet files in the specified folder
        parquet_files = glob.glob(os.path.join(parquet_folder_str, "*.parquet"))

        for parquet_file in parquet_files:
            # Read the parquet file into a DataFrame
            df = pd.read_parquet(parquet_file)

            # Determine table name from the file name (without extension)
            table_name = os.path.splitext(os.path.basename(parquet_file))[0]

            # Write the DataFrame to SQLite database
            df.to_sql(table_name, conn, if_exists="replace", index=True)
            print(f"Table '{table_name}' created successfully in SQLite database.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the database connection
        conn.close()
        print(f"SQLite database saved to {sqlite_db}")


def main():
    # Folder containing parquet files, set to current working directory
    parquet_folder = os.getcwd()
    print(parquet_folder)

    # Optional: specify a custom output database file path
    # sqlite_db = 'custom_output_path/output.db'  # Replace with your desired database file path

    # Convert parquet files to SQLite
    parquet_to_sqlite(parquet_folder, "fl_outages.db")


if __name__ == "__main__":
    main()




