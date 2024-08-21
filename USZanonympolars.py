import chardet
import gzip
from io import StringIO
import polars as pl
import os
import random

def read_and_process_data(file_path, common_year):
    """
    Reads a gzip-compressed CSV file, processes the data, and returns a DataFrame.

    Args:
        file_path (str): Path to the CSV file.
        common_year (int): Common year to be used for timestamps.

    Returns:
        pl.DataFrame: Processed DataFrame or None if errors occur.
    """

    base_name = os.path.basename(file_path)
    new_column_name = base_name.split('-', 1)[0]

    # Detect the file encoding
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    encoding = result['encoding']

    # Read the file content using the detected encoding
    try:
        with gzip.open(file_path, 'rt', encoding=encoding) as f:
            file_content = f.read()
    except (OSError, UnicodeDecodeError) as e:
        print(f"Error opening or decoding file: {file_path}. Skipping file. ({e})")
        return None

    # Convert the file content to UTF-8
    file_content_utf8 = file_content.encode('utf-8')

    # Use StringIO to simulate a file object from the UTF-8 encoded string
    file_like_object = StringIO(file_content_utf8.decode('utf-8'))

    # Read the CSV file from the file-like object
    df = pl.read_csv(file_like_object)
    df = df.rename({"value": new_column_name})

    # Timestamp processing with improved error handling
    if "timestamp" in df.columns:
        try:
            # Attempt two different parsing formats
            df = df.with_columns([
                pl.col("timestamp").str.replace(r'^\d{4}', str(common_year)).alias("new_timestamp")
            ])
            df = df.with_columns([
                pl.col("new_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S.%f")
            ])
            df = df.drop("timestamp").rename({"new_timestamp": "timestamp"})
        except ValueError:
            try:
                df = df.with_columns([
                    pl.col("timestamp").str.replace(r'^\d{4}', str(common_year)).alias("new_timestamp")
                ])
                df = df.with_columns([
                    pl.col("new_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                ])
                df = df.drop("timestamp").rename({"new_timestamp": "timestamp"})
            except ValueError:
                print(f"Unable to parse timestamps in {file_path}. Skipping file.")
                return None
    else:
        print(f"'timestamp' column not found in {file_path}. Skipping timestamp processing.")

    return df

def merge_data_files(main_dir):
    """
    Merges gzip-compressed CSV files from a directory into a single CSV and Parquet file.

    Args:
        main_dir (str): Path to the main directory containing CSV files.
    """

    used_years = set()  # Set to keep track of used years

    def generate_unique_year():
        """
        Generates a unique random year between 1900 and 2099.
        """
        while True:
            year = random.randint(1900, 2099)
            if year not in used_years:
                used_years.add(year)
                return year

    common_year = generate_unique_year()  # Generate a unique common year

    lazy_dfs = []  # List to store DataFrames for processing
    all_column_names = set()  # Set to store all unique column names
    column_types = {}  # Dictionary to track column types and their frequencies

    for subdir, dirs, files in os.walk(main_dir):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(subdir, file)
                processed_df = read_and_process_data(file_path, common_year)
                if processed_df is not None:
                    lazy_dfs.append(processed_df)
                    all_column_names.update(processed_df.columns)
                    for col in processed_df.columns:
                        # Update column_types with the most common type for each column
                        col_type = str(processed_df[col].dtype)
                        column_types[col] = column_types.get(col, {})
                        column_types[col][col_type] = column_types[col].get(col_type, 0) + 1

    # Determine the most common type for each column
    for col, types in column_types.items():
        most_common_type = max(types, key=types.get)
        column_types[col] = most_common_type

    # Ensure all dataframes have the same schema and types
    for i, df in enumerate(lazy_dfs):
        for col in all_column_names:
            if col not in df.columns:
                # Determine the correct data type for the missing column
                col_type = column_types[col]
                if col_type == "Float64":
                    df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
                elif col_type == "Int64":
                    df = df.with_columns(pl.lit(None).cast(pl.Int64).alias(col))
                elif col_type == "String":
                    df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))
                # Add other types as needed
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        lazy_dfs[i] = df

    if lazy_dfs:
        # Align columns in all dataframes before concatenation
        aligned_dfs = [df.select(sorted(all_column_names)) for df in lazy_dfs]
        merged_df = pl.concat(aligned_dfs)

        # Save the merged DataFrame with a common random year in timestamps
        output_file_csv = os.path.join(main_dir, "merged_data.csv")
        output_file_parquet = os.path.join(main_dir, "merged_data.parquet")
        merged_df.write_csv(output_file_csv)
        merged_df.write_parquet(output_file_parquet)
        print(f"Merged data saved to {output_file_csv} and {output_file_parquet}")
    else:
        print("No data files found to merge.")

# Example usage
main_dir = 'a92cf97'
merge_data_files(main_dir)
