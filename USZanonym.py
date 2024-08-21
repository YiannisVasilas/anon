import os
import pandas as pd
import random

def read_and_process_data(file_path, common_year):
    base_name = os.path.basename(file_path)
    new_column_name = base_name.split('-', 1)[0]
    
    df = pd.read_csv(file_path, compression='gzip')
    df.rename(columns={'value': new_column_name}, inplace=True)
    
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], infer_datetime_format=True)
        # Use the common year for all timestamps, keeping the rest of the timestamp intact
        df['timestamp'] = df['timestamp'].apply(lambda x: x.replace(year=common_year))
    else:
        print(f"'timestamp' column not found in {file_path}. Skipping timestamp processing.")
    return df

def generate_unique_year(used_years):
    while True:
        year = random.randint(1900, 2099)
        if year not in used_years:
            used_years.add(year)
            return year

def merge_data_files(main_dir, used_years):
    common_year = generate_unique_year(used_years)  # Generate a unique random year
    
    merged_df = pd.DataFrame()
    for subdir, dirs, files in os.walk(main_dir):
        for file in files:
            if file.endswith(".gz"):
                file_path = os.path.join(subdir, file)
                df = read_and_process_data(file_path, common_year)
                merged_df = pd.concat([merged_df, df], ignore_index=True)
    
    # Save the merged DataFrame with a common random year in timestamps
    output_parquet_file = os.path.join(main_dir, "merged_data.parquet")
    output_csv_file = os.path.join(main_dir, "merged_data.csv")
    
    merged_df.to_parquet(output_parquet_file, index=False)
    merged_df.to_csv(output_csv_file, index=False)

    print(f"Merged data saved to {output_parquet_file} and {output_csv_file}")

# Example usage
main_dirs = ['a92cf97']  # List of directories to process
used_years = set()

for main_dir in main_dirs:
    merge_data_files(main_dir, used_years)

print(f"All data files processed within the specified directories.")