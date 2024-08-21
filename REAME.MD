# Strucure of the USZ data 
main/
├── subdir1/
│   ├── file1.gz
│   ├── file2.gz
│   └── ...
├── subdir2/
│   ├── file3.gz
│   ├── file4.gz
│   └── ...
└── ...


# Functions:
read_and_process_data(filepath):

Takes the filepath of a compressed data file (.gz extension) as input.
Extracts the filename without extension and uses it to create a new column name.
Reads the file using Polars' lazy evaluation (pl.scan_csv) for efficiency.
Renames the second column based on the constructed new column name.
Returns the processed data as a Polars DataFrame using collect().
anonymize_timestamps(df):

Takes a Polars DataFrame (df) as input.
Checks if the DataFrame contains a 'timestamp' column. Raises an error if not.
Replaces the year in the 'timestamp' column with a random year between 1900 and 2099 for anonymization.
Returns the DataFrame with anonymized timestamps.
merge_data_files(main_dir):

Takes the path to the main directory (main_dir) containing data folders as input.
Iterates through subdirectories within the main directory.
For each subdirectory:
Defines an output filename with the subdirectory name and '.csv' extension.
Uses glob.glob to collect all file paths with the '.gz' extension within the subdirectory.
Initializes an empty list to store DataFrames.
Iterates through the file paths:
Reads and processes each file using read_and_process_data.
Appends the processed DataFrame to the list.
Concatenates all DataFrames in the list using pl.concat.
Anonymizes timestamps in the concatenated DataFrame using anonymize_timestamps.
Saves the anonymized data to a CSV file with the defined output filename using write_csv.
Prints a message indicating where the merged data is saved.
Prints a message indicating that all data files within the main directory have been processed.