import pandas as pd

def clean_stage_name(stage_name):
    # Clean and format stage name string
    return str(stage_name).strip().lower()

def pad_and_transform_csv(input_file, intermediate_output_file, final_output_file, start_col):
    try:
        # Read and pad the file
        with open(input_file, 'r') as file:
            lines = file.readlines()
            max_fields = max(len(line.split(',')) for line in lines)
        padded_lines = [','.join(line.rstrip('\n').split(',') + [''] * (max_fields - len(line.split(',')))) for line in lines]
        with open(intermediate_output_file, 'w') as file:
            file.writelines('\n'.join(padded_lines))

        # Read the padded CSV file
        df = pd.read_csv(intermediate_output_file, dtype=str, header=None)

        # Determine unique stages with cleaning
        unique_stages = set(df.iloc[:, start_col].dropna().apply(clean_stage_name).unique())
        transformed_df = pd.DataFrame()
        transformed_df['Record ID'] = df[0]

        # Add columns for each unique stage
        for stage in unique_stages:
            transformed_df[stage] = None

        # Process each row for value pairs
        for index, row in df.iterrows():
            for i in range(start_col, max_fields, 2):
                stage_value = clean_stage_name(row[i])
                date_value = row[i + 1]

                # Log the date if stage found and date not empty
                if stage_value in unique_stages and pd.notnull(date_value):
                    transformed_df.at[index, stage_value] = date_value

        # Save the transformed data
        transformed_df.to_csv(final_output_file, index=False)
    except Exception as e:
        print(f"Error during padding and transformation: {str(e)}")




# File paths for the input, intermediate, and output CSV files
input_file = 'path/history.csv'  # Replace with your input CSV file path
intermediate_output_file = 'padded.csv'  # Intermediate file path
final_output_file = 'output.csv'  # Final output file path
start_col = 1  # Replace with the column index where the current value is (starts at 0)

# Run the padding and transformation process
pad_and_transform_csv(input_file, intermediate_output_file, final_output_file, start_col)
