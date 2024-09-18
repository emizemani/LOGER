import os

def remove_trailing_delimiter(input_directory, output_directory, delimiter='|'):
    # Ensure the output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    # Iterate over all files in the input directory
    for filename in os.listdir(input_directory):
        if filename.endswith(".dat"):
            input_filepath = os.path.join(input_directory, filename)
            output_filepath = os.path.join(output_directory, filename)
            
            try:
                # Process each file
                with open(input_filepath, 'r', encoding='utf-8') as file:
                    lines = file.readlines()
            except UnicodeDecodeError:
                with open(input_filepath, 'r', encoding='latin1') as file:
                    lines = file.readlines()
            
            # Remove the trailing delimiter from each line
            cleaned_lines = []
            for line in lines:
                if line.endswith(delimiter + '\n'):
                    cleaned_lines.append(line[:-len(delimiter)-1] + '\n')
                else:
                    cleaned_lines.append(line)
            
            # Write the cleaned lines to the output file
            with open(output_filepath, 'w', encoding='utf-8') as file:
                file.writelines(cleaned_lines)
            
            print(f"Processed file: {filename}")

# Specify the input and output directories
input_directory = '/home/emionatrip/tpcds_data'
output_directory = '/home/emionatrip/tpcds_data_cleaned'

# Call the function to remove trailing delimiters and write to the new directory
remove_trailing_delimiter(input_directory, output_directory)
