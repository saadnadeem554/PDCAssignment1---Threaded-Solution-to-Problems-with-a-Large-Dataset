import h5py
import numpy as np

# Input and output file paths
input_file_path = 'matrix.h5'
output_file_path = 'extracted_matrix_1.5GB.txt'

# Define constants
element_size = 4  # Size of int32 in bytes
target_size_bytes = 1.5 * 1024 * 1024 * 1024  # 1.5 GB in bytes
num_elements_to_extract = int(target_size_bytes / element_size)  # Total elements to extract

# Calculate number of rows to extract (ensuring it does not exceed the total rows available)
# Open the HDF5 file
with h5py.File(input_file_path, 'r') as f:
    # Extract the 'block0_values' dataset (the matrix)
    matrix = f['t/block0_values']
    total_rows, total_columns = matrix.shape
    
    rows_to_extract = min(num_elements_to_extract // total_columns, total_rows)

    # Open the output file for writing
    with open(output_file_path, 'w') as f_out:
        rows_extracted = 0
        
        # Process in chunks of rows
        chunk_size = 500  # Process 500 rows at a time (you can adjust this based on memory)
        while rows_extracted < rows_to_extract:
            # Determine how many rows to read in the current chunk
            remaining_rows = rows_to_extract - rows_extracted
            chunk_rows = min(chunk_size, remaining_rows)

            # Extract the chunk of rows
            partial_matrix = matrix[rows_extracted:rows_extracted + chunk_rows, :]

            # Convert NaNs to 0 and cast to int32
            partial_matrix = np.nan_to_num(partial_matrix, nan=0).astype(np.int32)

            # Save the chunk to file
            np.savetxt(f_out, partial_matrix, fmt='%d')

            # Update the number of rows extracted
            rows_extracted += chunk_rows

print(f"Extracted {rows_extracted} rows (~1.5 GB) saved to {output_file_path}.")