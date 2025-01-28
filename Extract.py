import h5py
import numpy as np
import gc

# Input and output file paths
input_file_path = 'matrix.h5'
output_file_path = 'extracted_matrix_1GB.txt'

# Determine the size of each element (assuming int32)
element_size = 4  # Size of int32 in bytes (change to 4 for int32, or 8 for int64)

# Calculate how many elements to load for ~100 MB (reduce chunk size further)
# 100MB = 104857600 bytes, so this will load 100MB of data at a time instead of 1GB
num_elements_to_load = 104857600 // element_size

# Open the HDF5 file
with h5py.File(input_file_path, 'r') as f:
    # Extract the 'block0_values' dataset (the matrix)
    matrix = f['t/block0_values']
    
    # Get the total number of elements (rows * columns) in the matrix
    total_elements = matrix.shape[0] * matrix.shape[1]
    
    # Open the output file for writing
    with open(output_file_path, 'w') as f_out:
        for start_row in range(0, matrix.shape[0], num_elements_to_load // matrix.shape[1]):
            # Load the next chunk of the matrix
            end_row = min(start_row + (num_elements_to_load // matrix.shape[1]), matrix.shape[0])
            partial_matrix = matrix[start_row:end_row, :]
            
            # Convert NaN values to 0 and cast the matrix to integers
            partial_matrix = np.nan_to_num(partial_matrix, nan=0)  # Replace NaNs with 0
            partial_matrix = partial_matrix.astype(np.int32)  # Cast to integers (int32)
            
            # Save the partial matrix to the output file
            np.savetxt(f_out, partial_matrix, fmt='%d')  # Save as integers
            
            # Free memory and run garbage collection to avoid memory overload
            del partial_matrix
            gc.collect()
    
    print(f"Extracted data saved to {output_file_path}.")
