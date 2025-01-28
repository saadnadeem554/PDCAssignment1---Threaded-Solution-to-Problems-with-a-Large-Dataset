import h5py
import numpy as np

# Input and output file paths
input_file_path = 'matrix.h5'
output_file_path = 'extracted_matrix_1GB.txt'

# Determine the size of each element (assuming float64)
element_size = 8  # Size of float64 in bytes (change to 4 for float32)

# Calculate how many elements to load for ~1 GB (1 GB = 1073741824 bytes)
num_elements_to_load = 1073741824 // element_size

# Open the HDF5 file
with h5py.File(input_file_path, 'r') as f:
    # Extract the 'block0_values' dataset (the matrix)
    matrix = f['t/block0_values']
    
    # Get the total number of elements (rows * columns) in the matrix
    total_elements = matrix.shape[0] * matrix.shape[1]
    
    # Calculate the number of rows to load for approximately 1 GB of data
    rows_to_load = min(matrix.shape[0], num_elements_to_load // matrix.shape[1])
    print(f"Loading {rows_to_load} rows of data.")
    
    # Load the matrix data (only the selected number of rows)
    partial_matrix = matrix[:rows_to_load, :]
    
    # Save the partial matrix to a text file
    np.savetxt(output_file_path, partial_matrix, fmt='%f')  # Adjust fmt for integer if necessary
    print(f"Extracted data saved to {output_file_path}.")
