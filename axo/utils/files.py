import os
import time

def is_writing(file_path:str):
    try:
        # Get the initial size of the file
        initial_size = os.stat(file_path).st_size
        time.sleep(1)  # Wait for 1 second
        # Get the current size of the file
        current_size = os.stat(file_path).st_size
        # Compare sizes to check if the file is being written to
        return current_size > initial_size
    except FileNotFoundError:
        # Handle case where the file does not exist
        return False       