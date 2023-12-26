import csv
import glob
import os
import sys
from multiprocessing import Pool

allowed_extensions = ['.mp3', '.m4a', '.mkv', '.webm', '.mp4']

#python list_files.py '/mnt/EXT1/AppliedAI Course Content' new_audio_ai_files.csv 

def process_file(file_path):
    return (file_path, os.path.splitext(file_path)[0], os.path.splitext(file_path)[1])

def extract_filename(file_path):
                """
                Extracts the filename without extension from a given file path.

                Args:
                    file_path (str): The path to the file.

                Returns:
                    str: The filename without extension.
                """
                filename = os.path.basename(file_path)
                filename_without_extension = os.path.splitext(filename)[0]
                return filename_without_extension

def list_files(directory, output_file):
    """
    Returns a list of all files in the given directory and its subdirectories,
    and writes the file list, path, and extension to a CSV file.

    Args:
        directory (str): The directory to search for files.
        output_file (str): The path to the output CSV file.

    Returns:
        list: A list of file paths.
    """
    file_list = []

    with Pool() as pool:
            for result in pool.imap(process_file, glob.iglob(directory + '/**/*', recursive=True)):
                file_path, _, extension = result
                Filename = extract_filename(file_path)
                result = list(result)
                result.append(Filename)

                if extension in allowed_extensions:
                    file_list.append(tuple(result))
                    file_list.append(result)                
                
               
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['File Path', 'File Name', 'Extension','Filename'])
        writer.writerows(file_list)

    return file_list

def main():
    if len(sys.argv) != 3:
        print("Usage: python your_script.py directory output_file.csv")
        return

    directory = sys.argv[1]
    output_file = sys.argv[2]

    list_files(directory, output_file)


if __name__ == "__main__":
    main()
