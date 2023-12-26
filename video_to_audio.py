import os
import glob
from pydub import AudioSegment

import csv
import os
import datetime

from tqdm.auto import tqdm

video_dir = ''  # Path where the videos are located
audio_dir = ''  # Path to store audio files
extension_list = ('*.mp4', '*.flv', '.m4a', '.mkv', '.webm', '.mov', '.mp3')

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

# Define function to create csv file with list of headers and datetime as suffix.Also check if file already exists then add data to it.


# Create a csv file if it does not exist, with given variable name as file_name and append the data passed to it. Use list of headers to create column names and the datetime should be suffix in the generated file name.The file should have utf-8 encoding.
def update_csv_file(file_name, headers, data):
    """
    Creates a CSV file with the given file name and appends the data passed to it.
    The file name will have the current datetime as a suffix.

    Args:
        file_name (str): The name of the CSV file.
        headers (list): A list of header names.
        data (list): A list of data rows.

    Returns:
        None
    """
    file_path = file_name + "_" + datetime.datetime.now().strftime("%Y%m%d%H") + ".csv"

    if not os.path.exists(file_path):
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(data)
    else:
        with open(file_path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(data)

    


temp_error = []
temp_process = []
counter = 0
error_counter = 0

# define function for following code
def convert_videos_to_mp3(video_dir, audio_dir, extension_list):
    """
    Converts videos in the given directory to mp3 format and saves them in the specified audio directory.

    Args:
        video_dir (str): The directory containing the videos.
        audio_dir (str): The directory to save the mp3 files.
        extension_list (list): A list of video file extensions to convert.

    Returns:
        None
    """
    global counter
    global error_counter

    os.chdir(video_dir)
    for extension in tqdm(extension_list):
        for video in tqdm(glob.glob(extension)):
            mp3_filename = extract_filename(video) + '_new.mp3'
            new_filename_w_path = os.path.join(audio_dir, mp3_filename)
            print(f"Converting {video} to {new_filename_w_path}")

        # Define try block to handle exceptions.
            try:
                AudioSegment.from_file(video).export(new_filename_w_path, format='mp3')
                print(f"Converted {video} to {new_filename_w_path}")
                counter = counter + 1
                print(f"Converted {counter} files")
                update_csv_file('new_converted_audio_ai_files', ['File Path', 'New File Path', 'Datetime'], [[video, new_filename_w_path, datetime.datetime.now().strftime("%Y%b%d%H%M%S")]])
            except Exception as e:
                print(f"Error converting {video}: {e}")
                full_error_messsage = str(e)
                print(f"An error occurred: {e}")
                error_counter = error_counter + 1
                print(f"Errors encountered {error_counter} files")
                update_csv_file('conversion_error_log', ['error_file', 'full_error_messsage', 'datetime'], [[video, full_error_messsage, datetime.datetime.now().strftime("%Y%b%d%H%M%S")]])

import pandas as pd

def convert_to_mp3(audio_dir, extension_list, file_list_csv="new_audio_ai_files.csv"):
    global counter
    global error_counter

    
    df = pd.read_csv(file_list_csv, encoding='utf-8')

        # Process each row to get filepath
    for index, row in tqdm(df.iterrows()):
        file_path = row["File Path"]
            # Your code here to process the file_path

        mp3_filename = row["Filename"] + '_new.mp3'
        new_filename_w_path = os.path.join(audio_dir, mp3_filename)
        print(f"Converting {file_path} to {new_filename_w_path}")

        # Define try block to handle exceptions.
        try:
            AudioSegment.from_file(file_path).export(new_filename_w_path, format='mp3')
            print(f"Converted {file_path} to {new_filename_w_path}")
            counter = counter + 1
            print(f"Converted {counter} files")
            update_csv_file('new_converted_audio_ai_files', ['File Path', 'New File Path', 'Datetime'], [[file_path, new_filename_w_path, datetime.datetime.now().strftime("%Y%b%d%H%M%S")]])
        except Exception as e:
            print(f"Error converting {file_path}: {e}")
            full_error_messsage = str(e)
            print(f"An error occurred: {e}")
            error_counter = error_counter + 1
            print(f"Errors encountered {error_counter} files")
            update_csv_file('conversion_error_log', ['error_file', 'full_error_messsage', 'datetime'], [[file_path, full_error_messsage, datetime.datetime.now().strftime("%Y%b%d%H%M%S")]])




if __name__ == '__main__':
    convert_to_mp3(audio_dir, extension_list)