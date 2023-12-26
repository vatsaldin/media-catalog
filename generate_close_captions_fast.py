import os
import json
import torch
import csv

import datetime
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline

fast_model_name = "openai/whisper-large-v2"

device = "cuda:0" if torch.cuda.is_available() else "cpu"
torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32

model_id = "distil-whisper/distil-large-v2"

model = AutoModelForSpeechSeq2Seq.from_pretrained(
    model_id, torch_dtype=torch_dtype, low_cpu_mem_usage=True, use_safetensors=True
)
model.to(device)

processor = AutoProcessor.from_pretrained(model_id)
            # Generate error log file name with datetime as suffix
error_log_file = "error_log_{}.csv".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

data = []

processed_files_counter = 0

def extract_filename(file_path):
    filename = os.path.basename(file_path)
    filename_without_extension = os.path.splitext(filename)[0]
    return filename_without_extension

def write_csv(file_path, data):
    global processed_files_counter
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        if processed_files_counter == 0:
            writer.writerow(['folder_path', 'video_file_name', 'closed_caption_file_name'])
        
        writer.writerows(data)
        
        processed_files_counter = processed_files_counter + 1

def write_error_log(error_audio_file, full_error_messsage):

    with open(error_log_file, "a", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["error_audio_file", "full_error_messsage", "datetime"])
        writer.writerow([error_audio_file, full_error_messsage, datetime.datetime.now().strftime("%Y%m%d%H%M%S")])

def generate_closed_caption_file(folder_path, closed_caption_file_name, outputs):


    with open(os.path.join(folder_path, closed_caption_file_name), "w") as f:
        json.dump(outputs, f)
        print("Generated CC JSON file: {}".format(closed_caption_file_name))
 

def fast_audio_to_text(audio_video_file, closed_caption_file_name, folder_path, batch_size = 24, device_id = "0"):
    pipe = pipeline(
        "automatic-speech-recognition",
        model=fast_model_name,
        torch_dtype=torch.float16,
        device=f"cuda:{device_id}",
    )
    
    pipe.model = pipe.model.to_bettertransformer()

    outputs = pipe(
        audio_video_file,
        chunk_length_s=30,
        batch_size=batch_size,
        generate_kwargs={"task": "transcribe", "language": "en"},
        return_timestamps=True,
    )

        # Write the text to a text file
    generate_closed_caption_file(folder_path, closed_caption_file_name, outputs)
    
    data.append([folder_path, audio_video_file, closed_caption_file_name])

    write_csv('processed_files_audio_json_cc.csv', data)


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
    file_path = file_name + "_" + datetime.datetime.now().strftime("%Y_%b_%d") + ".csv"

    if not os.path.exists(file_path):
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(data)
    else:
        with open(file_path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(data)


def distill_whisper_audio_to_text(audio_video_file, closed_caption_file_name, folder_path, batch_size = 16):
    pipe = pipeline(
    "automatic-speech-recognition",
    model=model,
    tokenizer=processor.tokenizer,
    feature_extractor=processor.feature_extractor,
    max_new_tokens=128,
    chunk_length_s=15,
    batch_size=16,
    torch_dtype=torch_dtype,
    device=device,
    )

    outputs = pipe(audio_video_file)

    generate_closed_caption_file(folder_path, closed_caption_file_name, outputs)
    
    data.append([folder_path, audio_video_file, closed_caption_file_name])




    

def process_csv(csv_file="new_converted_audio_ai_files_2023110617.csv"):
    # Process each row from dataframe and the file from File Path column
    import pandas as pd

    df = pd.read_csv(csv_file, encoding='utf-8')

    # Assuming you have a DataFrame named df with a "File Path" column
    for index, row in df.iterrows():
        file_path = row["New File Path"]
        
        # Extract folder path from file path
        folder_path = os.path.dirname(file_path)
        

        # Generate text file name
        file_name_cc = extract_filename(file_path) + "_json_cc.json"

        # Process the file from the File Path column
        #audio_to_text(file_path, file_name_cc, folder_path)

        
        # Process the file from the File Path column
        try:
            #fast_audio_to_text(file_path, file_name_cc, folder_path)
            distill_whisper_audio_to_text(file_path, file_name_cc, folder_path)
            update_csv_file('audio_file_captioned', ['File Path', 'New File Path', 'Datetime'], [[file_path, file_name_cc, datetime.datetime.now().strftime("%Y_%b_%d%H_%M_%S")]])
            print("Processed file: {}".format(file_path))
        except Exception as e:
            
            full_error_messsage = str(e)
            print(f"An error occurred: {e}")

            update_csv_file('captioning_error_log', ['error_file', 'full_error_messsage', 'datetime'], [[file_path, full_error_messsage, datetime.datetime.now().strftime("%Y%b%d%H%M%S")]])
            
        

def main():
    process_csv()
    

if __name__ == '__main__':
    main()

