import os
import re
import time
import random
import shutil
import pyodbc
import fnmatch
import requests
import traceback
import webbrowser
import numpy as np
import pandas as pd
import tkinter as tk
from PIL import Image
import win32com.client
import concurrent.futures
import matplotlib.pyplot as plt

from time import sleep
from datetime import datetime
from packaging import version
from threading import Thread, Lock
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed

CROP_WIDTH_RV = 3690
CROP_HEIGHT_RV = 2750

CROP_WIDTH_FV = 3690
CROP_HEIGHT_FV = 2750

def crop_center(image, crop_width, crop_height):
    width, height = image.size
    left = (width - crop_width) // 2
    top = (height - crop_height) // 2
    right = left + crop_width
    bottom = top + crop_height
    return image.crop((left, top, right, bottom))


def log_message(message):
    """Append a message to the log_text widget and update the UI."""
    log_text.insert(tk.END, message + '\n')
    log_text.see(tk.END)
    root.update_idletasks()


def copy_files(src_dst_pairs):
    for src_file, dst_file in src_dst_pairs:
        try:
            if os.path.exists(dst_file):
                log_message(f"File already exists, skipping")
                continue
            shutil.copy2(src_file, dst_file)
        except Exception as e:
            log_message(f"Error copying {src_file} to {dst_file}: {e}")
            
            
def rename_files(file_pairs):
    for src_file, new_file_path in file_pairs:
        try:
            os.rename(src_file, new_file_path)
        except Exception as e:
            log_message(f"file already exists : {e}")


def process_date_folder(date_folder_name, source_directory, destination_parent_directory):
    # Create the main folder name
    main_folder_name = f"survey_data_{date_folder_name}"

    # Create the full path for the new directory
    survey_data_path = os.path.join(destination_parent_directory, main_folder_name)

    # Create the new directory
    os.makedirs(survey_data_path, exist_ok=True)

    # Create subdirectories
    subdirectories = ['Data', 'Output', 'PAVE', 'ROW']
    for subdirectory in subdirectories:
        subdirectory_path = os.path.join(survey_data_path, subdirectory)
        os.makedirs(subdirectory_path, exist_ok=True)

    # Paths for source directories
    data_folder_path = os.path.join(source_directory, date_folder_name, 'data')
    output_path = os.path.join(survey_data_path, 'Output')
    # 
    data_path = os.path.join(survey_data_path, 'Data')
    # 
    photo_directory = os.path.join(source_directory, data_folder_path)    

    # Check if the Data directory exists for the current date
    if os.path.exists(data_folder_path):
        for folder_name in os.listdir(data_folder_path):
            folder_path = os.path.join(data_folder_path, folder_name)
            if os.path.isdir(folder_path):
                # Create each folder found in the Data directory inside the Output directory
                output_folder_path = os.path.join(output_path, folder_name)
                os.makedirs(output_folder_path, exist_ok=True)

    # Copy .xlsx files from the source directory to the Output subdirectory
    xlsx_files = []
    for root, dirs, files in os.walk(source_directory):
        for file_name in files:
            if file_name.endswith('.xlsx'):
                src_file = os.path.join(root, file_name)
                # Check if the .xlsx file belongs to the current date folder
                if date_folder_name in root:
                    dst_file = os.path.join(output_path, file_name)
                    xlsx_files.append((src_file, dst_file))
    copy_files(xlsx_files)

    for data_output in os.listdir(data_folder_path):
        data_output_path = os.path.join(data_folder_path, data_output)
        if os.path.isdir(data_output_path):
            data_number = data_output.replace(date_folder_name, "").replace("RUN", "").lstrip("0")
            new_folder_data = f"{date_folder_name}_{data_number}"
            new_folder_data_path = os.path.join(data_path, new_folder_data)
            os.makedirs(new_folder_data_path, exist_ok=True)
            
    # Process Camera_GeoTagged and Log directories for the current date folder
    for run_folder in os.listdir(data_folder_path):
        run_folder_path = os.path.join(data_folder_path, run_folder)
        # print(run_folder)
        if os.path.isdir(run_folder_path):
            # Process Camera_GeoTagged
            camera_geotagged_path = os.path.join(run_folder_path, 'Camera_GeoTagged')
            if os.path.exists(camera_geotagged_path):
                run_number = run_folder.replace(date_folder_name, "").replace("RUN", "").lstrip("0")
                new_folder_name = f"{date_folder_name}_{run_number}"
                new_folder_path = os.path.join(survey_data_path, 'PAVE', new_folder_name, 'PAVE-0')
                os.makedirs(new_folder_path, exist_ok=True)

                # Copy .jpg files to the new folder and rename them
                jpg_files = []
                renamed_files = []
                jpg_counter = 1
                for file_name in os.listdir(camera_geotagged_path):
                    if file_name.endswith('.jpg'):
                        src_file = os.path.join(camera_geotagged_path, file_name)
                        dst_file = os.path.join(new_folder_path, file_name)
                        jpg_files.append((src_file, dst_file))

                        # Rename the file
                        new_file_name = f"{date_folder_name}_{run_number}_PAVE-0-{jpg_counter:05d}.jpg"
                        new_file_path = os.path.join(new_folder_path, new_file_name)
                        renamed_files.append((dst_file, new_file_path))
                        jpg_counter += 1

                copy_files(jpg_files)
                rename_files(renamed_files)

                # Crop images in PAVE-0 folder
                for file_name in os.listdir(new_folder_path):
                    if file_name.endswith('.jpg'):
                        file_path = os.path.join(new_folder_path, file_name)
                        with Image.open(file_path) as img:
                            cropped_img = crop_center(img, CROP_WIDTH_RV, CROP_HEIGHT_RV)
                            cropped_img.save(file_path)
                            # print(f"✅ Cropped and saved image: {file_name}")

            # Process Log
            # log_path = os.path.join(run_folder_path, 'Log')
            # if os.path.exists(log_path):
            #     for file_name in os.listdir(log_path):
            #         if file_name.endswith(f'{run_folder}.csv'):
            #             csv_path = os.path.join(log_path, file_name)
            #             destination_subfolder_path = os.path.join(output_path, run_folder)
            #             os.makedirs(destination_subfolder_path, exist_ok=True)
            #             shutil.copy2(csv_path, destination_subfolder_path)
            log_path = os.path.join(run_folder_path, 'Log')
            if os.path.exists(log_path):
                for file_name in os.listdir(log_path):
                    if file_name.endswith(f'{run_folder}.csv'):
                        csv_path = os.path.join(log_path, file_name)
                        destination_subfolder_path = os.path.join(output_path, run_folder)
                        os.makedirs(destination_subfolder_path, exist_ok=True)
                        # shutil.copy2(csv_path, destination_subfolder_path)
                        # print(file_name,csv_path,destination_subfolder_path)
                for file_name2 in os.listdir(log_path):
                    # if file_name2.endswith('.csv') and re.search(r'Session', file_name2):
                      if file_name2.endswith('.csv') and (
                        re.search(r'xw_iri_qgis', file_name2) or
                        re.search(r'drp_rutting_qgis', file_name2)):
                        csv_path2 = os.path.join(log_path, file_name2)
                        destination_subfolder_path2 = os.path.join(output_path, run_folder)
                        os.makedirs(destination_subfolder_path2, exist_ok=True)
                        shutil.copy2(csv_path2, destination_subfolder_path2)
                        # print(file_name2,csv_path2,destination_subfolder_path2)

    # Process ROW directory within photo directory
    if os.path.exists(photo_directory):
        for photo_run_folder in os.listdir(photo_directory):
            photo_run_folder_path = os.path.join(photo_directory, photo_run_folder, 'Camera1_GeoTagged')
            if os.path.isdir(photo_run_folder_path):
                # Process ROW
                run_number = photo_run_folder.replace(date_folder_name, "").replace("RUN", "").lstrip("0")
                new_folder_name = f"{date_folder_name}_{run_number}"
                new_folder_path = os.path.join(survey_data_path, 'ROW', new_folder_name, 'ROW-0')
                os.makedirs(new_folder_path, exist_ok=True)

                # Copy .jpg files to the new folder and rename them
                jpg_files = []
                renamed_files = []
                jpg_counter = 1
                for file_name in os.listdir(photo_run_folder_path):
                    if file_name.endswith('.jpg'):
                        src_file = os.path.join(photo_run_folder_path, file_name)
                        dst_file = os.path.join(new_folder_path, file_name)
                        jpg_files.append((src_file, dst_file))

                        # Ensure unique file name
                        new_file_name = f"{date_folder_name}_{run_number}-ROW-0-{jpg_counter:05d}.jpg"
                        new_file_path = os.path.join(new_folder_path, new_file_name)
                        renamed_files.append((dst_file, new_file_path))
                        jpg_counter += 1

                copy_files(jpg_files)
                rename_files(renamed_files)

                # Crop images in ROW-0 folder
                for file_name in os.listdir(new_folder_path):
                    if file_name.endswith('.jpg'):
                        file_path = os.path.join(new_folder_path, file_name)
                        with Image.open(file_path) as img:
                            cropped_img = crop_center(img, CROP_WIDTH_FV, CROP_HEIGHT_FV)
                            cropped_img.save(file_path)
                            # print(f"✅ Cropped and saved image: {file_name}")

def copy_and_organize_files(source_directory, destination_parent_directory):
    # Create the destination parent directory if it doesn't exist
    os.makedirs(destination_parent_directory, exist_ok=True)

    # Find all date folders in the source directory
    date_folders = [folder_name for folder_name in os.listdir(source_directory) if re.match(r'^\d{8}$', folder_name)]

    if not date_folders:
        print("No date folders found in the source directory.")
    else:
        with ThreadPoolExecutor(max_workers=100) as executor:
            future_to_date_folder = {executor.submit(process_date_folder, date_folder_name, source_directory, destination_parent_directory): date_folder_name for date_folder_name in date_folders}

            for future in as_completed(future_to_date_folder):
                date_folder_name = future_to_date_folder[future]
                try:
                    future.result()
                    print(f"✅ Processed folder: {date_folder_name} Successfully")
                except Exception as exc:
                    print(f"{date_folder_name} generated an exception: {exc}")


def generated_parts(target_values, num_parts, tolerance):
    parts_list = []
    for target_value in target_values:
        total_sum = target_value * num_parts
        while True:
            # Generate random parts
            parts = np.random.uniform(low=total_sum / num_parts * 0.9, high=total_sum / num_parts * 1.1, size=num_parts)
            # Ensure the sum is correct
            if np.abs(np.sum(parts) - total_sum) < tolerance:
                parts_list.append(parts)
                break
            
    return parts_list


def generate_deviations(target_value):
    """Generate three negative deviations (each less than main value)"""
    deviations = [random.uniform(-0.2, -0.001) for _ in range(3)]
    
    # Calculate the last deviation precisely
    last_deviation = -sum(deviations)

    # Ensure the last deviation does not exceed 0 (prevent exceeding main value)
    if last_deviation > 0:
        # Adjust deviations proportionally downward until last deviation is zero
        factor = (abs(sum(deviations)) - 0.001) / abs(sum(deviations))
        deviations = [d * factor for d in deviations]
        last_deviation = -sum(deviations)

    deviations.append(last_deviation)
    return deviations


def generate_parts(target_value):
    """Compute the parts ensuring each <= main value"""
    deviations = generate_deviations(target_value)
    parts = [target_value + d for d in deviations]

    # Safety-check and correction (due to float precision)
    for idx, part in enumerate(parts):
        if part > target_value:
            parts[idx] = target_value  # correct the overflow
            diff = sum(parts) - (4 * target_value)
            # evenly subtract the extra from other parts
            distribute = diff / (len(parts)-1)
            for j in range(len(parts)):
                if j != idx:
                    parts[j] -= distribute
    
    avg = sum(parts) / len(parts)

    # print(f"Main value: {target_value}")
    # print(f"Parts: {parts}")
    # print(f"Average: {avg}\n")
    
    return parts


def read_csv_with_flexible_columns(file_path, delimiter=';'):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    
    header = lines[1].strip().split(delimiter)
    num_columns = len(header)
    
    check_col = lines[2].strip().split(delimiter)
    num_check_col = len(check_col)
    
    if num_check_col > num_columns:
        additional_columns = [f'unknown' for i in range(num_columns, num_check_col)]
        header.extend(additional_columns)
        num_columns = len(header)
    
    data = []
    for line in lines[2:]:
        row = line.strip().split(delimiter)
        if len(row) < num_columns:
            # Fill missing columns with NaN
            row.extend([None] * (num_columns - len(row)))
        elif len(row) > num_columns:
            # Truncate extra columns
            row = row[:num_columns]
        data.append(row)
    
    df = pd.DataFrame(data, columns=header)
    df.columns = df.columns.str.strip()
    
    return df


def process_csv_files(path):
    all_iri_dataframes = [] # empty list
    all_rutting_dataframes = [] # empty list
    
    try:
        for root, dirs, files in os.walk(path):
            # Find files
            iri_files = [f for f in files if f.endswith('.csv') and 'xw_iri_qgis' in f]
            rutting_files = [f for f in files if f.endswith('.csv') and 'drp_rutting_qgis' in f]

            # Process 'xw_iri_qgis' files
            for filename in iri_files:
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")
                
                iri_df = read_csv_with_flexible_columns(file_path)
                iri_df.columns = iri_df.columns.str.strip()

                survey_code = filename.split('_')[3].split('.')[0]
                survey_date = survey_code[:8]
                iri_df['date'] = survey_date
                iri_df['survey_code'] = survey_code
                
                # iri_df['iri_lane'] = (iri_df.iloc[:, 3] + iri_df.iloc[:, 5]) / 2
                # columns_to_drop = iri_df.columns[[0, 8]] if len(iri_df.columns) > 8 else iri_df.columns[:1]
                # iri_df.drop(columns=columns_to_drop, errors='ignore', inplace=True)

                # Fix Columes #
                # Ensure columns are numeric before performing calculations
                iri_df.iloc[:, 3] = pd.to_numeric(iri_df.iloc[:, 3], errors='coerce')
                iri_df.iloc[:, 5] = pd.to_numeric(iri_df.iloc[:, 5], errors='coerce')

                # Drop rows with NaN in the relevant columns
                iri_df = iri_df.dropna(subset=[iri_df.columns[3], iri_df.columns[5]])

                # Calculate iri_lane
                iri_df['iri_lane'] = (iri_df.iloc[:, 3] + iri_df.iloc[:, 5]) / 2
                columns_to_drop = iri_df.columns[[0, 8]] if len(iri_df.columns) > 8 else iri_df.columns[:1]
                iri_df.drop(columns=columns_to_drop, errors='ignore', inplace=True)
                # Ensure columns are numeric before performing calculations
                # Fix Columes #
                
                # extract iri for avg it equal tp iri_lane
                target_values = iri_df['iri_lane']
                num_parts = 4
                
                parts_list = [np.array(generate_parts(value)) for value in target_values]
                parts_list = [np.atleast_1d(parts) for parts in parts_list]

                iri_df = iri_df.loc[iri_df.index.repeat(num_parts)].reset_index(drop=True)
                iri_df['iri'] = np.concatenate(parts_list)
                # extract iri for avg it equal iri_lane
                
                # # extract iri for avg it Nearby tp iri_lane
                # target_values = iri_df['iri_lane']
                # num_parts = 4
                # tolerance = 0.3
                # parts_list = generated_parts(target_values, num_parts, tolerance)
                
                # iri_df = iri_df.loc[iri_df.index.repeat(num_parts)].reset_index(drop=True)
                # iri_df['iri'] = np.concatenate(parts_list)
                # # extract iri for avg it Nearby tp iri_lane

                increment = 5 if fnmatch.fnmatch(filename, '*xw_iri_qgis*') else 5
                iri_df['event_start'] = range(0, len(iri_df) * increment, increment)
                iri_df['event_end'] = iri_df['event_start'] + increment
                # iri_df['chainage'] = iri_df['event_start']

                all_iri_dataframes.append(iri_df)

            # Process 'drp_rutting_qgis' files
            for filename in rutting_files:
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")
                
                rut_df = read_csv_with_flexible_columns(file_path)
                rut_df.columns = rut_df.columns.str.strip()
                if 'unknown' in rut_df.columns:
                    rut_df.drop(columns=['unknown'], errors='ignore', inplace=True)
                                    
                increment = 5 if fnmatch.fnmatch(filename, '*drp_rutting_qgis*') else 5
                rut_df['event_start'] = range(0, len(rut_df) * increment, increment)
                rut_df['event_end'] = rut_df['event_start'] + increment
                rut_df['chainage'] = rut_df['event_start']
                
                survey_code = filename.split('_')[3].split('.')[0]
                rut_df['survey_code'] = survey_code
                
                rut_df['rut_point_x'] = rut_df['geometry (start_lonlat,end_lonlat)'].apply(
                    lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[1])
                )
                rut_df['rut_point_y'] = rut_df['geometry (start_lonlat,end_lonlat)'].apply(
                    lambda x: float(x.split('(')[1].split(')')[0].split(',')[0].split(' ')[0])
                )
                rut_df['rut_point_x'].fillna(method='ffill', inplace=True)
                rut_df['rut_point_y'].fillna(method='ffill', inplace=True)
                rut_df['rut_point_x'].fillna(method='bfill', inplace=True)
                rut_df['rut_point_y'].fillna(method='bfill', inplace=True)
                rut_df['rut_point_x'].fillna(0, inplace=True)
                rut_df['rut_point_y'].fillna(0, inplace=True)
                rut_df['rut_point_x'].interpolate(method='linear', limit_direction='both', inplace=True)
                rut_df['rut_point_y'].interpolate(method='linear', limit_direction='both', inplace=True)

                if 'name' in rut_df.columns:
                    rut_df.drop(columns=['name'], errors='ignore', inplace=True)
                    
                rut_df.rename(columns={'left rutting height (mm)': 'left_rutting', 'right rutting height (mm)': 'right_rutting', 'average height (mm)': 'avg_rutting'}, inplace=True)
                rut_df.drop(columns=['geometry (start_lonlat,end_lonlat)', 'Timestamps', 'Heading (degrees)', 'Speed (m/s)'], errors='ignore', inplace=True)

                all_rutting_dataframes.append(rut_df)

            if all_iri_dataframes:
                iri_dataframes = pd.concat(all_iri_dataframes, ignore_index=True)
            else:
                iri_dataframes = pd.DataFrame()

            if all_rutting_dataframes:
                rutting_dataframes = pd.concat(all_rutting_dataframes, ignore_index=True)
            else:
                rutting_dataframes = pd.DataFrame()       

        log_message(f"✅ Finished Prepar : .CSV files.")
    except Exception as e:
        log_message(f"❌ Failed to Prepar : {e}.") 
        
    return iri_dataframes, rutting_dataframes


def left_join_dataframes(df_rutting, df_iri):
    return pd.merge(df_rutting, df_iri, how='left', on=['event_start', 'event_end', 'survey_code'], suffixes=('_rut', '_iri'))


def get_jpg_filenames(directory):
    jpg_dict = {}
    for root, dirs, files in os.walk(directory):
        jpg_files = [f for f in files if f.endswith('.jpg')]
        if jpg_files:
            folder_name = os.path.basename(os.path.dirname(root))
            jpg_dict[folder_name] = len(jpg_files)
            
    frame_df = pd.DataFrame(list(jpg_dict.items()), columns=['survey_code','frame_num'])
    frame_df['survey_code'] = frame_df['survey_code'].str.replace(
        r'_(\d+)', lambda m: f"RUN{int(m.group(1)):02d}", regex=True
    )
    
    detailed_df = pd.DataFrame(columns=['frame_num', 'survey_code'])
    for index, row in frame_df.iterrows():
        pic_counts = range(1, int(row['frame_num']) + 1)
        temp_df = pd.DataFrame({
            'frame_num': pic_counts,
            'survey_code': row['survey_code'],
        })
        detailed_df = pd.concat([detailed_df, temp_df], ignore_index=True)

    return detailed_df


# test gen #
def gen_frame(frame_num, survey_code):
    frame_df = pd.DataFrame(columns=['frame_num', 'survey_code'])
    pic_counts = range(1, frame_num + 1)
    temp_df = pd.DataFrame({
        'frame_num': pic_counts,
        'survey_code': survey_code,
    })
    frame_df = pd.concat([frame_df, temp_df], ignore_index=True)
    
    return frame_df
# test gen #


def add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers):
    joined_df['frame_num_ch'] = pd.NA
    joined_df['frame_num'] = pd.NA
    
    derived_to_frame_mapping = pd.DataFrame({
        'frame_num_ch': derived_values,
        'frame_num': frame_numbers
    })
    
    for i, frame_num_ch in enumerate(derived_values):
        mask = (joined_df['event_start'] <= frame_num_ch) & (joined_df['event_end'] >= frame_num_ch)
        joined_df.loc[mask, 'frame_num_ch'] = frame_num_ch
        joined_df.loc[mask, 'frame_num'] = frame_numbers[i]
        
    return joined_df


def process_fainal_df(output_dir):
    try:
        # #  test gen #
        # frame_num = 2656
        # survey_code = "20241107RUN06"
        # frame_numbers_df = gen_frame(frame_num, survey_code)
        # #  test gen #
        
        frame_numbers_df = get_jpg_filenames(output_dir)  # This returns a DataFrame
        frame_numbers = frame_numbers_df['frame_num'].astype(int).tolist()
        iri_dataframes, rutting_dataframes = process_csv_files(output_dir)

        joined_df = left_join_dataframes(rutting_dataframes, iri_dataframes)
        
        grouped_df = joined_df.groupby('survey_code').agg(
            max_chainage=('chainage', 'max'),
            min_chainage=('chainage', 'min')
        ).reset_index()

        joined_df = pd.merge(joined_df, grouped_df, on='survey_code', how='left')
        
        max_event_start = joined_df['event_start'].max()

        # Calculate derived values
        derived_values = [round((max_event_start * num) / max(frame_numbers)) for num in frame_numbers]

        # Add frame numbers to the joined DataFrame
        final_df = add_frame_num_to_joined_df(joined_df, derived_values, frame_numbers)
        
        final_df = final_df.rename(columns={'rut_chainage':'chainage'})
        
        selected_columns = [
            'left_rutting', 'right_rutting', 'avg_rutting', 'event_start', 'event_end', 'survey_code',
            'rut_point_x', 'rut_point_y', 'date', 'iri 0 (m/km)', 'iri 1 (m/km)', 'iri_lane', 'iri', 
            'chainage', 'max_chainage', 'min_chainage', 'frame_num', 'frame_num_ch'
        ] 

        
        selected_columns = [col for col in selected_columns if col in final_df.columns]
        final_df = final_df[selected_columns]
        
        return final_df
    except Exception as e:
        print(f'Error: {e}')
        return pd.DataFrame()


def find_csv_files(start_dir, prefix='log_'):
    csv_files = []
    for dirpath, dirnames, filenames in os.walk(start_dir):
        for filename in fnmatch.filter(filenames, f'{prefix}*.xlsx'):
            csv_files.append(os.path.join(dirpath, filename))
            
    return csv_files


def process_data(final_df, output_dir):
    for survey_date in os.listdir(output_dir): # eg. base_dir = r"D:\xenomatixs"
        path = os.path.join(output_dir, survey_date, 'Output')
        mdb = os.path.join(output_dir, survey_date, 'Data')
        
        log_csv_files = find_csv_files(path)
        if log_csv_files:
            log_df = pd.read_excel(log_csv_files[0])
            log_df.rename(columns={'ผิว': 'event_name', 'link_id ระบบ': 'section_id'}, inplace=True)
            log_df.columns = log_df.columns.str.strip()

            folder_names = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
            for folder_name in folder_names:
                log_message(f"🔄 Processing In Folder : {folder_name}. ")
                
                # Perform the initial merge and filter rows where frame_num is between numb_start and numb_end
                merged_df = pd.merge(final_df, log_df, how='left', on=['survey_code'], suffixes=('_final_df', '_log_df'))
                merged_df = merged_df[(merged_df['frame_num'] >= merged_df['numb_start']) & 
                                    (merged_df['frame_num'] <= merged_df['numb_end'])]
                
                filtered_df = merged_df[merged_df['survey_code'] == folder_name]
                run_code = re.sub(r'RUN0*(\d+)', r'_\1', folder_name)
                
                # add filter_df as min_chainage and max_chainage is group by numb_start and numb_end and merge to merged_df
                filter_df = merged_df.groupby(['numb_start', 'numb_end'], group_keys=False).agg(
                    min_chainage=('chainage', 'min'),
                    max_chainage=('chainage', 'max')
                ).reset_index()
                
                merged_df = pd.merge(merged_df, filter_df, on=['numb_start', 'numb_end'], how='left')
                filtered_df = pd.merge(filtered_df, filter_df, on=['numb_start', 'numb_end'], how='left')
# csv
                def process_val(df):
                    df['chainage'] = df['chainage']
                    df['lon'] = df['rut_point_y']
                    df['lat'] = df['rut_point_x']
                    df['iri_right'] = df['iri 0 (m/km)']
                    df['iri_left'] = df['iri 1 (m/km)']
                    df['iri_lane'] = df['iri_lane']
                    df['iri'] = df['iri']
                    df['rutt_right'] = df['right_rutting']
                    df['rutt_left'] = df['left_rutting']
                    df['rutting'] = df['avg_rutting']
                    df['texture'] = 0
                    df['etd_texture'] = 0
                    df['event_name'] = df['event_name'].str.lower()
                    df['frame_number'] = df['frame_num']
                    df['file_name'] = df['survey_code'].str.replace(r'RUN0*(\d+)', r'_\1', regex=True)
                    df['run_code'] = df['file_name'].str.split('_').str[-1]

                    return df

                processed_val = process_val(merged_df)

                selected_columns_val = [
                    'chainage', 'lon', 'lat', 'iri_right', 'iri_left', 'iri', 'iri_lane', 'rutt_right', 'rutt_left', 
                    'rutting', 'texture', 'etd_texture', 'event_name', 'frame_number', 'file_name', 'run_code'
                ]

                selected_columns_val = [col for col in selected_columns_val if col in processed_val.columns]
                processed_val_filename = os.path.join(mdb, 'access_valuelaser.csv')
                processed_val[selected_columns_val].to_csv(os.path.join(processed_val_filename), index=False)
                
                def process_dis(df):
                    df['chainage_pic'] = df['chainage']
                    df['frame_number'] = df['frame_num']
                    df['event_name'] = df['event_name'].str.lower()
                    df['name_key'] = df['survey_code'].str.replace(r'RUN0*(\d+)', r'_\1', regex=True)
                    df['run_code'] = df['file_name'].str.split('_').str[-1]

                    return df

                processed_dis = process_dis(merged_df)

                selected_columns_dis = [
                    'chainage_pic', 'frame_number', 'event_name', 'name_key', 'run_code'
                ]

                selected_columns_dis = [col for col in selected_columns_dis if col in processed_dis.columns]
                processed_dis_filename = os.path.join(mdb, 'access_distress_pic.csv')
                processed_dis[selected_columns_dis].to_csv(os.path.join(processed_dis_filename), index=False)
                
                def process_key(df):
                    df['event_str'] = df['min_chainage_y']
                    df['event_end'] = df['max_chainage_y']
                    df['event_num'] = df['event_name'].str[0].str.lower()
                    df['event_type'] = 'pave type'
                    df['event_name'] = df['event_name'].str.lower()
                    df['link_id'] = df['linkid']
                    df['lane_no'] = df['linkid'].apply(lambda x: str(x)[11:13] if isinstance(x, str) and len(x) >= 13 else None)
                    df['survey_date'] = df['date_final_df']
                    df['lat_str'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('first')
                    df['lat_end'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('last')
                    df['lon_str'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('first')
                    df['lon_end'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('last')
                    df['name_key'] = df['survey_code'].str.replace(r'RUN0*(\d+)', r'_\1', regex=True)
                    df['run_code'] = df['name_key'].str.split('_').str[-1]
                    
                    return df

                processed_key = merged_df.groupby('survey_code', group_keys=False).apply(process_key).reset_index(drop=True)
                processed_key = processed_key.groupby(['linkid', 'survey_date']).first().reset_index()

                selected_columns_key = [
                    'event_str', 'event_end', 'event_num', 'event_type', 'event_name', 'link_id', 'section_id', 
                    'km_start', 'km_end', 'length', 'lane_no', 'survey_date', 'lat_str', 'lat_end', 'lon_str', 
                    'lon_end', 'name_key', 'run_code'
                ]

                selected_columns_key = [col for col in selected_columns_key if col in processed_key.columns]
                processed_key_filename = os.path.join(mdb, 'access_key.csv')
                processed_key[selected_columns_key].sort_values(by=['run_code', 'event_str', 'event_end'], ascending=[True, True, False]).to_csv(os.path.join(processed_key_filename), index=False)
# .csv
# .mdb 
                mdb_folder_path = os.path.join(mdb, run_code)
                # print(f'store in: {mdb_folder_path}')
                mdb_path = os.path.join(mdb_folder_path, f'{run_code}_edit.mdb')
                print(f'this name: {mdb_path}')
            
                if not os.path.isdir(mdb):
                    print(f"⛔ Directory not found: {mdb}")
                    continue
                
                def mdb_video_process(df):
                    df['CHAINAGE'] = df['chainage']
                    df['LRP_OFFSET'] = df['chainage']
                    df['LRP_NUMBER'] = 0
                    df['FRAME'] = df['frame_num']
                    df['GPS_TIME'] = 0
                    df['X'] = df['rut_point_y']
                    df['Y'] = df['rut_point_x']
                    df['Z'] = 0
                    df['HEADING'] = 0
                    df['PITCH'] = 0
                    df['ROLL'] = 0

                    return df

                video_process = mdb_video_process(filtered_df)
                
                selected_mdb_video_process = [
                    'CHAINAGE', 'LRP_OFFSET', 'LRP_NUMBER', 'FRAME', 'GPS_TIME', 
                    'X', 'Y', 'Z', 'HEADING', 'PITCH', 'ROLL'
                ]

                selected_mdb_video_process = [col for col in selected_mdb_video_process if col in video_process.columns]
                mdb_video_process_filename = os.path.join(mdb_folder_path, f'Video_Processed_{run_code}_2.csv')
                video_process[selected_mdb_video_process].to_csv(mdb_video_process_filename, index=False)
                
                mdb_video_header = pd.DataFrame({
                    'CAMERA': [1, 2],
                    'NAME': ['ROW-0', 'PAVE-0'],
                    'DEVICE': ['XENO', 'XENO'],
                    'SERIAL': ['6394983', '6394984'],
                    'INTERVAL': [5, 2],
                    'WIDTH': [0, 0],
                    'HEIGHT': [0, 0],
                    'FRAME_RATE': [0, 0],
                    'FORMAT': ['422 YUV 8', 'Mono 8'],
                    'X_SCALE': [0, 0.5],
                    'Y_SCALE': [0, 0.5],
                    'DATA_FORMAT': [-1, -1],
                    'PROCESSING_METHOD': [-1, -1],
                    'ENABLE_MOBILE_MAPPING': [True, False],
                    'DISP_PITCH': [0, 0],
                    'DISP_ROLL': [0, 0],
                    'DISP_YAW': [0, 0],
                    'DISP_X': [0, 0],
                    'DISP_Y': [0, 0],
                    'DISP_Z': [0, 0],
                    'HFOV': [0, 0],
                    'VFOV': [0, 0]
                })
                
                mdb_video_header_filename = os.path.join(mdb_folder_path, f'Video_Header_{run_code}.csv')
                mdb_video_header.to_csv(mdb_video_header_filename, index=False)
                
                def mdb_survey_header(df):
                    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df['SURVEY_ID'] = run_code
                    df['SURVEY_FILE'] = run_code
                    df['SURVEY_DESC'] = None
                    df['SURVEY_DATE'] = current_datetime
                    df['VEHICLE'] = 'ISS'
                    df['OPERATOR'] = 'ISS'
                    df['USER_1_NAME'] = None
                    df['USER_1'] = None
                    df['USER_2_NAME'] = None
                    df['USER_2'] = None
                    df['USER_3_NAME'] = None
                    df['USER_3'] = None
                    df['LRP_FILE'] = f'LRP_{run_code}'
                    df['LRP_RESET'] = 'N'
                    df['LRP_START'] = 0
                    df['CHAIN_INIT'] = 0
                    df['CHAIN_START'] = 0
                    df['CHAIN_END'] = df['max_chainage_y'].max()
                    df['SECT_LEN'] = 0
                    df['DIR'] = 'I'
                    df['LANE'] = 1
                    df['DEVICES'] = 'GPS-Geo-DR,LP_V3-LWP,LP_V3-RWP,TPL,Video'
                    df['OTHERSIDE'] = True
                    df['VERSION'] = '2.7.3.4/2.7.3.4'
                    df['MEMO'] = None
                    df['LENGTH'] = df['max_chainage_y'].max()

                    # Fill NaN values with default values before converting to integers
                    df['LRP_START'] = df['LRP_START'].fillna(0).astype(int)
                    df['CHAIN_INIT'] = df['CHAIN_INIT'].fillna(0).astype(int)
                    df['CHAIN_START'] = df['CHAIN_START'].fillna(0).astype(int)
                    df['CHAIN_END'] = df['CHAIN_END'].fillna(0).astype(int)
                    df['SECT_LEN'] = df['SECT_LEN'].fillna(0).astype(int)
                    df['LANE'] = df['LANE'].fillna(1).astype(int)
                    df['LENGTH'] = df['LENGTH'].fillna(0).astype(int)

                    # Ensure boolean column is properly handled
                    df['OTHERSIDE'] = df['OTHERSIDE'].fillna(False).astype(bool)

                    # Convert SURVEY_DATE to datetime
                    df['SURVEY_DATE'] = pd.to_datetime(df['SURVEY_DATE'], errors='coerce')

                    return df

                survey_header = mdb_survey_header(filtered_df)
                survey_header = survey_header.groupby(['SURVEY_ID']).first().reset_index()
                
                selected_mdb_survey_header = [
                    'SURVEY_ID', 'SURVEY_FILE', 'SURVEY_DESC', 'SURVEY_DATE', 'VEHICLE', 'OPERATOR', 'USER_1_NAME', 'USER_1', 
                    'USER_2_NAME', 'USER_2', 'USER_3_NAME', 'USER_3', 'LRP_FILE', 'LRP_RESET', 'LRP_START', 'CHAIN_INIT', 
                    'CHAIN_START','CHAIN_END', 'SECT_LEN', 'DIR', 'LANE', 'DEVICES', 'OTHERSIDE', 'VERSION', 'MEMO', 'LENGTH'
                ]

                selected_mdb_survey_header = [col for col in selected_mdb_survey_header if col in survey_header.columns]
                mdb_survey_header_filename = os.path.join(mdb_folder_path, f'Survey_Header_{run_code}.csv')
                survey_header[selected_mdb_survey_header].to_csv(mdb_survey_header_filename, index=False)
                
                def mdb_KeyCode_Raw(df):
                    df['CHAINAGE_START'] = df['min_chainage_y']
                    df['CHAINAGE_END'] = df['max_chainage_y']
                    df['EVENT'] = df['event_name'].str[0].str.lower()
                    df['SWITCH_GROUP'] = 'pave type.'
                    df['EVENT_DESC'] = df['event_name'].str.lower()
                    df['LATITUDE_START'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('first')
                    df['LATITUDE_END'] = df.groupby(['survey_code', 'linkid'])['rut_point_x'].transform('last')
                    df['LONGITUDE_START'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('first')
                    df['LONGITUDE_END'] = df.groupby(['survey_code', 'linkid'])['rut_point_y'].transform('last')
                    df['link_id'] = df['linkid']
                    df['section_id'] = df['section_id']
                    df['km_start'] = df['km_start']
                    df['km_end'] = df['km_end']
                    df['length'] = df['length']
                    df['lane_no'] = df['linkid'].apply(lambda x: str(x)[11:13] if isinstance(x, str) and len(x) >= 13 else None)
                    df['survey_date'] = df['date_final_df']
                    
                    return df

                KeyCode_Raw = merged_df.groupby('survey_code', group_keys=False).apply(mdb_KeyCode_Raw).reset_index(drop=True)
                KeyCode_Raw = KeyCode_Raw.groupby(['linkid', 'survey_date']).first().reset_index()
                KeyCode_Raw = KeyCode_Raw[KeyCode_Raw['survey_code'] == folder_name]

                selected_mdb_KeyCode_Raw = [
                    'CHAINAGE_START', 'CHAINAGE_END', 'EVENT', 'SWITCH_GROUP', 'EVENT_DESC', 'LATITUDE_START', 'LATITUDE_END', 
                    'LONGITUDE_START', 'LONGITUDE_END', 'link_id', 'section_id', 'km_start', 'km_end', 'length', 'lane_no', 
                    'survey_date'
                ]

                selected_mdb_KeyCode_Raw = [col for col in selected_mdb_KeyCode_Raw if col in KeyCode_Raw.columns]
                mdb_KeyCode_Raw_filename = os.path.join(mdb_folder_path, f'KeyCode_Raw_{run_code}.csv')
                KeyCode_Raw[selected_mdb_KeyCode_Raw].sort_values(by=['lane_no', 'CHAINAGE_START', 'CHAINAGE_END'], ascending=[True, True, False]).to_csv(mdb_KeyCode_Raw_filename, index=False)    
# .mdb 
# # insert .mdb
                def create_access_db(db_path):
                    if os.path.isfile(db_path):
                        print(f"⛔ File already exists: {db_path}")
                    else:
                        access_app = win32com.client.Dispatch("Access.Application")
                        access_app.NewCurrentDatabase(db_path)
                        # log_message(f"✅ Created new Access database at: {db_path}")
                        log_message(f"✅ Created New Access Database.")
                        access_app.Quit()

                def table_exists(con, table_name):
                    try:
                        cur = con.cursor()
                        cur.execute(f"SELECT 1 FROM [{table_name}] WHERE 1=0;")
                        cur.close()
                        return True
                    except pyodbc.Error as e:
                        if '42S02' in str(e):  # '42S02' indicates a "table not found" error in SQL
                            return False
                        else:
                            raise e 

                def insert_csv_to_access(csv_path, table_name, access_db_path, max_retries=2, retry_delay=2):
                    conn_str = r"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={};".format(access_db_path)
                    con = None
                    retries = 0
                    
                    while retries < max_retries:
                        try:
                            con = pyodbc.connect(conn_str)
                            con.autocommit = False  # Turn off auto-commit for better performance
                            
                            if table_exists(con, table_name):
                                print(f"⏯️ Table {table_name} already exists. Skipping insertion.")
                                break
                            
                            cur = con.cursor()

                            start_time = time.time()

                            strSQL = (f"SELECT * INTO [{table_name}] "
                                    f"FROM [text;HDR=Yes;FMT=Delimited(,);Database={os.path.dirname(csv_path)}].{os.path.basename(csv_path)};")
                            cur.execute(strSQL)

                            con.commit()  # Commit the transaction after all operations

                            end_time = time.time()
                            print(f"⌛ Inserted table {table_name} in {end_time - start_time:.2f} seconds.")
                            break  # If the operation is successful, exit the retry loop

                        except pyodbc.Error as e:
                            error_code = e.args[0]
                            if error_code == 'HY000':
                                print(f"⌛ Database is locked, retrying in {retry_delay} seconds... (Attempt {retries + 1}/{max_retries})")
                                retries += 1
                                time.sleep(retry_delay)
                            else:
                                print(f"An error occurred: {e}")
                                break
                        except Exception as e:
                            print(f"⛔ An unexpected error occurred: {e}")
                            break
                        finally:
                            if con:
                                con.close()

                    if retries == max_retries:
                        print(f"⛔ Failed to insert table {table_name} after {max_retries} attempts.")

                def process_csv_files(csv_files, mdb_folder_path, mdb_path):
                    countc = os.cpu_count()
                    cpu = countc / 2
                    with ThreadPoolExecutor(max_workers=cpu) as executor:
                        futures = [executor.submit(insert_csv_to_access, os.path.join(mdb_folder_path, csv_name), table_name, mdb_path) for csv_name, table_name in csv_files.items()]
                        for future in as_completed(futures):
                            try:
                                future.result()  # Ensure exceptions are raised
                            except Exception as e:
                                print(f"⛔ Error processing CSV file: {e}")
                                
                create_access_db(mdb_path)

                csv_files = {
                    f'KeyCode_Raw_{run_code}.csv': f'KeyCode_Raw_{run_code}', 
                    f'Survey_Header_{run_code}.csv': f'Survey_Header',
                    f'Video_Header_{run_code}.csv': f'Video_Header_{run_code}',
                    f'Video_Processed_{run_code}_2.csv': f'Video_Processed_{run_code}_2'
                }
                
                process_csv_files(csv_files, mdb_folder_path, mdb_path)
                
                for csv_name in csv_files.keys():
                    os.remove(os.path.join(mdb_folder_path, csv_name))
# insert .mdb
    log_message(f"Successfully Create .MDB ...")


def process_single_image(path, matrix, angle):
    try:
        img_2 = cv2.imread(path)
        img_2 = cv2.resize(img_2, (0, 0), fx=0.5, fy=0.5)
        corrected_img = cv2.warpPerspective(img_2, matrix, (1200, 1200))

        (h, w) = corrected_img.shape[:2]
        center = (w // 2, h // 2)
        rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
        rotated_img = cv2.warpAffine(corrected_img, rotation_matrix, (w, h))
        rotated_img = cv2.rotate(corrected_img, cv2.ROTATE_90_CLOCKWISE)
        cnv_img_rgb = cv2.cvtColor(rotated_img, cv2.COLOR_BGR2RGB)
        
        # cv2.imwrite(path, cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))
        
        success = cv2.imwrite(path, cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))
        if not success:
            log_message(f"Error in process_single_image to {path}")
    except Exception as e:
        log_message(f"Error in process_single_image: {e}")
        
        
def transfromimage(input_folder):
    # Transformation matrix
    matrix = np.asarray([[-4.72213304e-01, 6.07375445e+00, -2.36383184e+01],
                         [9.15608405e-01, 2.65031461e+00, -7.51851357e+02],
                         [-2.53338772e-04, 4.22432334e-03, 1.00000000e+00]])

    angle = -90 
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for root, dirs, files in os.walk(input_folder):
                if 'PAVE-0' in root:
                    for image_test in files:
                        path = os.path.join(root, image_test)
                        if path.endswith('.jpg'):
                            futures.append(executor.submit(process_single_image, path, matrix, angle))
                
                for future in concurrent.futures.as_completed(futures):
                     future.result()

    except Exception as e:
        log_message(f"Error in transformimage: {e}")   


def move_folder(src, dest):
    try:
        shutil.move(src, dest)
        log_message(f"📁 Moved folder {os.path.basename(src)} to processed.")
    except Exception as e:
        log_message(f"⛔ Error moving folder {os.path.basename(src)}: {str(e)}")


def make_processed_file(base_dir):
    processed = os.path.join(base_dir, 'processed')
    input_dir = os.path.join(base_dir, 'input')

    os.makedirs(processed, exist_ok=True)  # Create processed directory if not exists

    folder_paths = [
        os.path.join(input_dir, folder_name)
        for folder_name in os.listdir(input_dir)
        if os.path.isdir(os.path.join(input_dir, folder_name)) and re.match(r'^\d{8}$', folder_name)
    ]

    log_message(f"Found {len(folder_paths)} folders to process.")

    # Use ThreadPoolExecutor to move folders in parallel
    with ThreadPoolExecutor(max_workers=100) as executor:
        future_to_folder = {
            executor.submit(move_folder, folder_path, os.path.join(processed, os.path.basename(folder_path))): folder_path
            for folder_path in folder_paths
        }

        for future in as_completed(future_to_folder):
            folder = future_to_folder[future]
            try:
                future.result()  # This will raise any exception from the thread
            except Exception as e:
                log_message(f"⛔ Error processing folder {os.path.basename(folder)}: {str(e)}")

    log_message("🎉 All files moved successfully.")


def main(base_dir, input_dir, output_dir, progress_callback=None):
    try:
        steps = 5  # Number of steps in the process
        current_step = 0

        log_message("🔄 Starting file processing...")
        
        log_message("🔄 Organizing files...")
        copy_and_organize_files(input_dir, output_dir)
        current_step += 1
        if progress_callback:
            progress_callback(current_step / steps * 100)
        log_message("✔️ Files organized successfully.")
        log_message("---" * 20)
        
        log_message("---" * 20)
        log_message("📝 Processing final dataframe...")
        final_df = process_fainal_df(output_dir)
        current_step += 1
        if progress_callback:
            progress_callback(current_step / steps * 100)
        log_message("✔️ Final dataframe processed.")
        log_message("---" * 20)
        
        log_message("---" * 20)
        log_message("🔄 Running main function...")
        process_data(final_df, output_dir)
        current_step += 1
        if progress_callback:
            progress_callback(current_step / steps * 100)
        log_message("✔️ Main function processing completed.")
        log_message("---" * 20)
        
        # log_message("🔄 Processing transformimage...")
        # # transfromimage(output_dir)
        # current_step += 1
        # if progress_callback:
        #     progress_callback(current_step / steps * 100)
        # log_message("✔️ Transformimage completed.")
        
        # log_message("🔄 Moving processed files...")
        # # make_processed_file(base_dir)
        # current_step += 1
        # if progress_callback:
        #     progress_callback(current_step / steps * 100)
        # log_message("✔️ Processed files moved successfully.")

        log_message("🎉 Process completed! ")
    except Exception as e:
        log_message(f"⛔ Error: {str(e)}")
        traceback.print_exc() # type: ignore
        raise

# ---------------------------------------------------------------- #

class LineLoader:
    ANIMATION_STEPS = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]

    def __init__(self, desc="Loading...", end="Done!", timeout=0.1, label=None) -> None:
        self._config = {"desc": desc, "end": end, "timeout": timeout}
        self._done = False
        self._lock = Lock()
        self.label = label

    def __enter__(self) -> None:
        with self._lock:
            self._done = False
        self._thread = Thread(target=self._animate, daemon=True)
        self._thread.start()

    def _animate(self) -> None:
        step_count = 0
        try:
            while True:
                with self._lock:
                    if self._done:
                        break
                if self.label:
                    self.label.config(text=f"{self.ANIMATION_STEPS[step_count]} {self._config['desc']}")
                time.sleep(self._config["timeout"])
                step_count = (step_count + 1) % len(self.ANIMATION_STEPS)
        except Exception as e:
            pass

    def __exit__(self, *args) -> None:
        with self._lock:
            self._done = True
        if self.label:
            self.label.config(text=self._config['end'])
        self._thread.join()

class MyApp:
    def __init__(self, root, latest_version):
        self.root = root
        self.latest_version = latest_version
        self.error_occurred = False  # Track if an error occurred
        self.iri_dataframes = None  # Initialize iri_dataframes
        self.root.title(f"Xeno Tool : {latest_version}")
        
        # Base directory
        self.base_dir_label = ttk.Label(root, text="Base Directory:")
        self.base_dir_label.grid(row=0, column=0, padx=5, pady=5, sticky="W")
        self.base_dir_entry = ttk.Entry(root, width=80)
        self.base_dir_entry.grid(row=0, column=1, padx=5, pady=5)
        self.base_dir_browse = ttk.Button(root, text="Browse", command=self.select_base_dir)
        self.base_dir_browse.grid(row=0, column=2, padx=5, pady=5)

        # Start button
        self.start_button = ttk.Button(root, text="Start Process", command=self.start_process)
        self.start_button.grid(row=1, column=0, columnspan=3, pady=10)

        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(root, orient="horizontal", length=500, mode="determinate", variable=self.progress_var)
        self.progress_bar.grid(row=2, column=0, columnspan=3, pady=10)

        # Progress label
        self.progress_label = ttk.Label(root, text="Progress: 0%")
        self.progress_label.grid(row=3, column=0, columnspan=3, pady=5)
        
        # Animation label
        self.animation_label = ttk.Label(root, text="")
        self.animation_label.grid(row=4, column=0, columnspan=3, pady=5)

        # Add export log button
        self.export_log_button = ttk.Button(root, text="Export Log", command=self.export_log)
        self.export_log_button.grid(row=10, column=0, columnspan=3, pady=5)
        
        # Automatically check for updates on startup
        self.root.after(1000, self.check_for_updates)  # Delay by 1 second to allow the UI to load
        
        # self.update_button = ttk.Button(root, text="Check for Updates", command=self.check_for_updates)
        # self.update_button.grid(row=11, column=0, columnspan=3, pady=5)
        
        #     # Add theme selector
        #     self.theme_label = ttk.Label(root, text="Select Theme:")
        #     self.theme_label.grid(row=9, column=0, padx=5, pady=5, sticky="w")

        #     self.theme_selector = ttk.Combobox(root, values=["Light", "Dark"], state="readonly")
        #     self.theme_selector.grid(row=9, column=1, padx=5, pady=5)
        #     self.theme_selector.bind("<<ComboboxSelected>>", lambda e: self.change_theme(self.theme_selector.get().lower()))
                
        # def change_theme(self, theme):
        #     if theme == "dark":
        #         self.root.tk_setPalette(background="#333", foreground="#fff")
        #     else:
        #         self.root.tk_setPalette(background="#fff", foreground="#000")
        
    
    def export_log(self):
        file_path = filedialog.asksaveasfilename(defaultextension=".txt", filetypes=[("Text Files", "*.txt")])
        if file_path:
            try:
                with open(file_path, "w", encoding="utf-8") as file:  # Specify UTF-8 encoding
                    # Write the log messages
                    log_content = log_text.get("1.0", tk.END)
                    file.write("Log Messages:\n")
                    file.write(log_content)
                    
                    # Check if there are any errors in the log
                    if "⛔ Error" in log_content:
                        file.write("\n\nIRI DataFrames: Not included due to errors in processing.\n")
                    elif hasattr(self, 'iri_dataframes') and isinstance(self.iri_dataframes, pd.DataFrame) and not self.iri_dataframes.empty:
                        file.write("\n\nIRI DataFrames:\n")
                        self.iri_dataframes.to_csv(file, index=False)
                    else:
                        file.write("\n\nIRI DataFrames: No data available.\n")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export log: {e}")


    def select_base_dir(self):
        """Open a dialog to select a base directory and update the entry field."""
        base_dir = filedialog.askdirectory()
        if base_dir:
            self.base_dir_entry.delete(0, tk.END)
            self.base_dir_entry.insert(0, base_dir)


    def start_process(self):
        base_dir = os.path.normpath(self.base_dir_entry.get())
        if not base_dir:
            messagebox.showwarning("Warning", "Please select a base directory!")
            return

        input_dir = os.path.join(base_dir, "input")
        output_dir = os.path.join(base_dir, "output")

        if not os.path.exists(input_dir):
            messagebox.showerror("Error", f"Input directory does not exist: {input_dir}")
            return

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        log_message("🔄 Starting file processing...")
        log_message(f"📂 Base directory: {base_dir}")

        self.progress_label.config(text="Processing files...")
        self.progress_var.set(0)
        
        # Run the background task with a loading spinner
        Thread(target=self.run_with_loader, args=(base_dir, input_dir, output_dir), daemon=True).start()


    def run_with_loader(self, base_dir, input_dir, output_dir):
        with LineLoader(desc="Processing", end="Completed", timeout=0.1, label=self.animation_label):
            self.background_task(base_dir, input_dir, output_dir)
             
                
    def background_task(self, base_dir, input_dir, output_dir):
        """Perform file processing task."""
        def progress_callback(progress):
            # Update the progress bar and label from the background task
            self.root.after(0, self.update_progress, progress)

        try:
            # Process the CSV files and store iri_dataframes
            iri_dataframes, _ = process_csv_files(output_dir)
            self.iri_dataframes = iri_dataframes  # Store iri_dataframes in the class

            main(base_dir, input_dir, output_dir, progress_callback)
            self.root.after(0, self.task_completed)  # Schedule the task_completed method
        except Exception as e:
            self.error_occurred = True  # Set error flag
            log_message(f"⛔ Error: {str(e)}")
            traceback.print_exc()  # Log the full traceback
            self.root.after(0, lambda: messagebox.showerror("Error", f"An error occurred: {str(e)}"))


    def update_progress(self, progress):
        """Update the progress bar and label."""
        self.progress_var.set(progress)
        self.progress_label.config(text=f"Progress: {int(progress)}%")


    def task_completed(self):
        """Called after the background task is finished."""
        self.progress_label.config(text="Processing completed!")
        self.progress_var.set(100)
        log_message("🎉 All files processed successfully!")
        messagebox.showinfo("Success", "Processing completed successfully!")
        
    
    def check_for_updates(self):
        current_version = self.latest_version
        try:
            version_url = "https://raw.githubusercontent.com/FUIJI/xeno-tool/refs/heads/dev/version.txt"            
            response = requests.get(version_url)
            response.raise_for_status()
            latest_version = response.text.strip()
            # for test version
            # latest_version = '1.0.2'

            # Compare versions
            if version.parse(latest_version) > version.parse(current_version):
                # Show a popup with the update information
                if messagebox.askyesno("Update Available", f"A new version ({latest_version}) is available."):
                    # webbrowser.open("https://github.com/FUIJI/xeno-tool/releases")  # Replace with your download URL
                    webbrowser.open("https://github.com/FUIJI/xeno-tool/blob/main/Xeno-Tool.exe")
            else:
                log_message("You are using the latest version.")
        except Exception as e:
            log_message(f"Error checking for updates: {e}")


# ---------------------------------------------------------------- #


if __name__ == "__main__":
    current_version = "1.0.2"
    
    # Init
    root = tk.Tk()
    app = MyApp(root, current_version)

    # Log area
    log_text = tk.Text(root, height=15, width=70, state="normal")
    log_text.grid(row=4, column=0, columnspan=3, pady=5)

    root.mainloop()
    
