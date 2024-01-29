import warnings
warnings.filterwarnings("ignore", module="google.oauth2")

import os
import io
import json
import tools
import zipfile
import requests
import numpy as np
import pandas as pd
import concurrent.futures
from fastapi import Request
from typing import Optional
from dotenv import load_dotenv
from pysurveycto import SurveyCTOObject
from datetime import datetime, timedelta
from fastapi import Form, FastAPI, UploadFile
from fastapi.responses import StreamingResponse


# ================================================================================================================
# Initial Setup

# Load env
load_dotenv()

# Define app
app = FastAPI()

# Global Variables
url_send_sms = os.environ.get('url_send_sms')
url_bubble = os.environ.get('url_bubble')
local_disk = os.environ.get('local_disk')
BUBBLE_API_KEY = os.environ.get('BUBBLE_API_KEY')
SCTO_SERVER_NAME = os.environ.get('SCTO_SERVER_NAME')
SCTO_USER_NAME = os.environ.get('SCTO_USER_NAME')
SCTO_PASSWORD = os.environ.get('SCTO_PASSWORD')
NUSA_USER_NAME = os.environ.get('NUSA_USER_NAME')
NUSA_PASSWORD = os.environ.get('NUSA_PASSWORD')

# Bubble Headers
headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}



# ================================================================================================================
# Endpoint to read the "inbox.txt" file
@app.get("/sms_inbox")
async def read_inbox():
    try:
        with open(f"{local_disk}/inbox.json", "r") as json_file:
            data = [json.loads(line) for line in json_file]
        return {"inbox_data": data}
    except FileNotFoundError:
        return {"message": "File not found"}



# ================================================================================================================
# Endpoint to receive SMS message, to validate, and to forward the pre-processed data

# Define the number of endpoints
num_endpoints = 16

# Endpoint to receive SMS message, to validate, and to forward the pre-processed data
for port in range(17, num_endpoints + 17):
    @app.post(f"/receive-{port}")
    async def receive_sms(
        request: Request,
        id: str = Form(...),
        gateway_number: str = Form(...),
        originator: str = Form(...),
        msg: str = Form(...),
        receive_date: str = Form(...)
    ):

        # Extract the port number from the request
        port = request.url.path.split('-')[-1]
        
        # Create a dictionary to store the data
        raw_data = {
            "ID": id,
            "Gateway Port": port,
            "Gateway ID": gateway_number,
            "Sender": originator,
            "Message": msg,
            "Receive Date": receive_date
        }

        # Log the received data to a JSON file
        with open(f"{local_disk}/inbox.json", "a") as json_file:
            json.dump(raw_data, json_file)
            json_file.write('\n')  # Add a newline to separate the JSON objects

        # Split message
        info = msg.lower().split('#')

        # Default Values
        error_type = None
        raw_sms_status = 'Rejected'
        format = 'KK#UID#capres1*capres2*capres3#partai1*partai2*...*partai17'

        # Check Error Type 1 (prefix)
        if info[0] == 'kk':

            try:
                uid = info[1].lower()
                capres = info[2].split('*')
                partai = info[3].split('*')

                template_error_msg = 'cek & kirim ulang dgn format:\n' + format

                tmp = pd.read_excel(f'{local_disk}/target.xlsx', usecols=['UID'])

                # Check Error Type 2 (UID)
                if uid not in tmp['UID'].str.lower().tolist():
                    message = f'UID "{uid.upper()}" tidak terdaftar, ' + template_error_msg
                    error_type = 2
                else:
                    # Check Error Type 3 & 4 (data completeness)
                    if len(capres) != 3:
                        message = 'Data pilpres tidak lengkap, ' + template_error_msg
                        error_type = 3
                    elif len(partai) != 17:
                        message = 'Data pileg tidak lengkap, ' + template_error_msg
                        error_type = 4
                    else:
                        # Get capres votes
                        votes_capres = np.array(capres).astype(int)
                        vote_capres_1 = votes_capres[0]
                        vote_capres_2 = votes_capres[1]
                        vote_capres_3 = votes_capres[2]
                        # Get parpol votes
                        votes_parpol = np.array(partai).astype(int)
                        vote_parpol_1 = votes_parpol[0]
                        vote_parpol_2 = votes_parpol[1]
                        vote_parpol_3 = votes_parpol[2]
                        vote_parpol_4 = votes_parpol[3]
                        vote_parpol_5 = votes_parpol[4]
                        vote_parpol_6 = votes_parpol[5]
                        vote_parpol_7 = votes_parpol[6]
                        vote_parpol_8 = votes_parpol[7]
                        vote_parpol_9 = votes_parpol[8]
                        vote_parpol_10 = votes_parpol[9]
                        vote_parpol_11 = votes_parpol[10]
                        vote_parpol_12 = votes_parpol[11]
                        vote_parpol_13 = votes_parpol[12]
                        vote_parpol_14 = votes_parpol[13]
                        vote_parpol_15 = votes_parpol[14]
                        vote_parpol_16 = votes_parpol[15]
                        vote_parpol_17 = votes_parpol[16]
                        # Get total votes
                        total_valid_capres = np.array(votes_capres).astype(int).sum()
                        total_valid_parpol = np.array(votes_parpol).astype(int).sum()
                        summary = f'Suara Sah Pilpres: {total_valid_capres}' + f'\nSuara Sah Pileg: {total_valid_parpol}'
                        # Check Error Type 5 & 6 (maximum votes for pilpres)
                        if total_valid_capres > 300:
                            message = summary + 'Jumlah suara pilpres melebihi 300, ' + template_error_msg
                            error_type = 5
                        elif total_valid_parpol > 300:
                            message = summary + 'Jumlah suara pileg melebihi 300, ' + template_error_msg
                            error_type = 6
                        else:
                            message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama:\n' + format

                            # Retrieve data with this UID from Bubble database
                            filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid.upper()}]
                            filter_json = json.dumps(filter_params)
                            params = {"constraints": filter_json}
                            res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
                            data = res.json()
                            data = data['response']['results'][0]

                            # Check if SCTO data exists
                            scto = data['SCTO']
                            if scto:
                                status = 'Not Verified'
                            else:
                                status = 'SMS Only'
                            
                            # Extract the hour as an integer
                            tmp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                            hour = tmp.hour
                            
                            # Delta Time
                            if 'SCTO Timestamp' in data:
                                sms_timestamp = datetime.strptime(receive_date, "%Y-%m-%d %H:%M:%S")
                                scto_timestamp = datetime.strptime(data['SCTO Timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
                                delta_time = abs(scto_timestamp - sms_timestamp)
                                delta_time_hours = delta_time.total_seconds() / 3600
                            else:
                                delta_time_hours = None

                            # Payload
                            payload = {
                                'Active': True,
                                'SMS': True,
                                'SMS Int': 1,
                                'UID': uid.upper(),
                                'SMS Gateway Port': port,
                                'SMS Gateway ID': gateway_number,
                                'SMS Sender': originator,
                                'SMS Timestamp': receive_date,
                                'SMS Hour': hour,
                                'SMS Votes Parpol': votes_parpol,
                                'SMS Votes Capres': votes_capres,
                                'Vote Parpol 1': vote_parpol_1,
                                'Vote Parpol 2': vote_parpol_2,
                                'Vote Parpol 3': vote_parpol_3,
                                'Vote Parpol 4': vote_parpol_4,
                                'Vote Parpol 5': vote_parpol_5,
                                'Vote Parpol 6': vote_parpol_6,
                                'Vote Parpol 7': vote_parpol_7,
                                'Vote Parpol 8': vote_parpol_8,
                                'Vote Parpol 9': vote_parpol_9,
                                'Vote Parpol 10': vote_parpol_10,
                                'Vote Parpol 11': vote_parpol_11,
                                'Vote Parpol 12': vote_parpol_12,
                                'Vote Parpol 13': vote_parpol_13,
                                'Vote Parpol 14': vote_parpol_14,
                                'Vote Parpol 15': vote_parpol_15,
                                'Vote Parpol 16': vote_parpol_16,
                                'Vote Parpol 17': vote_parpol_17,
                                'Vote Capres 1': vote_capres_1,
                                'Vote Capres 2': vote_capres_2,
                                'Vote Capres 3': vote_capres_3,
                                'Complete': scto,
                                'Status': status,
                                'Delta Time': delta_time_hours,
                            }

                            raw_sms_status = 'Accepted'

                            # Load the JSON file into a dictionary
                            with open(f'{local_disk}/uid.json', 'r') as json_file:
                                uid_dict = json.load(json_file)

                            # Forward data to Bubble database
                            _id = uid_dict[uid.upper()]
                            out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
                            print(out)

            except Exception as e:
                error_type = 1
                message = f'Format tidak dikenali. Kirim ulang dengan format berikut:\n{format}'
                print(f'Error Location: SMS - Error Type 1, keyword: {e}')

            # Return the message to the sender via SMS Masking
            params = {
                "user": NUSA_USER_NAME,
                "password": NUSA_PASSWORD,
                "SMSText": message,
                "GSM": originator,
                "output": "json",
            }
            requests.get(url_send_sms, params=params)

        elif msg == 'the gateway is active':
            # Payload (Gateway Check)
            payload_status = {
                'Gateway Port': port,
                'Gateway Status': True,
                'Last Check': receive_date,
            }

            # Retrieve data with this SIM Number from Bubble database (GatewayCheck)
            filter_params = [{"key": "Gateway ID", "constraint_type": "text contains", "value": gateway_number}]
            filter_json = json.dumps(filter_params)
            params = {"constraints": filter_json}
            res = requests.get(f'{url_bubble}/GatewayCheck', headers=headers, params=params)
            data = res.json()
            data = data['response']['results'][0]
            # Forward data to Bubble database (Check Gateway)
            _id = data['_id']
            requests.patch(f'{url_bubble}/GatewayCheck/{_id}', headers=headers, data=payload_status)
            # Set SMS status
            raw_sms_status = 'Check Gateway'        
        
        else:
            error_type = 0

        # Payload (RAW SMS)
        payload_raw = {
            'SMS ID': id,
            'Receive Date': receive_date,
            'Sender': originator,
            'Gateway Port': port, 
            'Gateway ID': gateway_number,
            'Message': msg,
            'Error Type': error_type,
            'Status': raw_sms_status
        }

        # Forward data to Bubble database (Raw SMS)
        # requests.post(f'{url_bubble}/RAW_SMS', headers=headers, data=payload_raw)

        ###############
        print(payload_raw)



# ================================================================================================================
# Endpoint to check gateway status
@app.post("/check_gateway_status")
async def check_gateway_status(     
    gateway_1: Optional[str] = Form(None),
    gateway_2: Optional[str] = Form(None),
    gateway_3: Optional[str] = Form(None),
    gateway_4: Optional[str] = Form(None),
    gateway_5: Optional[str] = Form(None),
    gateway_6: Optional[str] = Form(None),
    gateway_7: Optional[str] = Form(None),
    gateway_8: Optional[str] = Form(None),
    gateway_9: Optional[str] = Form(None),
    gateway_10: Optional[str] = Form(None),
    gateway_11: Optional[str] = Form(None),
    gateway_12: Optional[str] = Form(None),
    gateway_13: Optional[str] = Form(None),
    gateway_14: Optional[str] = Form(None),
    gateway_15: Optional[str] = Form(None),
    gateway_16: Optional[str] = Form(None),
    gateway_17: Optional[str] = Form(None),
    gateway_18: Optional[str] = Form(None),
    gateway_19: Optional[str] = Form(None),
    gateway_20: Optional[str] = Form(None),
    gateway_21: Optional[str] = Form(None),
    gateway_22: Optional[str] = Form(None),
    gateway_23: Optional[str] = Form(None),
    gateway_24: Optional[str] = Form(None),
    gateway_25: Optional[str] = Form(None),
    gateway_26: Optional[str] = Form(None),
    gateway_27: Optional[str] = Form(None),
    gateway_28: Optional[str] = Form(None),
    gateway_29: Optional[str] = Form(None),
    gateway_30: Optional[str] = Form(None),
    gateway_31: Optional[str] = Form(None),
    gateway_32: Optional[str] = Form(None),
):

    numbers = [gateway_1, gateway_2, gateway_3, gateway_4, gateway_5, gateway_6, gateway_7, gateway_8, gateway_9, gateway_10, 
               gateway_11, gateway_12, gateway_13, gateway_14, gateway_15, gateway_16, gateway_17, gateway_18, gateway_19,
               gateway_20, gateway_21, gateway_22, gateway_23, gateway_24, gateway_25, gateway_26, gateway_27, gateway_28,
               gateway_29, gateway_30, gateway_31, gateway_32]

    # Sent trigger via SMS Masking
    for num in numbers:
        # if number is not empty
        if num:
            params = {
                "user": NUSA_USER_NAME,
                "password": NUSA_PASSWORD,
                "SMSText": 'the gateway is active',
                "GSM": num,
                "output": "json",
            }
            requests.get(url_send_sms, params=params)



# ================================================================================================================
# Endpoint to generate UID
@app.post("/getUID")
async def get_uid(
    N_TPS: int = Form(...)
    ):

    # Generate target file
    tools.create_target(N_TPS)
    
    # Forward file to Bubble database
    excel_file_path = f'{local_disk}/target.xlsx'
    
    def file_generator():
        with open(excel_file_path, 'rb') as file_content:
            yield from file_content

    response = StreamingResponse(file_generator(), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response.headers["Content-Disposition"] = f"attachment; filename=target.xlsx"

    # Return response
    return response



# ================================================================================================================
# Endpoint to generate SCTO xlsform
@app.post("/generate_xlsform")
async def generate_xlsform(
    target_file: UploadFile = Form(...)
    ):

    # Save the target file to a temporary location
    with open(f'{local_disk}/target.xlsx', 'wb') as target_file_content:
        target_file_content.write(target_file.file.read())

    # Get UIDs from the target file
    df = pd.read_excel(f'{local_disk}/target.xlsx')

    # Rename regions
    df['Kab/Kota Ori'] = df['Kab/Kota'].copy()
    df['Kecamatan Ori'] = df['Kecamatan'].copy()
    df['Kelurahan Ori'] = df['Kelurahan'].copy()
    for index, row in df.iterrows():
        input_regions = [row['Kab/Kota'], row['Kecamatan'], row['Kelurahan']]
        output_regions = tools.rename_region(input_regions)
        df.loc[index, 'Kab/Kota'] = output_regions[0]
        df.loc[index, 'Kecamatan'] = output_regions[1]
        df.loc[index, 'Kelurahan'] = output_regions[2]

    # Save the target file after renaming regions
    df.to_excel(f'{local_disk}/target.xlsx', index=False)

    # Generate Text for API input
    data = '\n'.join([
        f'{{"UID": "{uid}", '
        f'"Active": false, '
        f'"Complete": false, '
        f'"Dapil DPR RI": "{dapil_dprri}", '
        f'"Dapil DPRD Jawa Barat": "{dapil_dprd}", '
        f'"SMS-1": false, '
        f'"SMS-2": false, '
        f'"SCTO-1": false, '
        f'"SCTO-2": false, '        
        f'"SCTO-3": false, '
        f'"Status Pilpres": "Empty", '
        f'"Status DPR RI": "Empty", '
        f'"Status DPRD Jabar": "Empty", '
        f'"Korwil": "{korwil}", '
        f'"Kab/Kota": "{kab_kota}", '
        f'"Kecamatan": "{kecamatan}", '
        f'"Kelurahan": "{kelurahan}", '
        f'"Kab/Kota Ori": "{kab_kota_ori}", '
        f'"Kecamatan Ori": "{kecamatan_ori}", '
        f'"Kelurahan Ori": "{kelurahan_ori}"}}'
        for uid, dapil_dprri, dapil_dprd, korwil, kab_kota, kecamatan, kelurahan, kab_kota_ori, kecamatan_ori, kelurahan_ori in zip(
            df['UID'],
            df['Dapil DPR RI'],
            df['Dapil DPRD Jawa Barat'],
            df['Korwil'],
            df['Kab/Kota'],
            df['Kecamatan'],
            df['Kelurahan'],
            df['Kab/Kota Ori'],
            df['Kecamatan Ori'],
            df['Kelurahan Ori']
        )
    ])

    # Populate votes table in bulk
    headers = {
        'Authorization': f'Bearer {BUBBLE_API_KEY}', 
        'Content-Type': 'text/plain'
        }
    requests.post(f'{url_bubble}/Votes/bulk', headers=headers, data=data)

    # Get UIDs and store as json
    headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
    res = requests.get(f'{url_bubble}/Votes', headers=headers)
    uid_dict = {i['UID']:i['_id'] for i in res.json()['response']['results']}
    with open(f'{local_disk}/uid.json', 'w') as json_file:
        json.dump(uid_dict, json_file)

    # Generate xlsform logic using the target file
    tools.create_xlsform_pilpres()
    tools.create_xlsform_dpr()
    tools.create_xlsform_jabar()
    xlsform_paths = [
        f'{local_disk}/xlsform_pilpres.xlsx',
        f'{local_disk}/xlsform_dpr.xlsx',
        f'{local_disk}/xlsform_jabar.xlsx'
    ]

    def file_generator(paths):
        with io.BytesIO() as buffer:
            with zipfile.ZipFile(buffer, 'w') as zip_file:
                for path in paths:
                    zip_file.write(path, arcname=path.split('/')[-1])
            buffer.seek(0)
            yield from buffer

    response = StreamingResponse(file_generator(xlsform_paths), media_type='application/zip')
    response.headers["Content-Disposition"] = "attachment; filename=xlsforms.zip"

    return response



# ================================================================================================================
# Endpoint to delete event
@app.post("/delete_event")
async def delete_event(
    ):
    os.system(f'rm -f {local_disk}/uid.json {local_disk}/target.xlsx {local_disk}/*xlsform*')



# ================================================================================================================
# Endpoint to trigger SCTO data processing
@app.post("/scto_data")
def scto_data(
    input_time: datetime = Form(...), 
    ):

    #####################
    print(f'\nInput Time: {input_time}')
    #####################

    # Calculate the oldest completion date based on the current time
    date_obj = input_time - timedelta(seconds=301)

    ################# PILPRES #################
    try:
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)        
        # Retrieve data from SCTO (Pilpres)
        list_data = scto.get_form_data('qc_pilpres_pks_jabar', format='json', shape='wide', oldest_completion_date=date_obj)
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process_pilpres, data)
    
    except Exception as e:
        print(f'Process: scto_pilpres endpoint\t Keyword: {e}\n')

    ################# DPR-RI #################
    try:
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)        
        # Retrieve data from SCTO (Pilpres)
        list_data = scto.get_form_data('qc_dprri_pks_jabar', format='json', shape='wide', oldest_completion_date=date_obj)
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process_dpr, data)
    
    except Exception as e:
        print(f'Process: scto_dpr endpoint\t Keyword: {e}\n')

    ################# DPRD Provinsi Jawa Barat #################
    try:
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
        # Retrieve data from SCTO (Pilpres)
        list_data = scto.get_form_data('qc_dprdprov_pks_jabar', format='json', shape='wide', oldest_completion_date=date_obj)
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process_jabar, data)
    
    except Exception as e:
        print(f'Process: scto_jabar endpoint\t Keyword: {e}\n')