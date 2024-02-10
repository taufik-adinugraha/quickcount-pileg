import warnings
warnings.filterwarnings("ignore", module="google.oauth2")

import os
import io
import json
import time
import tools
import zipfile
import requests
import numpy as np
import pandas as pd
import concurrent.futures
from fastapi import Request
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv
from pysurveycto import SurveyCTOObject
from datetime import datetime, timedelta
from fastapi import Form, FastAPI, UploadFile
from fastapi.responses import StreamingResponse, FileResponse


# ================================================================================================================
# Initial Setup

# Load env
load_dotenv()

# Define app
app = FastAPI()

# Global Variables
url_send_sms = os.environ.get('url_send_sms')
url_bubble = os.environ.get('url_bubble')
url_getUID = os.environ.get('url_getUID')
local_disk = os.environ.get('local_disk')
BUBBLE_API_KEY = os.environ.get('BUBBLE_API_KEY')
SCTO_SERVER_NAME = os.environ.get('SCTO_SERVER_NAME')
SCTO_USER_NAME = os.environ.get('SCTO_USER_NAME')
SCTO_PASSWORD = os.environ.get('SCTO_PASSWORD')
NUSA_USER_NAME = os.environ.get('NUSA_USER_NAME')
NUSA_PASSWORD = os.environ.get('NUSA_PASSWORD')

# Bubble Headers
headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}

# SMS Formats
format_pilpres = 'KK#UID#pilpres#01#02#03#TDKSAH'
format_dpr = 'KK#UID#dpr#p1#p2#...#p18#TDKSAH'
format_jabar = 'KK#UID#jabar#p1#p2#...#p18#TDKSAH'
format_universal = f'\nUtk pilpres:\n{format_pilpres}\n\nUtk DPR-RI:\n{format_dpr}\n\nUtk DPRD Jabar:\n{format_jabar}'


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

        # Check Error Type 1 (prefix)
        if info[0] == 'kk':

            try:
                uid = info[1].lower()
                template_error_msg = 'cek & kirim ulang dgn format:\n' + format_universal
                tmp = pd.read_excel(f'{local_disk}/target.xlsx', usecols=['UID'])

                # Check Error Type 2 (UID)
                if uid not in tmp['UID'].str.lower().tolist():
                    message = f'UID "{uid.upper()}" tidak terdaftar, ' + template_error_msg
                    error_type = 2

                else:

                    try:
                        # Get event
                        event = info[2].lower()

                        if event == 'pilpres':
                            try:
                                votes_pilpres = np.array(info[3:6]).astype(int)
                                # Invalid Votes
                                invalid_pilpres = int(info[6])
                                # Valid Votes
                                vote_capres_1 = votes_pilpres[0]
                                vote_capres_2 = votes_pilpres[1]
                                vote_capres_3 = votes_pilpres[2]
                                # Total Votes
                                total_capres = votes_pilpres.sum()
                                # Summary
                                summary = 'Event: pilpres\n' + '\n'.join([f'Paslon-0{i+1}: {votes_pilpres[i]}' for i in range(3)]) + f'\nTidak Sah: {invalid_pilpres}' + f'\nTotal: {total_capres+invalid_pilpres}\n'                                

                                # # Check Error Type 4 (maximum votes)
                                # if total_capres > 300:
                                #     message = summary + '\nJumlah suara melebihi 300, ' + template_error_msg
                                #     error_type = 4
                                # else:

                                message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama.'
                                # Retrieve data with this UID from Bubble database
                                filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid.upper()}]
                                filter_json = json.dumps(filter_params)
                                params = {"constraints": filter_json}
                                res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
                                data = res.json()
                                data = data['response']['results'][0]

                                # Get existing validator
                                if 'Validator' in data:
                                    validator = data['Validator']
                                else:
                                    validator = None

                                # Check if SCTO data exists
                                scto = data['SCTO-1']
                                if scto:
                                    if (np.array_equal(votes_pilpres, np.array(data['SCTO-1 AI Votes']).astype(int))) and (invalid_pilpres == int(data['SCTO-1 AI Invalid'])):
                                        status_pilpres = 'Verified'
                                        validator = 'System'
                                    else:
                                        status_pilpres = 'Not Verified'
                                else:
                                    status_pilpres = 'SMS Only'

                                # Completeness
                                if data['SMS-2'] and data['SMS-3'] and data['SCTO-1'] and data['SCTO-2'] and data['SCTO-3'] and data['SCTO-4']:
                                    complete = True
                                else:
                                    complete = False

                                # Payload
                                payload = {
                                    'Active': True,
                                    'SMS-1': True,
                                    'SMS-1 Gateway Port': port,
                                    'SMS-1 Gateway ID': gateway_number,
                                    'SMS-1 Sender': originator,
                                    'SMS-1 Timestamp': receive_date,
                                    'SMS-1 Votes Pilpres': votes_pilpres,
                                    'Vote Capres 1': vote_capres_1,
                                    'Vote Capres 2': vote_capres_2,
                                    'Vote Capres 3': vote_capres_3,
                                    'Total Valid Pilpres': total_capres,
                                    'SMS-1 Invalid Pilpres': invalid_pilpres,
                                    'Complete': complete,
                                    'Status Pilpres': status_pilpres,
                                    'Validator Pilpres': validator
                                }

                                raw_sms_status = 'Accepted'

                                # Load the JSON file into a dictionary
                                with open(f'{local_disk}/uid.json', 'r') as json_file:
                                    uid_dict = json.load(json_file)

                                # Forward data to Bubble database
                                _id = uid_dict[uid.upper()]
                                requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)


                            except Exception as e:
                                error_type = 3
                                message = f'Data tidak lengkap. Kirim ulang dengan format berikut:\n{format_pilpres}'
                                print(f'Error Location: SMS Pilpres - Error Type 3, keyword: {e}')
                        


                        elif event == 'dpr':
                            try:
                                votes_parpol_dpr = np.array(info[3:21]).astype(int)
                                # Invalid Votes
                                invalid_parpol_dpr = int(info[21])
                                # Valid Votes
                                vote_parpol_dpr_1 = votes_parpol_dpr[0]
                                vote_parpol_dpr_2 = votes_parpol_dpr[1]
                                vote_parpol_dpr_3 = votes_parpol_dpr[2]
                                vote_parpol_dpr_4 = votes_parpol_dpr[3]
                                vote_parpol_dpr_5 = votes_parpol_dpr[4]
                                vote_parpol_dpr_6 = votes_parpol_dpr[5]
                                vote_parpol_dpr_7 = votes_parpol_dpr[6]
                                vote_parpol_dpr_8 = votes_parpol_dpr[7]
                                vote_parpol_dpr_9 = votes_parpol_dpr[8]
                                vote_parpol_dpr_10 = votes_parpol_dpr[9]
                                vote_parpol_dpr_11 = votes_parpol_dpr[10]
                                vote_parpol_dpr_12 = votes_parpol_dpr[11]
                                vote_parpol_dpr_13 = votes_parpol_dpr[12]
                                vote_parpol_dpr_14 = votes_parpol_dpr[13]
                                vote_parpol_dpr_15 = votes_parpol_dpr[14]
                                vote_parpol_dpr_16 = votes_parpol_dpr[15]
                                vote_parpol_dpr_17 = votes_parpol_dpr[16]
                                vote_parpol_dpr_18 = votes_parpol_dpr[17]
                                # Total Votes
                                total_parpol_dpr = votes_parpol_dpr.sum()
                                # Summary
                                summary = f'Event: DPR-RI\n' + f'Suara Sah: {total_parpol_dpr}' + f'\nSuara Tidak Sah: {invalid_parpol_dpr}\n'

                                # # Check Error Type 4 (maximum votes)
                                # if total_parpol_dpr + invalid_parpol_dpr > 300:
                                #     message = summary + 'Jumlah suara melebihi 300, ' + template_error_msg
                                #     error_type = 4
                                # else:

                                message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama.'
                                # Retrieve data with this UID from Bubble database
                                filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid.upper()}]
                                filter_json = json.dumps(filter_params)
                                params = {"constraints": filter_json}
                                res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
                                data = res.json()
                                data = data['response']['results'][0]

                                # Check if SCTO data exists
                                scto = data['SCTO-2']
                                if scto:
                                    status_dpr = 'Not Verified'
                                else:
                                    status_dpr = 'SMS Only'

                                # Completeness
                                if data['SMS-1'] and data['SMS-3'] and data['SCTO-1'] and data['SCTO-2'] and data['SCTO-3'] and data['SCTO-4']:
                                    complete = True
                                else:
                                    complete = False

                                # Payload
                                payload = {
                                    'Active': True,
                                    'SMS-2': True,
                                    'SMS-2 Gateway Port': port,
                                    'SMS-2 Gateway ID': gateway_number,
                                    'SMS-2 Sender': originator,
                                    'SMS-2 Timestamp': receive_date,
                                    'Vote Parpol DPR 1': vote_parpol_dpr_1,
                                    'Vote Parpol DPR 2': vote_parpol_dpr_2,
                                    'Vote Parpol DPR 3': vote_parpol_dpr_3,
                                    'Vote Parpol DPR 4': vote_parpol_dpr_4,
                                    'Vote Parpol DPR 5': vote_parpol_dpr_5,
                                    'Vote Parpol DPR 6': vote_parpol_dpr_6,
                                    'Vote Parpol DPR 7': vote_parpol_dpr_7,
                                    'Vote Parpol DPR 8': vote_parpol_dpr_8,
                                    'Vote Parpol DPR 9': vote_parpol_dpr_9,
                                    'Vote Parpol DPR 10': vote_parpol_dpr_10,
                                    'Vote Parpol DPR 11': vote_parpol_dpr_11,
                                    'Vote Parpol DPR 12': vote_parpol_dpr_12,
                                    'Vote Parpol DPR 13': vote_parpol_dpr_13,
                                    'Vote Parpol DPR 14': vote_parpol_dpr_14,
                                    'Vote Parpol DPR 15': vote_parpol_dpr_15,
                                    'Vote Parpol DPR 16': vote_parpol_dpr_16,
                                    'Vote Parpol DPR 17': vote_parpol_dpr_17,
                                    'Vote Parpol DPR 18': vote_parpol_dpr_18,
                                    'Total Valid Parpol DPR': total_parpol_dpr,
                                    'SMS-2 Invalid DPR-RI': invalid_parpol_dpr,
                                    'Complete': complete,
                                    'Status DPR RI': status_dpr,
                                }

                                raw_sms_status = 'Accepted'

                                # Load the JSON file into a dictionary
                                with open(f'{local_disk}/uid.json', 'r') as json_file:
                                    uid_dict = json.load(json_file)

                                # Forward data to Bubble database
                                _id = uid_dict[uid.upper()]
                                requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)

                            except Exception as e:
                                error_type = 3
                                message = f'Data tidak lengkap. Kirim ulang dengan format berikut:\n{format_dpr}'
                                print(f'Error Location: SMS DPR RI- Error Type 3, keyword: {e}')
                        
                        
                        
                        elif event == 'jabar':
                            try:
                                votes_parpol_jabar = np.array(info[3:21]).astype(int)
                                # Invalid Votes
                                invalid_parpol_jabar = int(info[21])
                                # Valid Votes
                                vote_parpol_jabar_1 = votes_parpol_jabar[0]
                                vote_parpol_jabar_2 = votes_parpol_jabar[1]
                                vote_parpol_jabar_3 = votes_parpol_jabar[2]
                                vote_parpol_jabar_4 = votes_parpol_jabar[3]
                                vote_parpol_jabar_5 = votes_parpol_jabar[4]
                                vote_parpol_jabar_6 = votes_parpol_jabar[5]
                                vote_parpol_jabar_7 = votes_parpol_jabar[6]
                                vote_parpol_jabar_8 = votes_parpol_jabar[7]
                                vote_parpol_jabar_9 = votes_parpol_jabar[8]
                                vote_parpol_jabar_10 = votes_parpol_jabar[9]
                                vote_parpol_jabar_11 = votes_parpol_jabar[10]
                                vote_parpol_jabar_12 = votes_parpol_jabar[11]
                                vote_parpol_jabar_13 = votes_parpol_jabar[12]
                                vote_parpol_jabar_14 = votes_parpol_jabar[13]
                                vote_parpol_jabar_15 = votes_parpol_jabar[14]
                                vote_parpol_jabar_16 = votes_parpol_jabar[15]
                                vote_parpol_jabar_17 = votes_parpol_jabar[16]
                                vote_parpol_jabar_18 = votes_parpol_jabar[17]
                                # Total Votes
                                total_parpol_jabar = votes_parpol_jabar.sum()
                                # Summary
                                summary = f'Event: DPRD Jabar\n' + f'Suara Sah: {total_parpol_jabar}' + f'\nSuara Tidak Sah: {invalid_parpol_jabar}\n'

                                # # Check Error Type 4 (maximum votes)
                                # if total_parpol_jabar + invalid_parpol_jabar > 300:
                                #     message = summary + 'Jumlah suara melebihi 300, ' + template_error_msg
                                #     error_type = 4
                                # else:

                                message = summary + 'Berhasil diterima. Utk koreksi, kirim ulang dgn format yg sama.'
                                # Retrieve data with this UID from Bubble database
                                filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid.upper()}]
                                filter_json = json.dumps(filter_params)
                                params = {"constraints": filter_json}
                                res = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
                                data = res.json()
                                data = data['response']['results'][0]

                                # Check if SCTO data exists
                                scto = data['SCTO-4']
                                if scto:
                                    status_jabar = 'Not Verified'
                                else:
                                    status_jabar = 'SMS Only'

                                # Completeness
                                if data['SMS-1'] and data['SMS-2'] and data['SCTO-1'] and data['SCTO-2'] and data['SCTO-3'] and data['SCTO-4']:
                                    complete = True
                                else:
                                    complete = False

                                # Payload
                                payload = {
                                    'Active': True,
                                    'SMS-3': True,
                                    'SMS-3 Gateway Port': port,
                                    'SMS-3 Gateway ID': gateway_number,
                                    'SMS-3 Sender': originator,
                                    'SMS-3 Timestamp': receive_date,
                                    'Vote Parpol Jabar 1': vote_parpol_jabar_1,
                                    'Vote Parpol Jabar 2': vote_parpol_jabar_2,
                                    'Vote Parpol Jabar 3': vote_parpol_jabar_3,
                                    'Vote Parpol Jabar 4': vote_parpol_jabar_4,
                                    'Vote Parpol Jabar 5': vote_parpol_jabar_5,
                                    'Vote Parpol Jabar 6': vote_parpol_jabar_6,
                                    'Vote Parpol Jabar 7': vote_parpol_jabar_7,
                                    'Vote Parpol Jabar 8': vote_parpol_jabar_8,
                                    'Vote Parpol Jabar 9': vote_parpol_jabar_9,
                                    'Vote Parpol Jabar 10': vote_parpol_jabar_10,
                                    'Vote Parpol Jabar 11': vote_parpol_jabar_11,
                                    'Vote Parpol Jabar 12': vote_parpol_jabar_12,
                                    'Vote Parpol Jabar 13': vote_parpol_jabar_13,
                                    'Vote Parpol Jabar 14': vote_parpol_jabar_14,
                                    'Vote Parpol Jabar 15': vote_parpol_jabar_15,
                                    'Vote Parpol Jabar 16': vote_parpol_jabar_16,
                                    'Vote Parpol Jabar 17': vote_parpol_jabar_17,
                                    'Vote Parpol Jabar 18': vote_parpol_jabar_18,
                                    'Total Valid Parpol Jabar': total_parpol_jabar,
                                    'SMS-3 Invalid Jabar': invalid_parpol_jabar,
                                    'Complete': complete,
                                    'Status DPRD Jabar': status_jabar,
                                }

                                raw_sms_status = 'Accepted'

                                # Load the JSON file into a dictionary
                                with open(f'{local_disk}/uid.json', 'r') as json_file:
                                    uid_dict = json.load(json_file)

                                # Forward data to Bubble database
                                _id = uid_dict[uid.upper()]
                                requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)

                            except Exception as e:
                                error_type = 3
                                message = f'Data tidak lengkap. Kirim ulang dengan format berikut:\n{format_jabar}'
                                print(f'Error Location: SMS DPRD Jabar - Error Type 3, keyword: {e}')
                        
                        
                        else:
                            error_type = 3
                            message = 'Format tidak dikenali.\n' + format_universal                            
                    
                    except Exception as e:
                        error_type = 3
                        message = 'Format tidak dikenali.\n' + format_universal
                        print(f'Error Location: SMS - Error Type 3, keyword: {e}')


            except Exception as e:
                error_type = 1
                message = f'Format tidak dikenali. Kirim ulang dengan format berikut:\n{format_universal}'
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
        requests.post(f'{url_bubble}/RAW_SMS', headers=headers, data=payload_raw)



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

    # Break into batches
    batch_size = 100
    n_batches = int(np.ceil(len(df) / batch_size))

    for batch in range(n_batches):
        start = batch * batch_size
        end = min((batch + 1) * batch_size, len(df)) - 1 
        tdf = df.loc[start:end, :]

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
                tdf['UID'],
                tdf['Dapil DPR RI'],
                tdf['Dapil DPRD Jawa Barat'],
                tdf['Korwil'],
                tdf['Kab/Kota'],
                tdf['Kecamatan'],
                tdf['Kelurahan'],
                tdf['Kab/Kota Ori'],
                tdf['Kecamatan Ori'],
                tdf['Kelurahan Ori']
            )
        ])

        # Populate votes table in bulk
        headers = {
            'Authorization': f'Bearer {BUBBLE_API_KEY}', 
            'Content-Type': 'text/plain'
            }
        requests.post(f'{url_bubble}/Votes/bulk', headers=headers, data=data)

        time.sleep(3)

    # Get UIDs and store as json
    headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
    uid_dict = {}
    for uid_start in range(1, len(df), 50):
        params = {'start': uid_start, 'end': uid_start+50}
        res = requests.get(url_getUID, headers=headers, params=params)
        out = res.json()['response']
        uid_dict.update(zip(out['UID'], out['id_']))

    # headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}
    # res = requests.get(f'{url_bubble}/Votes', headers=headers)
    # uid_dict = {i['UID']:i['_id'] for i in res.json()['response']['results']}

    with open(f'{local_disk}/uid.json', 'w') as json_file:
        json.dump(uid_dict, json_file)

    # Generate xlsform logic using the target file
    tools.create_xlsform_pilpres()
    tools.create_xlsform_dpr()
    tools.create_xlsform_dpd()
    tools.create_xlsform_jabar()
    xlsform_paths = [
        f'{local_disk}/xlsform_pilpres.xlsx',
        f'{local_disk}/xlsform_dpr.xlsx',
        f'{local_disk}/xlsform_dpd.xlsx',
        f'{local_disk}/xlsform_jabar.xlsx'
    ]

    def create_zip_file(paths, output_filename):
        with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for path in paths:
                zip_file.write(path, arcname=os.path.basename(path))

    zip_filename = "xlsforms.zip"
    create_zip_file(xlsform_paths, zip_filename)
    return FileResponse(zip_filename, media_type='application/zip', filename='xlsforms.zip')

    # def file_generator(paths):
    #     for path in paths:
    #         filename = os.path.basename(path)
    #         with open(path, "rb") as file:
    #             while True:
    #                 chunk = file.read(8192)  # Adjust chunk size as needed
    #                 if not chunk:
    #                     break
    #                 yield chunk, filename

    # def zip_generator(files):
    #     with io.BytesIO() as buffer:
    #         with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
    #             for content, filename in files:
    #                 zip_file.writestr(filename, content)
    #         buffer.seek(0)
    #         yield from buffer.getvalue()

    # response = StreamingResponse(zip_generator(file_generator(xlsform_paths)), media_type='application/zip')
    # response.headers["Content-Disposition"] = "attachment; filename=xlsforms.zip"

    # return response

    # def file_generator(paths):
    #     with io.BytesIO() as buffer:
    #         with zipfile.ZipFile(buffer, 'w') as zip_file:
    #             for path in paths:
    #                 zip_file.write(path, arcname=path.split('/')[-1])
    #         buffer.seek(0)
    #         yield from buffer

    # response = StreamingResponse(file_generator(xlsform_paths), media_type='application/zip')
    # response.headers["Content-Disposition"] = "attachment; filename=xlsforms.zip"

    # return response



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

    # Calculate the oldest completion date based on the current time
    date_obj = input_time - timedelta(seconds=301)

    ################# PILPRES #################
    print(f'\nCollect Pilpres: {input_time}\t Current Time {datetime.now()}')
    try:
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)        
        # Retrieve data from SCTO
        list_data = scto.get_form_data('qc_pilpres_pks_jabar', format='json', shape='wide', oldest_completion_date=date_obj)
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process_pilpres, data)
    
    except Exception as e:
        print(f'Process: scto_pilpres endpoint\t Keyword: {e}\n')

    ################# DPR-RI #################
    print(f'\nCollect DPR-RI: {input_time}\t Current Time {datetime.now()}')
    try:
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)        
        # Retrieve data from SCTO
        list_data = scto.get_form_data('qc_dprri_pks_jabar', format='json', shape='wide', oldest_completion_date=date_obj)
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process_dpr, data)
    
    except Exception as e:
        print(f'Process: scto_dpr endpoint\t Keyword: {e}\n')

    ################# DPD I #################
    print(f'\nCollect DPD-I: {input_time}\t Current Time {datetime.now()}')
    try:
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
        # Retrieve data from SCTO
        list_data = scto.get_form_data('qc_dpd_pks_jabar', format='json', shape='wide', oldest_completion_date=date_obj)
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process_dpd, data)
    
    except Exception as e:
        print(f'Process: scto_dpd endpoint\t Keyword: {e}\n')

    ################# DPRD Provinsi Jawa Barat #################
    print(f'\nCollect DPRD Jabar: {input_time}\t Current Time {datetime.now()}')
    try:
        scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
        # Retrieve data from SCTO
        list_data = scto.get_form_data('qc_dprdprov_pks_jabar', format='json', shape='wide', oldest_completion_date=date_obj)
        if len(list_data) > 0:
            for data in list_data:
                # Run 'scto_process' function asynchronously
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(tools.scto_process_jabar, data)
    
    except Exception as e:
        print(f'Process: scto_jabar endpoint\t Keyword: {e}\n')
