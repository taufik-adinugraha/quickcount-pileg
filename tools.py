import os
import re
import json
import random
import requests
import threading
import numpy as np
import pandas as pd
from Bio import Align
import geopandas as gpd
from dotenv import load_dotenv
from shapely.geometry import Point
from google.cloud import documentai
from pysurveycto import SurveyCTOObject
from datetime import datetime, timedelta



# ================================================================================================================
# Initial Setup

# Load env
load_dotenv()

# Load the shapefile
shapefile_path = 'location.shp'
gdf = gpd.read_file(shapefile_path)
gdf.crs = "EPSG:4326"

# Load region data from JSON
with open('region.json', 'r') as json_file:
    region_data = json.load(json_file)

# Create a threading lock for synchronization
print_lock = threading.Lock()

# Global Variables
url_send_sms = os.environ.get('url_send_sms')
url_bubble = os.environ.get('url_bubble')
local_disk = os.environ.get('local_disk')
BUBBLE_API_KEY = os.environ.get('BUBBLE_API_KEY')
SCTO_SERVER_NAME = os.environ.get('SCTO_SERVER_NAME')
SCTO_USER_NAME = os.environ.get('SCTO_USER_NAME')
SCTO_PASSWORD = os.environ.get('SCTO_PASSWORD')

# Bubble Headers
headers = {'Authorization': f'Bearer {BUBBLE_API_KEY}'}

# Parpol
list_parpol = ['PKB', 'GERINDRA', 'PDI-P', 'GOLKAR', 'NASDEM', 'PARTAI BURUH', 'PARTAI GELORA', 'PKS', 'PKN', 'PARTAI HANURA', 'PARTAI GARUDA', 'PAN', 'PBB', 
                'DEMOKRAT', 'PSI', 'PERINDO', 'PPP', 'PARTAI UMMAT']
parpol = {'PKB': 'PKB', 
          'GERINDRA': 'GERINDRA', 
          'PDI-P': 'PDI', 
          'GOLKAR': 'GOLKAR', 
          'NASDEM': 'NASDEM', 
          'PARTAI BURUH': 'BURUH', 
          'PARTAI GELORA': 'GELORA', 
          'PKS': 'PKS', 
          'PKN': 'PKN', 
          'PARTAI HANURA': 'HANURA', 
          'PARTAI GARUDA': 'GARUDA', 
          'PAN': 'PAN', 
          'PBB': 'PBB', 
          'DEMOKRAT': 'DEMOKRAT', 
          'PSI': 'PSI', 
          'PERINDO': 'PERINDO', 
          'PPP': 'PPP', 
          'PARTAI UMMAT': 'UMMAT'}

# ================================================================================================================
# Auxiliary Functions

# Rename regions
def rename_region(data):
    provinsi = 'Jawa Barat'
    # kabupaten/kota
    reference = list(region_data[provinsi].keys())
    kabkota = find_closest_string(data[0], reference, 'Kab/Kota')       
    # kecamatan
    reference = list(region_data[provinsi][kabkota].keys())
    kecamatan = find_closest_string(data[1], reference, 'Kecamatan') 
    # kelurahan
    reference = list(region_data[provinsi][kabkota][kecamatan])
    kelurahan = find_closest_string(data[2], reference, 'Kelurahan')
    return kabkota, kecamatan, kelurahan

def preprocess_text(text):
    # Remove spaces and punctuation, convert to lowercase
    return re.sub(r'\W+', '', text.lower())

def compare_sequences(seq1, seq2):
    aligner = Align.PairwiseAligner()
    alignments = aligner.align(seq1, seq2)
    best_alignment = alignments[0]  # Assuming you want the best alignment
    return best_alignment.score

def compare_with_list(string1, string2_list):
    scores = []
    for seq2 in string2_list:
        score = compare_sequences(string1, seq2)
        scores.append(score)
    return scores

def find_closest_string(string1, string_list, region):
    if region == 'Kab/Kota':
        first_string = string1.split(' ')[0].lower()
        if first_string != 'kota':
            if first_string not in ['kab.', 'kabupaten', 'kab']:
                string1 = 'Kab. ' + string1
    preprocessed_string_list = [preprocess_text(s) for s in string_list]
    preprocessed_target = preprocess_text(string1)
    scores = compare_with_list(preprocessed_target, preprocessed_string_list)
    ss = [len([i for i in list(s2) if i not in list(preprocessed_target)]) for s2 in preprocessed_string_list]
    tt = [np.sum([preprocessed_target.count(t1) for t1 in list(t2)])/len(preprocessed_target) for t2 in preprocessed_string_list]
    scores = np.array(scores) - np.array(ss) - np.array(tt)
    closest_index = np.argmax(scores)
    return string_list[closest_index]


# Get administrative regions from coordinate
def get_location(coordinate):
    # Create a Shapely Point object from the input coordinate
    point = Point(coordinate)
    # Check which polygon contains the point
    selected_row = gdf[gdf.geometry.contains(point)]
    # Output
    out = {
        'Provinsi': selected_row['Provinsi'].values[0],
        'Kab/Kota': selected_row['Kab/Kota'].values[0],
        'Kecamatan': selected_row['Kecamatan'].values[0],
        'Kelurahan': selected_row['Kelurahan'].values[0]
    }
    return out


# Document inference
def read_form(scto, attachment_url):
    project_id = "quick-count-410523"
    processor_id = "3ae5a6c7afc5a8dd"
    location = "us"

    # Initialize the DocumentProcessorServiceClient
    client = documentai.DocumentProcessorServiceClient.from_service_account_file('document-ai.json')
    
    # Construct the processor path
    name = f'projects/{project_id}/locations/{location}/processors/{processor_id}'
        
    # Convert the attachment URL content to a byte array
    file_content = scto.get_attachment(attachment_url)
    
    # Load binary data
    raw_document = documentai.RawDocument(content=file_content, mime_type='image/jpeg')

    # Configure the process request
    request = documentai.ProcessRequest(
        name=name,
        raw_document=raw_document,
    )

    # Process Document
    out = client.process_document(request)
    entities = out.document.entities
    output = {}
    if len(entities) > 0:
        # Valid Votes
        try:
            for entity in entities[0].properties:
                output.update({entity.type_: entity.normalized_value.text})
        except:
            pass
        try:
            # Invalid votes
            entity = entities[1]
            output.update({entity.type_: entity.normalized_value.text})
        except:
            pass
    else:
        ai_votes = [0] * 3
        ai_invalid = 0

    # Post-processing
    ai_votes = [0] * 3
    for var_ in range(3):
        try:
            ai_votes[var_] = remove_non_numbers_and_convert_to_int(output[f'suara{var_+1}'])
        except:
            ai_votes[var_] = 0
    try:
        ai_invalid = remove_non_numbers_and_convert_to_int(output['suara_rusak'])
    except:
        ai_invalid = 0
            
    return ai_votes, ai_invalid


def remove_non_numbers_and_convert_to_int(input_string):
    # Use a list comprehension to create a string containing only digits
    digits_only = ''.join(char for char in input_string if char.isdigit())
    # Convert the string of digits to an integer
    result_integer = int(digits_only)
    return result_integer



# ================================================================================================================
# Functions to generate UID

def generate_code():
    characters = 'abcdefghjkmnpqrstuvwxyz123456789'
    code = ''.join([random.choice(characters) for i in range(3)])
    return code.upper()

def generate_unique_codes(N):
    codes = []
    while len(codes) < N:
        code = generate_code()
        if code not in codes:
            codes.append(code)
    return codes

def create_target(N):
    df = pd.DataFrame(columns=['UID', 'Korwil', 'Dapil DPR RI', 'Dapil DPRD Jawa Barat', 'Kab/Kota', 'Kecamatan', 'Kelurahan'])
    # Generate unique IDs
    df['UID'] = generate_unique_codes(N)
    # Save excel file
    with pd.ExcelWriter(f'{local_disk}/target.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='survey')



# ================================================================================================================
# Function to generate SCTO xlsform Pilpres

def create_xlsform_pilpres():

    # Load target data from Excel
    target_data = pd.read_excel(f'{local_disk}/target.xlsx')

    # List UID
    list_uid = '|'.join(target_data['UID'].tolist())
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the survey sheet
    survey_pilpres = pd.DataFrame(columns=['type', 'name', 'label', 'required', 'choice_filter', 'calculation', 'constraint'])

    # default fields
    survey_pilpres['type'] = ['start', 'end', 'deviceid', 'phonenumber', 'username', 'calculate', 'calculate', 'caseid']
    survey_pilpres['name'] = ['starttime', 'endtime', 'deviceid', 'devicephonenum', 'username', 'device_info', 'duration', 'caseid']
    survey_pilpres['calculation'] = ['', '', '', '', '', 'device-info()', 'duration()', '']
    
    # UID
    survey_pilpres = survey_pilpres.append({'type': 'text',
                                            'name': 'UID',
                                            'label': 'Masukkan UID (3 karakter) yang sama dengan UID SMS',
                                            'required': 'yes',
                                            'constraint': f"string-length(.) = 3 and regex(., '^({list_uid})$')",
                                            'constraint message': 'UID tidak terdaftar'
                                            }, ignore_index=True)    
        
    # Regions 
    survey_pilpres = survey_pilpres.append({'type': 'select_one list_kabkota',
                                'name': 'selected_kabkota',
                                'label': 'Pilih Kabupaten/Kota',
                                'required': 'yes',
                                }, ignore_index=True)
    survey_pilpres = survey_pilpres.append({'type': 'select_one list_kecamatan',
                                'name': 'selected_kecamatan',
                                'label': 'Pilih Kecamatan',
                                'required': 'yes',
                                'choice_filter': 'filter_kabkota=${selected_kabkota}',
                                }, ignore_index=True)
    survey_pilpres = survey_pilpres.append({'type': 'select_one list_kelurahan',
                                'name': 'selected_kelurahan',
                                'label': 'Pilih Kelurahan',
                                'required': 'yes',
                                'choice_filter': 'filter_kabkota=${selected_kabkota} and filter_kecamatan=${selected_kecamatan}',
                                }, ignore_index=True)

    # Address
    for (n, l) in zip(['no_tps', 'alamat', 'rt', 'rw'], ['No. TPS', 'Alamat', 'RT', 'RW']):
        survey_pilpres = survey_pilpres.append({'type': 'text',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True) 

    # Upload images
    survey_pilpres = survey_pilpres.append({'type': 'begin_group',
                                  'name': 'upload',
                                  'label': 'Bagian untuk mengunggah/upload foto formulir C1',
                                 }, ignore_index=True) 
    for (n, l) in zip(['pilpres_c1_a4', 'pilpres_c1_plano'], ['Foto Formulir C1-A4 Pemilihan Presiden', 'Foto Formulir C1-Plano Pemilihan Presiden']):
        survey_pilpres = survey_pilpres.append({'type': 'image',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True)
    survey_pilpres = survey_pilpres.append({'type': 'end_group',
                                  'name': 'upload',
                                 }, ignore_index=True) 
    
    # GPS
    survey_pilpres = survey_pilpres.append({'type': 'geopoint',
                                  'name': 'koordinat',
                                  'label': 'Koordinat Lokasi (GPS)',
                                  'required': 'yes',
                                 }, ignore_index=True)

    # Personal Info
    txt = 'Masukkan foto Anda yang sedang berada di TPS (diusahakan di samping tanda nomor TPS)'
    for (t, n, l) in zip(['image', 'text', 'text'], ['selfie', 'nama', 'no_hp'], [txt, 'Nama Anda', 'No. HP Anda']):
        survey_pilpres = survey_pilpres.append({'type': t,
                                    'name': n,
                                    'label': l,
                                    'required': 'yes',
                                    }, ignore_index=True)

    # Save choices to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_pilpres.xlsx', engine='openpyxl') as writer:
        survey_pilpres.to_excel(writer, index=False, sheet_name='survey')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Create a nested dictionary
    nested_target = {}
    for row in target_data.itertuples(index=False):
        kab_kota, kecamatan = row[4:6]
        # Check for None values and initialize nested dictionaries
        if kab_kota is not None:
            nested_target.setdefault(kab_kota, [])
        if kecamatan is not None and kab_kota in nested_target:
            nested_target[kab_kota].append(kecamatan)
        if kelurahan is not None and kab_kota in nested_target and kecamatan in nested_target[kab_kota]:
            nested_target[kab_kota][kecamatan].append(kelurahan)


    # Create a DataFrame for choices
    choices_pilpres = pd.DataFrame(columns=['list_name', 'name', 'label', 'filter_kabkota', 'filter_kecamatan'])

    # Add kabupaten_kota choices
    kab_kota = list(nested_target.keys())
    kab_kota = sorted(kab_kota)
    choices_pilpres = choices_pilpres.append(pd.DataFrame({'list_name': 'list_kabkota', 
                                                    'name': ['_'.join(i.split(' ')) for i in kab_kota],
                                                    'label': kab_kota,
                                                }))

    # Add kecamatan choices
    for kk in kab_kota:
        kecamatan = nested_target[kk]
        kecamatan = sorted(kecamatan)
        choices_pilpres = choices_pilpres.append(pd.DataFrame({'list_name': 'list_kecamatan', 
                                                        'name': ['_'.join(i.split(' ')) for i in kecamatan],
                                                        'label': kecamatan,
                                                        'filter_kabkota': '_'.join(kk.split(' '))
                                                    }))

        # Add kelurahan choices
        for kec in kecamatan:
            kelurahan = nested_target[kk][kec]
            kelurahan = sorted(kelurahan)
            choices_pilpres = choices_pilpres.append(pd.DataFrame({'list_name': 'list_kelurahan', 
                                                            'name': ['_'.join(i.split(' ')) for i in kelurahan],
                                                            'label': kelurahan,
                                                            'filter_kabkota': '_'.join(kk.split(' ')),                                                           
                                                            'filter_kecamatan': '_'.join(kec.split(' '))
                                                        }))

    # Save choices to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_pilpres.xlsx', engine='openpyxl', mode='a') as writer:
        choices_pilpres.to_excel(writer, index=False, sheet_name='choices')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the settings
    settings_df = pd.DataFrame({'form_title': ['QuickCount PilPres PKS Jawa Barat'], 
                                'form_id': ['qc_pilpres_pks_jabar']
                               })
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Save settings to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_pilpres.xlsx', engine='openpyxl', mode='a') as writer:
        settings_df.to_excel(writer, index=False, sheet_name='settings')
            





# ================================================================================================================
# Function to generate SCTO xlsform DPR RI

def create_xlsform_dpr():

    # Load target data from Excel
    target_data = pd.read_excel(f'{local_disk}/target.xlsx')

    # List UID
    list_uid = '|'.join(target_data['UID'].tolist())
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the survey sheet
    survey_dpr = pd.DataFrame(columns=['type', 'name', 'label', 'required', 'choice_filter', 'calculation', 'constraint', 'default', 'appearance', 'relevance'])

    # default fields
    survey_dpr['type'] = ['start', 'end', 'deviceid', 'phonenumber', 'username', 'calculate', 'calculate', 'caseid']
    survey_dpr['name'] = ['starttime', 'endtime', 'deviceid', 'devicephonenum', 'username', 'device_info', 'duration', 'caseid']
    survey_dpr['calculation'] = ['', '', '', '', '', 'device-info()', 'duration()', '']
    
    # UID
    survey_dpr = survey_dpr.append({'type': 'text',
                                    'name': 'UID',
                                    'label': 'Masukkan UID (3 karakter) yang sama dengan UID SMS',
                                    'required': 'yes',
                                    'constraint': f"string-length(.) = 3 and regex(., '^({list_uid})$')",
                                    'constraint message': 'UID tidak terdaftar'
                                    }, ignore_index=True)    

    # Kabupaten/Kota
    survey_dpr = survey_dpr.append({'type': 'select_one KOTA_KAB',
                                    'name': 'KOTA_KAB',
                                    'label': 'Pilih Kabupaten/Kota',
                                    'required': 'yes',
                                    }, ignore_index=True)  

    # Dapil
    calculation = """if((${KOTA_KAB}=181) or (${KOTA_KAB}=185),"Jawa_Barat_1",
if((${KOTA_KAB}=164) or (${KOTA_KAB}=177),"Jawa_Barat_2",
if((${KOTA_KAB}=163) or (${KOTA_KAB}=179),"Jawa_Barat_3",
if((${KOTA_KAB}=162) or (${KOTA_KAB}=180),"Jawa_Barat_4", if((${KOTA_KAB}=161),"Jawa_Barat_5",
if((${KOTA_KAB}=183) or (${KOTA_KAB}=184),"Jawa_Barat_6",
if((${KOTA_KAB}=176) or (${KOTA_KAB}=174) or (${KOTA_KAB}=175),"Jawa_Barat_7",
if((${KOTA_KAB}=169) or (${KOTA_KAB}=172) or (${KOTA_KAB}=182),"Jawa_Barat_8",
if((${KOTA_KAB}=170) or (${KOTA_KAB}=171) or (${KOTA_KAB}=173),"Jawa_Barat_9",
if((${KOTA_KAB}=165) or (${KOTA_KAB}=166) or (${KOTA_KAB}=186),"Jawa_Barat_11",
"Jawa_Barat_10"))))))))))"""
    survey_dpr = survey_dpr.append({'type': 'calculate',
                                    'name': 'DAPIL_SET',
                                    'calculation': calculation
                                    }, ignore_index=True)

    # Caleg DPR RI
    survey_dpr = survey_dpr.append({'type': 'begin_group',
                                  'name': 'CALEG',
                                  'label': 'PEROLEHAN SUARA CALON LEGISLATIF PKS',
                                 }, ignore_index=True)
    data_dpr_1 = [
        ("note", "NOTE_CALEG1", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 1 Jawa Barat).", "", "", "", ""),
        ("integer", "CALEG1_1",	"1. Hj. LEDIA HANIFA A., S.Si., M.Psi.T.", "yes", 0, "", ""),
        ("integer", "CALEG1_2",	"2. Dr. H. HARU SUANDHARU, S.Si., M.Si.", "yes", 0, "", ""),
        ("integer", "CALEG1_3",	"3. Prof. Dr. SANUSI UWES, M.Pd.", "yes", 0, "", ""),
        ("integer", "CALEG1_4",	"4. TEDDY SETIADI, S.Sos.", "yes", 0, "", ""),
        ("integer", "CALEG1_5",	"5. dr. AULIYA RAHMI FADLILAH", "yes", 0, "", ""),
        ("integer", "CALEG1_6",	"6. HENDRA SETIAWAN, S.E., M.M.", "yes", 0, "", ""),
        ("integer", "CALEG1_7",	"7. SRI CHOLIFAH", "yes", 0, "", "")
    ]

    data_dpr_2 = [
        ("note", "NOTE_CALEG2", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 2 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG2_1",	"1. Dr. H. AHMAD HERYAWAN, Lc., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG2_2",	"2. Dipl. Ing. Hj. DIAH NURWITASARI, M.I.Pol.", "yes", 0, "", ""),
        ("integer",	"CALEG2_3",	"3. H. A. MULYANA, S.H., M.Pd., M.H.Kes.", "yes", 0, "", ""),
        ("integer",	"CALEG2_4",	"4. AEP NURDIN, S.Ag., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG2_5",	"5. Dr. H. DASEP KURNIA GUNARUDIN, S.H., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG2_6",	"6. Apt. Hj. CHAIRINI, S.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG2_7",	"7. M. RIYADUL HAQ, S.Ars.", "yes", 0, "", ""),
        ("integer",	"CALEG2_8",	"8. Dra. Hj. IRA DALILAH, M.A.P.", "yes", 0, "", ""),
        ("integer",	"CALEG2_9",	"9. Drs. IANG DARMAWAN", "yes", 0, "", ""),
        ("integer",	"CALEG2_10", "10. Dr. H. IHSAN, M.Si.", "yes", 0, "", "")
    ]

    data_dpr_3 = [
        ("note", "NOTE_CALEG3", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 3 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG3_1",	"1. Dr. H. SUSWONO", "yes", 0, "", ""),
        ("integer",	"CALEG3_2",	"2. H. ECKY AWAL MUCHARAM", "yes", 0, "", ""),
        ("integer",	"CALEG3_3",	"3. SRI PANGESTI BUDI UTAMI, S.Pd., S.E., Ak., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG3_4",	"4. H. SADAR MUSLIHAT, S.H.", "yes", 0, "", ""),
        ("integer",	"CALEG3_5",	"5. IRWAN GUNAWAN, S.P., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG3_6",	"6. ISMA AIDA, Lc., M.I.Kom.", "yes", 0, "", ""),
        ("integer",	"CALEG3_7",	"7. NAUFAL AL-QASSAM S., S.Akun.", "yes", 0, "", ""),
        ("integer",	"CALEG3_8",	"8. CECEP AGAM NUGRAHA, S.H., M.Kn.", "yes", 0, "", ""),
        ("integer",	"CALEG3_9",	"9. FITRI NINGSIH", "yes", 0, "", "")
    ]

    data_dpr_4 = [
        ("note", "NOTE_CALEG4", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 4 Jawa Barat).", "", "", "", ""),
        ("integer", "CALEG4_1",	"1. ASEP SAEPULLOH DANU", "yes", 0, "", ""),
        ("integer", "CALEG4_2",	"2. drh. SLAMET", "yes", 0, "", ""),
        ("integer", "CALEG4_3",	"3. Hj. FITRI HAYATI, S.Ag., M.M.Pd.", "yes", 0, "", ""),
        ("integer", "CALEG4_4",	"4. ABDUL MUIZ", "yes", 0, "", ""),
        ("integer", "CALEG4_5",	"5. drh. PRIYO INDRIANTO", "yes", 0, "", ""),
        ("integer", "CALEG4_6",	"6. SITI WULANDARI RAYANANDA, S.Keb., Bd.", "yes", 0, "", "")
    ]

    data_dpr_5 = [
        ("note", "NOTE_CALEG5", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 5 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG5_1",	"1. drh. H. ACHMAD RU'YAT, M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG5_2",	"2. Dr. H. FAHMY ALAYDROES", "yes", 0, "", ""),
        ("integer",	"CALEG5_3",	"3. Hj. RINI PURA KIRANA", "yes", 0, "", ""),
        ("integer",	"CALEG5_4",	"4. Ir. AJI ASYHARI", "yes", 0, "", ""),
        ("integer",	"CALEG5_5",	"5. DADENG WAHYUDI, S.Pd., M.E.", "yes", 0, "", ""),
        ("integer",	"CALEG5_6",	"6. AI SUTINI", "yes", 0, "", ""),
        ("integer",	"CALEG5_7",	"7. H. AHMAD ZAKKY, S.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG5_8",	"8. EMAN SULAEMAN NASIM, S.Sos., M.H.", "yes", 0, "", ""),
        ("integer",	"CALEG5_9",	"9. ATIKAH SHALIHAT, S.E.I.", "yes", 0, "", "")
    ]

    data_dpr_6 = [
        ("note", "NOTE_CALEG6", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 6 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG6_1", "1. MAHFUDZ ABDURRAHMAN, S.Sos.", "yes", 0, "", ""),
        ("integer",	"CALEG6_2", "2. H. MUHAMMAD KHOLID,S.E., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG6_3", "3. Hj. NUR AZIZAH TAMHID, B.A., M.A.", "yes", 0, "", ""),
        ("integer",	"CALEG6_4", "4. Ir. H. SYAMSU HILAL, M.P.", "yes", 0, "", ""),
        ("integer",	"CALEG6_5", "5. Dr. dr. Hj. TITI MASRIFAHATI, M.K.M., M.A.R.S.", "yes", 0, "", ""),
        ("integer",	"CALEG6_6", "6. SURYA HADIPERMANA, S.E.", "yes", 0, "", "")
    ]

    data_dpr_7 = [
        ("note", "NOTE_CALEG7", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 7 Jawa Barat).", "", "", "", ""),
        ("integer", "CALEG7_1", "1. AHMAD SYAIKHU", "yes", 0, "", ""),
        ("integer", "CALEG7_2", "2. H. PIPIN SOPIAN, S.Sos., IMRI.", "yes", 0, "", ""),
        ("integer", "CALEG7_3", "3. ALLEGRA PUTRI KARTIKA, M.B.A.", "yes", 0, "", ""),
        ("integer", "CALEG7_4", "4. Dr. ABD. JABAR MAJID, M.A.", "yes", 0, "", ""),
        ("integer", "CALEG7_5", "5. Drs. H. M. SALEH MANAF", "yes", 0, "", ""),
        ("integer", "CALEG7_6", "6. RATNA MULYA MADURANI, S.H., M.H.", "yes", 0, "", ""),
        ("integer", "CALEG7_7", "7. H. JALAL ABDUL NASIR, Ak.", "yes", 0, "", ""),
        ("integer", "CALEG7_8", "8. Ir. MUHAMAD SIDARTA", "yes", 0, "", ""),
        ("integer", "CALEG7_9", "9. TIA TRESNAWATI, S.Kom.", "yes", 0, "", ""),
        ("integer", "CALEG7_10", "10. AGUS TOLIB, S.E., S.H., M.H.", "yes", 0, "", "")
    ]

    data_dpr_8 = [
        ("note", "NOTE_CALEG8", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 8 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG8_1",	"1. Dr. Hj. NETTY PRASETIYANI, M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG8_2",	"2. Drs. H. ANWAR YASIN WARYA", "yes", 0, "", ""),
        ("integer",	"CALEG8_3",	"3. H. AGUS MAKMUR SANTOSO, S.Kom., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG8_4",	"4. apt. H. FAISAL AGUS MULYANA", "yes", 0, "", ""),
        ("integer",	"CALEG8_5",	"5. H. DEDE MUHARAM", "yes", 0, "", ""),
        ("integer",	"CALEG8_6",	"6. ETZA IMELDA FITRI, S.H., M.H.", "yes", 0, "", ""),
        ("integer",	"CALEG8_7",	"7. MAHMUD, S.H., M.H.", "yes", 0, "", ""),
        ("integer",	"CALEG8_8",	"8. LIAN WULIANDRI, S.Sos.I.", "yes", 0, "", ""),
        ("integer",	"CALEG8_9",	"9. DWI NUR AFANDI, S.Si.", "yes", 0, "", "")
    ]

    data_dpr_9 = [
        ("note", "NOTE_CALEG9", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 9 Jawa Barat).", "", "", "", ""),
        ("integer", "CALEG9_1", "1. H. NURHASAN ZAIDI", "yes", 0, "", ""),
        ("integer", "CALEG9_2", "2. H. RIDWAN SOLICHIN, S.I.P., M.Si.", "yes", 0, "", ""),
        ("integer", "CALEG9_3", "3. ETIN SRI SETIATIN", "yes", 0, "", ""),
        ("integer", "CALEG9_4", "4. AGUS MASYKUR ROSYADI, S.Si., M.M.", "yes", 0, "", ""),
        ("integer", "CALEG9_5", "5. H. YOSEPH SALMON YUSUF, Lc., M.M.", "yes", 0, "", ""),
        ("integer", "CALEG9_6", "6. HANA SAUSAN", "yes", 0, "", ""),
        ("integer", "CALEG9_7", "7. MU'ALIMAH", "yes", 0, "", ""),
        ("integer", "CALEG9_8", "8. Ir. H. ATENG SUTISNA", "yes", 0, "", "")
    ]

    data_dpr_10 = [
        ("note", "NOTE_CALEG10", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 10 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG10_1", "1. Dr. K.H. SURAHMAN HIDAYAT, Lc., M.A.", "yes", 0, "", ""),
        ("integer",	"CALEG10_2", "2. MIRANTI MAYANGSARI, S.I.Kom.", "yes", 0, "", ""),
        ("integer",	"CALEG10_3", "3. Dr. H. INDRA KUSUMAH, S.Psi., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG10_4", "4. TSABITAH TAQIYYAH", "yes", 0, "", ""),
        ("integer",	"CALEG10_5", "5. HILWA SYAHIDAH BANAN, S.Ag.", "yes", 0, "", ""),
        ("integer",	"CALEG10_6", "6. DEDE SUDRAJAT", "yes", 0, "", ""),
        ("integer",	"CALEG10_7", "7. Ir. H. ENCOK KURYASA, M.M.", "yes", 0, "", "")
    ]

    data_dpr_11 = [
        ("note", "NOTE_CALEG11", "Masukkan jumlah suara setiap calon legislatif DPR-RI PKS (DAPIL 11 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG11_1", "1. H. MOHAMAD SOHIBUL IMAN, M.Sc., Ph.D.", "yes", 0, "", ""),
        ("integer",	"CALEG11_2", "2. K.H. TORIQ HIDAYAT", "yes", 0, "", ""),
        ("integer",	"CALEG11_3", "3. dr. HANI FIRDIANI", "yes", 0, "", ""),
        ("integer",	"CALEG11_4", "4. Dr. ANTON MINARDI, S.I.P., S.H., M.Ag.", "yes", 0, "", ""),
        ("integer",	"CALEG11_5", "5. NIDA NOERVIKA DEWI, S.Pd., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG11_6", "6. AHMAD DZIKRI ALHIKAM", "yes", 0, "", ""),
        ("integer",	"CALEG11_7", "7. EGI HERGIANA, S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG11_8", "8. EDI JUNAEDI, S.Kep., M.H.", "yes", 0, "", ""),
        ("integer",	"CALEG11_9", "9. Dra. LILIS RUHAELIS", "yes", 0, "", ""),
        ("integer",	"CALEG11_10", "10 HUSNI AHMADI, S.E.", "yes", 0, "", "")
    ]

    # Combine all the data
    list_data_dpr = [data_dpr_1, data_dpr_2, data_dpr_3, data_dpr_4, data_dpr_5, data_dpr_6, data_dpr_7, data_dpr_8, data_dpr_9, data_dpr_10, data_dpr_11]
    combined_data = []
    for i, data_dpr in enumerate(list_data_dpr):
        combined_data += [("begin group", f"DAPIL_DPR_{i+1}", f"PEROLEHAN SUARA CALEG DPR-RI PKS DAPIL {i+1} JAWA BARAT", "", "", "field-list", "selected(${DAPIL_SET}, 'Jawa_Barat_" + str(i+1) + "')")] + data_dpr + [("end group", f"DAPIL_DPR_{i+1}", "", "", "", "", "")]
    tmp = pd.DataFrame(combined_data, columns=["type", "name", "label", "required", "default", "appearance", "relevance"])

    survey_dpr = pd.concat([survey_dpr, tmp])

    # Invalid Votes
    survey_dpr = survey_dpr.append({'type': 'integer',
                                  'name': 'TIDAK_SAH',
                                  'label': 'Jumlah Suara Tidak Sah',
                                  'required': 'yes',
                                  'default': 0,
                                 }, ignore_index=True) 

    survey_dpr = survey_dpr.append({'type': 'end_group',
                                  'name': 'CALEG',
                                 }, ignore_index=True) 

    # Upload images
    survey_dpr = survey_dpr.append({'type': 'begin_group',
                                  'name': 'upload',
                                  'label': 'Bagian untuk mengunggah/upload foto formulir C1',
                                 }, ignore_index=True) 
    for (n, l) in zip([f'P_{i}' for i in range(1, 19)], [f'Foto Formulir C1-Plano ({list_parpol[i]})' for i in range(1, 19)]):
        survey_dpr = survey_dpr.append({'type': 'image',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True)
    survey_dpr = survey_dpr.append({'type': 'image',
                                'name': 'P_19',
                                'label': 'Foto Formulir C1-Plano (Suara Tidak Sah)',
                                'required': 'yes',
                                }, ignore_index=True)
    survey_dpr = survey_dpr.append({'type': 'end_group',
                                  'name': 'upload',
                                 }, ignore_index=True) 

    # Save to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_dpr.xlsx', engine='openpyxl') as writer:
        survey_dpr.to_excel(writer, index=False, sheet_name='survey')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    # Choices of Kab/Kota
    
    data = [
        (164, 'Kab. Bandung'),
        (177, 'Kab. Bandung Barat'),
        (176, 'Kab. Bekasi'),
        (161, 'Kab. Bogor'),
        (167, 'Kab. Ciamis'),
        (163, 'Kab. Cianjur'),
        (169, 'Kab. Cirebon'),
        (165, 'Kab. Garut'),
        (172, 'Kab. Indramayu'),
        (175, 'Kab. Karawang'),
        (168, 'Kab. Kuningan'),
        (170, 'Kab. Majalengka'),
        (178, 'Kab. Pangandaran'),
        (174, 'Kab. Purwakarta'),
        (173, 'Kab. Subang'),
        (162, 'Kab. Sukabumi'),
        (171, 'Kab. Sumedang'),
        (166, 'Kab. Tasikmalaya'),
        (181, 'Kota Bandung'),
        (187, 'Kota Banjar'),
        (183, 'Kota Bekasi'),
        (179, 'Kota Bogor'),
        (185, 'Kota Cimahi'),
        (182, 'Kota Cirebon'),
        (184, 'Kota Depok'),
        (180, 'Kota Sukabumi'),
        (186, 'Kota Tasikmalaya')
    ]

    # Create a DataFrame
    choices_dpr = pd.DataFrame(data, columns=['value', 'label'])
    choices_dpr['list_name'] = 'KOTA_KAB'  # Assign the same list name to all choices

    # Save choices to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_dpr.xlsx', engine='openpyxl', mode='a') as writer:
        choices_dpr.to_excel(writer, index=False, sheet_name='choices')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the settings
    settings_df = pd.DataFrame({'form_title': ['QuickCount DPR RI PKS Jawa Barat'], 
                                'form_id': ['qc_dprri_pks_jabar']
                               })
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Save settings to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_dpr.xlsx', engine='openpyxl', mode='a') as writer:
        settings_df.to_excel(writer, index=False, sheet_name='settings')




# ================================================================================================================
# Function to generate SCTO xlsform DPD I

def create_xlsform_dpd():

    # Load target data from Excel
    target_data = pd.read_excel(f'{local_disk}/target.xlsx')

    # List UID
    list_uid = '|'.join(target_data['UID'].tolist())
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the survey sheet
    survey_dpd = pd.DataFrame(columns=['type', 'name', 'label', 'required', 'choice_filter', 'calculation', 'constraint', 'default', 'appearance', 'relevance'])

    # default fields
    survey_dpd['type'] = ['start', 'end', 'deviceid', 'phonenumber', 'username', 'calculate', 'calculate', 'caseid']
    survey_dpd['name'] = ['starttime', 'endtime', 'deviceid', 'devicephonenum', 'username', 'device_info', 'duration', 'caseid']
    survey_dpd['calculation'] = ['', '', '', '', '', 'device-info()', 'duration()', '']
    
    # UID
    survey_dpd = survey_dpd.append({'type': 'text',
                                    'name': 'UID',
                                    'label': 'Masukkan UID (3 karakter) yang sama dengan UID SMS',
                                    'required': 'yes',
                                    'constraint': f"string-length(.) = 3 and regex(., '^({list_uid})$')",
                                    'constraint message': 'UID tidak terdaftar'
                                    }, ignore_index=True)    

    # Caleg DPD I
    survey_dpd = survey_dpd.append({'type': 'begin_group',
                                  'name': 'CALEG',
                                  'label': 'PEROLEHAN SUARA CALON LEGISLATIF DPD I',
                                 }, ignore_index=True)
    data_dpd_1= [
        ("note", "NOTE_CALEG1", "Masukkan jumlah suara setiap calon legislatif DPD-I (Nomor 1-15).", "", "", "", ""),
        ("integer", "CALONDPD_1", "1. AA ADE KADARISMAN, S.Sos., M.T.", "yes", 0, "", ""),
        ("integer", "CALONDPD_2", "2. AANYA RINA CASMAYANTI, S.E.", "yes", 0, "", ""),
        ("integer", "CALONDPD_3", "3. ABAS ABDUL JALIL", "yes", 0, "", ""),
        ("integer", "CALONDPD_4", "4. H. ACENG HM FIKRI, S.Ag.", "yes", 0, "", ""),
        ("integer", "CALONDPD_5", "5. H. ADIL MAKMUR SANTOSA, S.Pd., M.Si.", "yes", 0, "", ""),
        ("integer", "CALONDPD_6", "6. Dr. AEP SAEPUDIN MUHTAR, M.Sos.", "yes", 0, "", ""),
        ("integer", "CALONDPD_7", "7. AGITA NURFIANTI, S.Psi.", "yes", 0, "", ""),
        ("integer", "CALONDPD_8", "8. A IRWAN BOLA", "yes", 0, "", ""),
        ("integer", "CALONDPD_9", "9. AJI SAPTAJI, S.H.I., M.E.Sy.", "yes", 0, "", ""),
        ("integer", "CALONDPD_10", "10. ALFIANSYAH KOMENG", "yes", 0, "", ""),
        ("integer", "CALONDPD_11", "11. K.H. AMANG SYAFRUDIN, M.M.", "yes", 0, "", ""),
        ("integer", "CALONDPD_12", "12. AMBU USDEK KANIAWATI, S.Sos.", "yes", 0, "", ""),
        ("integer", "CALONDPD_13", "13. ANDRI PERKASA KANTAPRAWIRA, S.I.P., M.M", "yes", 0, "", ""),
        ("integer", "CALONDPD_14", "14. ANNIDA ALLIVIA", "yes", 0, "", ""),
        ("integer", "CALONDPD_15", "15. O OGI SOS", "yes", 0, "", "")
    ]

    data_dpd_2= [
        ("note", "NOTE_CALEG2", "Masukkan jumlah suara setiap calon legislatif DPD-I (Nomor 16-30).", "", "", "", ""),
        ("integer",	"CALONDPD_16", "16. ARIF RAHMAN HIDAYAT", "yes", 0, "", ""),
        ("integer",	"CALONDPD_17", "17. A. TAUPIK HIDAYAT", "yes", 0, "", ""),
        ("integer",	"CALONDPD_18", "18. K.H. A WAWAN GHOZALI", "yes", 0, "", ""),
        ("integer",	"CALONDPD_19", "19. BIBEN FIKRIANA, S.Kep., Ners., M.Kep.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_20", "20. BUDIYANTO, S.Pi.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_21", "21. BUDIYONO, S.P.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_22", "22. DEDE AMAR", "yes", 0, "", ""),
        ("integer",	"CALONDPD_23", "23. DEDI RUDIANSYAH, S.T.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_24", "24. DENDA ALAMSYAH, S.T.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_25", "25. DENI RUSYNIANDI, S.Ag.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_26", "26. DIAN RAHADIAN", "yes", 0, "", ""),
        ("integer",	"CALONDPD_27", "27. DJUMONO", "yes", 0, "", ""),
        ("integer",	"CALONDPD_28", "28. EDI KUSDIANA, S.A.P., M.M.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_29", "29. Ir. ELAN HERYANTO", "yes", 0, "", ""),
        ("integer",	"CALONDPD_30", "30. Dra. Ir. Hj. ENI SUMARNI, M.Kes.", "yes", 0, "", "")
    ]

    data_dpd_3= [
        ("note", "NOTE_CALEG3", "Masukkan jumlah suara setiap calon legislatif DPD-I (Nomor 31-45).", "", "", "", ""),
        ("integer",	"CALONDPD_31", "31. ERNAWATY TAMPUBOLON, S.T., M.Th.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_32", "32. Dr. HAIDAN S.Pd.I., S.H., M.Ag.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_33", "33. HENDRIK KURNIAWAN, S.Pd.I", "yes", 0, "", ""),
        ("integer",	"CALONDPD_34", "34. Dr. Hj. IFA FAIZAH ROHMAH, M.Pd.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_35", "35. IMAM SOLAHUDIN, S.T., S.Ag., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_36", "36. IMAM SUGIARTO, S.H.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_37", "37. JAHENOS SARAGIH, S.Th., M.Th., M.M", "yes", 0, "", ""),
        ("integer",	"CALONDPD_38", "38. JAJANG KURNIA, S.Sos., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_39", "39. JIHAN FAHIRA", "yes", 0, "", ""),
        ("integer",	"CALONDPD_40", "40. MUHAMAD DAWAM", "yes", 0, "", ""),
        ("integer",	"CALONDPD_41", "41. MUHAMMAD MURTADLOILLAH", "yes", 0, "", ""),
        ("integer",	"CALONDPD_42", "42. MUHAMMAD YAMIN, M.H.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_43", "43. MULYADI ELHAN ZAKARIA", "yes", 0, "", ""),
        ("integer",	"CALONDPD_44", "44. RIA SUGIAT, S.H.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_45", "45. RIFKI KARTINI", "yes", 0, "", "")
    ]

    data_dpd_4= [
        ("note", "NOTE_CALEG4", "Masukkan jumlah suara setiap calon legislatif DPD-I (Nomor 46-54).", "", "", "", ""),
        ("integer",	"CALONDPD_46", "46. ROBBY MAULANA ZULKARNAEN", "yes", 0, "", ""),
        ("integer",	"CALONDPD_47", "47. RUSDI HIDAYAT", "yes", 0, "", ""),
        ("integer",	"CALONDPD_48", "48. Dr. SITTI HIKMAWATTY, S.ST., M.Pd.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_49", "49. Dr. Drs. SONNY HERSONA GW, M.M", "yes", 0, "", ""),
        ("integer",	"CALONDPD_50", "50. Drs. H. SURATTO SISWODIHARJO", "yes", 0, "", ""),
        ("integer",	"CALONDPD_51", "51. Dr. SUROYO", "yes", 0, "", ""),
        ("integer",	"CALONDPD_52", "52. TEDY GIANTARA, S.T.", "yes", 0, "", ""),
        ("integer",	"CALONDPD_53", "53. WAWAN DEDE AMUNG SUTARYA", "yes", 0, "", ""),
        ("integer",	"CALONDPD_54", "54. YUNITA DIAN SUWANDARI, S.T., M.M., M.T.", "yes", 0, "", "")
    ]

    # Combine all the data
    list_data_dpd = [data_dpd_1, data_dpd_2, data_dpd_3, data_dpd_4]
    combined_data = []
    for i, data_dpd in enumerate(list_data_dpd):
        combined_data += [("begin group", f"DPD_{i+1}", f"PEROLEHAN SUARA CALEG DPD I", "", "", "field-list", "")] + data_dpd + [("end group", f"DPD_{i+1}", "", "", "", "", "")]
    tmp = pd.DataFrame(combined_data, columns=["type", "name", "label", "required", "default", "appearance", "relevance"])

    survey_dpd = pd.concat([survey_dpd, tmp])

    # Invalid Votes
    survey_dpd = survey_dpd.append({'type': 'integer',
                                  'name': 'TIDAK_SAH',
                                  'label': 'Jumlah Suara Tidak Sah',
                                  'required': 'yes',
                                  'default': 0,
                                 }, ignore_index=True) 

    survey_dpd = survey_dpd.append({'type': 'end_group',
                                  'name': 'CALEG',
                                 }, ignore_index=True) 

    # Upload images
    survey_dpd = survey_dpd.append({'type': 'begin_group',
                                  'name': 'upload',
                                  'label': 'Bagian untuk mengunggah/upload foto formulir C1',
                                 }, ignore_index=True) 
    
    pages = ['(Caleg 1-15)', '(Caleg 16-30)', '(Caleg 31-45)', '(Caleg 46-54)', '(Suara Tidak Sah)']
    for (n, l) in zip([f'A4_{i}' for i in range(1, 6)], [f'Foto Formulir C1-A4 Halaman {i+2} {page}' for i, page in enumerate(pages)]):
        survey_dpd = survey_dpd.append({'type': 'image',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True)
    survey_dpd = survey_dpd.append({'type': 'end_group',
                                  'name': 'upload',
                                 }, ignore_index=True) 

    # Save to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_dpd.xlsx', engine='openpyxl') as writer:
        survey_dpd.to_excel(writer, index=False, sheet_name='survey')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the settings
    settings_df = pd.DataFrame({'form_title': ['QuickCount DPD I'], 
                                'form_id': ['qc_dpd_pks_jabar']
                               })
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Save settings to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_dpd.xlsx', engine='openpyxl', mode='a') as writer:
        settings_df.to_excel(writer, index=False, sheet_name='settings')






# ================================================================================================================
# Function to generate SCTO xlsform DPRD Jawa Barat

def create_xlsform_jabar():

    # Load target data from Excel
    target_data = pd.read_excel(f'{local_disk}/target.xlsx')

    # List UID
    list_uid = '|'.join(target_data['UID'].tolist())
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the survey sheet
    survey_jabar = pd.DataFrame(columns=['type', 'name', 'label', 'required', 'choice_filter', 'calculation', 'constraint', 'default', 'appearance', 'relevance'])

    # default fields
    survey_jabar['type'] = ['start', 'end', 'deviceid', 'phonenumber', 'username', 'calculate', 'calculate', 'caseid']
    survey_jabar['name'] = ['starttime', 'endtime', 'deviceid', 'devicephonenum', 'username', 'device_info', 'duration', 'caseid']
    survey_jabar['calculation'] = ['', '', '', '', '', 'device-info()', 'duration()', '']
    
    # UID
    survey_jabar = survey_jabar.append({'type': 'text',
                                    'name': 'UID',
                                    'label': 'Masukkan UID (3 karakter) yang sama dengan UID SMS',
                                    'required': 'yes',
                                    'constraint': f"string-length(.) = 3 and regex(., '^({list_uid})$')",
                                    'constraint message': 'UID tidak terdaftar'
                                    }, ignore_index=True)    

    # Kabupaten/Kota
    survey_jabar = survey_jabar.append({'type': 'select_one KOTA_KAB',
                                    'name': 'KOTA_KAB',
                                    'label': 'Pilih Kabupaten/Kota',
                                    'required': 'yes',
                                    }, ignore_index=True)  

    # Dapil
    calculation = """if((${KOTA_KAB}=181) or (${KOTA_KAB}=185),"Jawa_Barat_1",
if((${KOTA_KAB}=164),"Jawa_Barat_2",
if((${KOTA_KAB}=177),"Jawa_Barat_3",
if((${KOTA_KAB}=163),"Jawa_Barat_4",
if((${KOTA_KAB}=162) or (${KOTA_KAB}=180),"Jawa_Barat_5",
if((${KOTA_KAB}=161),"Jawa_Barat_6",
if((${KOTA_KAB}=179),"Jawa_Barat_7",
if((${KOTA_KAB}=183) or (${KOTA_KAB}=184),"Jawa_Barat_8",
if((${KOTA_KAB}=176),"Jawa_Barat_9",
if((${KOTA_KAB}=174) or (${KOTA_KAB}=175),"Jawa_Barat_10",
if((${KOTA_KAB}=170) or (${KOTA_KAB}=171) or (${KOTA_KAB}=173),"Jawa_Barat_11",
if((${KOTA_KAB}=169) or (${KOTA_KAB}=172) or (${KOTA_KAB}=182),"Jawa_Barat_12",
if((${KOTA_KAB}=165),"Jawa_Barat_14",
if((${KOTA_KAB}=166) or (${KOTA_KAB}=186),"Jawa_Barat_15",
"Jawa_Barat_13"))))))))))))))"""
    survey_jabar = survey_jabar.append({'type': 'calculate',
                                    'name': 'DAPIL_SET',
                                    'calculation': calculation
                                    }, ignore_index=True)

    # Caleg DPR RI
    survey_jabar = survey_jabar.append({'type': 'begin_group',
                                  'name': 'CALEG',
                                  'label': 'PEROLEHAN SUARA CALON LEGISLATIF PKS',
                                 }, ignore_index=True)
    data_dprd_1 = [
        ("note", "NOTE_CALEG1", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 1 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG1_1",	"1. Hj. SITI MUNTAMAH, S.A.P.", "yes", 0, "", ""),
        ("integer",	"CALEG1_2",	"2. H. YOYOK SWITOHANDOYO, S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG1_3",	"3. H. TEDY RUSMAWAN, A.T., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG1_4",	"4. H. ACHMAD ZULKARNAIN", "yes", 0, "", ""),
        ("integer",	"CALEG1_5",	"5. Hj. SALMIAH RAMBE, S.Pd.I., M.Sos.", "yes", 0, "", ""),
        ("integer",	"CALEG1_6",	"6. H. ENDUN HAMDUN, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG1_7",	"7. Dr. RICKY HERMAYANTO, S.E., M.M., M.E.", "yes", 0, "", ""),
        ("integer",	"CALEG1_8",	"8. Dra. Hj. EL EL ELLYA", "yes", 0, "", "")
    ]

    data_dprd_2 = [
        ("note", "NOTE_CALEG2", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 2 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG2_1",	"1. H. JAJANG ROHANA, S.Pd.I.", "yes", 0, "", ""),
        ("integer",	"CALEG2_2",	"2. Hj. SARI SUNDARI, S.Sos., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG2_3",	"3. TEDI SURAHMAN, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG2_4",	"4. Dr. H. NANDANG KOSWARA, M.Pd.", "yes", 0, "", ""),
        ("integer",	"CALEG2_5",	"5. MATTIN SANTIKA SAEPUL RINALDI, S.Pd.", "yes", 0, "", ""),
        ("integer",	"CALEG2_6",	"6. Dra. Hj. LENNY OEMAR, M.Pd.I.", "yes", 0, "", ""),
        ("integer",	"CALEG2_7",	"7. MUHAMMAD YUSUF ABDULLAH, S.Sos.", "yes", 0, "", ""),
        ("integer",	"CALEG2_8",	"8. Dr. H. TONTON TAUFIK RACHMAN, S.T., M.B.A.", "yes", 0, "", ""),
        ("integer",	"CALEG2_9",	"9. Dra. SRI WIYANTI, M.Pd.I.", "yes", 0, "", ""),
        ("integer",	"CALEG2_10", "10. DANA VEGA SUPRIATNA, S.E", "yes", 0, "", "")
    ]

    data_dprd_3 = [
        ("note", "NOTE_CALEG3", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 3 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG3_1",	"1. DIDIK AGUS T., M.Pd", "yes", 0, "", ""),
        ("integer",	"CALEG3_2",	"2. BAGJA SETIAWAN, S.Sy", "yes", 0, "", ""),
        ("integer",	"CALEG3_3",	"3. Hj. SRI DEWI ANGGRAINI", "yes", 0, "", ""),
        ("integer",	"CALEG3_4",	"4. ADE IHDINAYAH, S.H.", "yes", 0, "", "")
    ]

    data_dprd_4 = [
        ("note", "NOTE_CALEG4", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 4 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG4_1",	"1. H. R.K. DADAN SURYA NEGARA, S.P.", "yes", 0, "", ""),
        ("integer",	"CALEG4_2",	"2. H. WILMAN SINGAWINATA", "yes", 0, "", ""),
        ("integer",	"CALEG4_3",	"3. NUR FATMAWATI ANWAR, S.H.", "yes", 0, "", ""),
        ("integer",	"CALEG4_4",	"4. H. JOKO ARDI", "yes", 0, "", ""),
        ("integer",	"CALEG4_5",	"5. Dr. H. AHMAD YANI, S.I.P., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG4_6",	"6. JIJAH KHODIJAH, S.H.", "yes", 0, "", "")
    ]

    data_dprd_5 = [
        ("note", "NOTE_CALEG5", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 5 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG5_1",	"1. Ir. H. YUSUF MAULANA", "yes", 0, "", ""),
        ("integer",	"CALEG5_2",	"2. IZHARUL HAQ", "yes", 0, "", ""),
        ("integer",	"CALEG5_3",	"3. SALSABILA", "yes", 0, "", ""),
        ("integer",	"CALEG5_4",	"4. M. SODIKIN, S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG5_5",	"5. ANJAK PRIATAMA SUKMA", "yes", 0, "", ""),
        ("integer",	"CALEG5_6",	"6. PRIATNA AYU BHESTARI, S.P.", "yes", 0, "", ""),
        ("integer",	"CALEG5_7",	"7. ADE ERNA MAHARANI LESMANA, S.P.", "yes", 0, "", ""),
        ("integer",	"CALEG5_8",	"8. HENDRA, S.Pd.I.", "yes", 0, "", "")
    ]

    data_dprd_6 = [
        ("note", "NOTE_CALEG6", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 6 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG6_1",	"1. H. FIKRI HUDI OKTIARWAN, S.Sos.", "yes", 0, "", ""),
        ("integer",	"CALEG6_2",	"2. DEDI AROZA, S.Ag., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG6_3",	"3. Dra. IIN SUPRIHATIN", "yes", 0, "", ""),
        ("integer",	"CALEG6_4",	"4. MOCHAMAD ICHSAN M., S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG6_5",	"5. NUR LAELA TUROHMAH, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG6_6",	"6. SOPIAN APIP, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG6_7",	"7. ZULFIKAR MAHBUB ROBBANI", "yes", 0, "", ""),
        ("integer",	"CALEG6_8",	"8. ABDURROKHIM, S.Pt.", "yes", 0, "", ""),
        ("integer",	"CALEG6_9",	"9. DEWI RACHMAWATI, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG6_10", "10. MURDOKO, S.H.", "yes", 0, "", ""),
        ("integer",	"CALEG6_11", "11. RETNO UMMY ASTHASARI, S.T.P.", "yes", 0, "", "")
    ]

    data_dprd_7 = [
        ("note", "NOTE_CALEG7", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 7 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG7_1",	"1. H. IWAN SURYAWAN, S.Sos.", "yes", 0, "", ""),
        ("integer",	"CALEG7_2",	"2. Dra. EUIS SUFI JATININGSIH", "yes", 0, "", ""),
        ("integer",	"CALEG7_3",	"3. Dr. H. NAJAMUDIN, S.Pd., M.Pd.I.", "yes", 0, "", "")
    ]

    data_dprd_8 = [
        ("note", "NOTE_CALEG8", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 8 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG8_1",	"1. HERI KOSWARA, S.Ag., M.A.", "yes", 0, "", ""),
        ("integer",	"CALEG8_2",	"2. IIN NUR FATINAH, A.Md.", "yes", 0, "", ""),
        ("integer",	"CALEG8_3",	"3. Ir. ASEP ARWIN KOTSARA, M.Eng.", "yes", 0, "", ""),
        ("integer",	"CALEG8_4",	"4. ELLY FARIDA", "yes", 0, "", ""),
        ("integer",	"CALEG8_5",	"5. KOLONEL (PURN) SUBAGIO A.S., S.Sos., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG8_6",	"6. AFWAN RIYADI WIDIYANTO, S.K.M.", "yes", 0, "", ""),
        ("integer",	"CALEG8_7",	"7. H. QURTIFA WIJAYA, S.Ag.", "yes", 0, "", ""),
        ("integer",	"CALEG8_8",	"8. AHMAD SYIHAN ISMAIL, S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG8_9",	"9. LILIS NURLIA, S.Pd.", "yes", 0, "", ""),
        ("integer",	"CALEG8_10", "10. EKA WIDYANI LATIEF, S.K.M., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG8_11", "11. Dr. YOYO ARIFARDHANI, S.H., M.M., LL.M.", "yes", 0, "", "")
    ]

    data_dprd_9 = [
        ("note", "NOTE_CALEG9", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 9 Jawa Barat).", "", "", "", ""),
        ("integer", "CALEG9_1", "1. Dr. Hj. CUCU SUGIARTI, S.I.P., M.Pd.", "yes", 0, "", ""),
        ("integer", "CALEG9_2", "2. H. FAIZAL HAFAN FARID, S.E., M.Si.", "yes", 0, "", ""),
        ("integer", "CALEG9_3", "3. M. AZMI ROBBANI", "yes", 0, "", ""),
        ("integer", "CALEG9_4", "4. Dr. H. AYUB ROHADI, M.Phil.", "yes", 0, "", ""),
        ("integer", "CALEG9_5", "5. H. MOHAMAD NUH", "yes", 0, "", ""),
        ("integer", "CALEG9_6", "6. Hj. DEWI FITRI LESTARI, S.S.", "yes", 0, "", ""),
        ("integer", "CALEG9_7", "7.  HERLINA", "yes", 0, "", "")
    ]

    data_dprd_10 = [
        ("note", "NOTE_CALEG10", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 10 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG10_1", "1. Ir. H. ABDUL HADI WIJAYA, M.Sc", "yes", 0, "", ""),
        ("integer",	"CALEG10_2", "2. H. BUDIWANTO, S.Si., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG10_3", "3. Hj. SITI HINDUN KOMALA, S.Sos.I.", "yes", 0, "", ""),
        ("integer",	"CALEG10_4", "4. dr. ATA SUBAGJA DINATA", "yes", 0, "", ""),
        ("integer",	"CALEG10_5", "5. Drs. THOHA MAHSUN", "yes", 0, "", ""),
        ("integer",	"CALEG10_6", "6. Hj. Apt. LINA ALIYANI MARDIANA, S.Si., M.Farm.", "yes", 0, "", ""),
        ("integer",	"CALEG10_7", "7. FATHIYATUL MAR'ATUSH SHALIHAH, S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG10_8", "8. YUDI KRISTANTO, M.Pd.", "yes", 0, "", "")
    ]

    data_dprd_11 = [
        ("note", "NOTE_CALEG11", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 11 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG11_1", "1. dr. H. ENCEP SUGIANA", "yes", 0, "", ""),
        ("integer",	"CALEG11_2", "2. MOHAMAD AGUNG ANUGRAH, S.E., Ak.", "yes", 0, "", ""),
        ("integer",	"CALEG11_3", "3. RIRIEN MEIDIANA SUMANTRI", "yes", 0, "", ""),
        ("integer",	"CALEG11_4", "4. H. BAMBANG HERDADI, S.H.", "yes", 0, "", ""),
        ("integer",	"CALEG11_5", "5. H. FAIZAL FIRMANSYAH, S.ST.", "yes", 0, "", ""),
        ("integer",	"CALEG11_6", "6. ELY WALIMAH, S.K.M., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG11_7", "7. HERU PAKU SUJANTO, S.H.", "yes", 0, "", ""),
        ("integer",	"CALEG11_8", "8. H. DEDI RASIDI", "yes", 0, "", ""),
        ("integer",	"CALEG11_9", "9. ELINA, A.Md.", "yes", 0, "", ""),
        ("integer",	"CALEG11_10", "10. H. ASEP KOMARUDIN, S.Ag., M.Ud.", "yes", 0, "", "")
    ]

    data_dprd_12 = [
        ("note", "NOTE_CALEG12", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 12 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG12_1", "1. H. JUNAEDI, S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG12_2", "2. ROBANI HENDRA PERMANA, S.T.", "yes", 0, "", ""),
        ("integer",	"CALEG12_3", "3. MUMUNG MUNAWAROH, S.Sos.I.", "yes", 0, "", ""),
        ("integer",	"CALEG12_4", "4. AHMAD FAWAZ, S.TP.", "yes", 0, "", ""),
        ("integer",	"CALEG12_5", "5. H. MOHAMMAD ABDULLAH, S.Ag.", "yes", 0, "", ""),
        ("integer",	"CALEG12_6", "6. TITI INAYATI, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG12_7", "7. JAMALUDIN NASIR, S.Pd.", "yes", 0, "", ""),
        ("integer",	"CALEG12_8", "8. Ir. H. DIDI MUJAHIRI", "yes", 0, "", ""),
        ("integer",	"CALEG12_9", "9. Hj. SRI UMI MAZIAH", "yes", 0, "", ""),
        ("integer",	"CALEG12_10", "10. WAHYADI, S.T., M.M.", "yes", 0, "", ""),
        ("integer",	"CALEG12_11", "11. IIS ROIJAH, S.Pd.I.", "yes", 0, "", ""),
        ("integer",	"CALEG12_12", "12. CECEP SYARIEF ARIFIEN, S.E., M.Ag.", "yes", 0, "", "")
    ]

    data_dprd_13 = [
        ("note", "NOTE_CALEG13", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 13 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG13_1", "1. RIJALUDDIN, S.Pd.", "yes", 0, "", ""),
        ("integer",	"CALEG13_2", "2. H. DIDI SUKARDI, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG13_3", "3. ETIK WIDIATI", "yes", 0, "", ""),
        ("integer",	"CALEG13_4", "4. H. SUPRIYADI", "yes", 0, "", ""),
        ("integer",	"CALEG13_5", "5. H. ENDANG AHMAD HIDAYAT", "yes", 0, "", ""),
        ("integer",	"CALEG13_6", "6. Hj. YENI ROHLIANI", "yes", 0, "", ""),
        ("integer",	"CALEG13_7", "7. AIP PIANSAH, S.H.", "yes", 0, "", ""),
        ("integer",	"CALEG13_8", "8. RINA RIDIASTUTI, A.Ma.", "yes", 0, "", "")
    ]

    data_dprd_14 = [
        ("note", "NOTE_CALEG14", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 14 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG14_1", "1. H. AHAB SIHABUDIN, S.H.I.", "yes", 0, "", ""),
        ("integer",	"CALEG14_2", "2. KARNOTO, S.Kep., M.Si.", "yes", 0, "", ""),
        ("integer",	"CALEG14_3", "3. Hj. YAYAH ROKAYAH, S.Kom.", "yes", 0, "", ""),
        ("integer",	"CALEG14_4", "4. SITI HASNAH PAULIAH", "yes", 0, "", ""),
        ("integer",	"CALEG14_5", "5. USUP, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG14_6", "6. WILDAN NURFAHMI, S.Pd.I.", "yes", 0, "", "")
    ]

    data_dprd_15 = [
        ("note", "NOTE_CALEG15", "Masukkan jumlah suara setiap calon legislatif DPRD dari PKS (DAPIL 15 Jawa Barat).", "", "", "", ""),
        ("integer",	"CALEG15_1", "1. Drs. K.H. TETEP ABDULATIP", "yes", 0, "", ""),
        ("integer",	"CALEG15_2", "2. DEDE MUHAMAD MUHARAM", "yes", 0, "", ""),
        ("integer",	"CALEG15_3", "3. LANI SURYANI, S.Pd.", "yes", 0, "", ""),
        ("integer",	"CALEG15_4", "4. ANDI NUGRAHA SAEP, S.E.", "yes", 0, "", ""),
        ("integer",	"CALEG15_5", "5. VIKI SYAHRIJAL, S.Hub.Int.", "yes", 0, "", ""),
        ("integer",	"CALEG15_6", "6. DEDE ANALISNUR", "yes", 0, "", ""),
        ("integer",	"CALEG15_7", "7. Dra. Hj. NAFILAH ALIS,M.Ag", "yes", 0, "", "")
    ]

    # Combine all the data
    list_data_dpr = [data_dprd_1, data_dprd_2, data_dprd_3, data_dprd_4, data_dprd_5, data_dprd_6, data_dprd_7, data_dprd_8, data_dprd_9, data_dprd_10, data_dprd_11, data_dprd_12, data_dprd_13, data_dprd_14, data_dprd_15]
    combined_data = []
    for i, data_dpr in enumerate(list_data_dpr):
        combined_data += [("begin group", f"DAPIL_DPRD_{i+1}", f"PEROLEHAN SUARA CALEG DPRD PROVINSI PKS DAPIL {i+1} JAWA BARAT", "", "", "field-list", "selected(${DAPIL_SET}, 'Jawa_Barat_" + str(i+1) + "')")] + data_dpr + [("end group", f"DAPIL_DPRD_{i+1}", "", "", "", "", "")]
    tmp = pd.DataFrame(combined_data, columns=["type", "name", "label", "required", "default", "appearance", "relevance"])

    survey_jabar = pd.concat([survey_jabar, tmp])

    # Invalid Votes
    survey_jabar = survey_jabar.append({'type': 'integer',
                                  'name': 'TIDAK_SAH',
                                  'label': 'Jumlah Suara Tidak Sah',
                                  'required': 'yes',
                                  'default': 0,
                                 }, ignore_index=True) 

    survey_jabar = survey_jabar.append({'type': 'end_group',
                                  'name': 'CALEG',
                                 }, ignore_index=True) 

    # Upload images
    survey_jabar = survey_jabar.append({'type': 'begin_group',
                                  'name': 'upload',
                                  'label': 'Bagian untuk mengunggah/upload foto formulir C1',
                                 }, ignore_index=True) 
    for (n, l) in zip([f'P_{i}' for i in range(1, 19)], [f'Foto Formulir C1-Plano ({list_parpol[i]})' for i in range(1, 19)]):
        survey_jabar = survey_jabar.append({'type': 'image',
                                      'name': n,
                                      'label': l,
                                      'required': 'yes',
                                     }, ignore_index=True)
    survey_jabar = survey_jabar.append({'type': 'image',
                                'name': 'P_19',
                                'label': 'Foto Formulir C1-Plano (Suara Tidak Sah)',
                                'required': 'yes',
                                }, ignore_index=True)
    survey_jabar = survey_jabar.append({'type': 'end_group',
                                  'name': 'upload',
                                 }, ignore_index=True) 

    # Save to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_jabar.xlsx', engine='openpyxl') as writer:
        survey_jabar.to_excel(writer, index=False, sheet_name='survey')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    # Choices of Kab/Kota
    
    data = [
        (164, 'Kab. Bandung'),
        (177, 'Kab. Bandung Barat'),
        (176, 'Kab. Bekasi'),
        (161, 'Kab. Bogor'),
        (167, 'Kab. Ciamis'),
        (163, 'Kab. Cianjur'),
        (169, 'Kab. Cirebon'),
        (165, 'Kab. Garut'),
        (172, 'Kab. Indramayu'),
        (175, 'Kab. Karawang'),
        (168, 'Kab. Kuningan'),
        (170, 'Kab. Majalengka'),
        (178, 'Kab. Pangandaran'),
        (174, 'Kab. Purwakarta'),
        (173, 'Kab. Subang'),
        (162, 'Kab. Sukabumi'),
        (171, 'Kab. Sumedang'),
        (166, 'Kab. Tasikmalaya'),
        (181, 'Kota Bandung'),
        (187, 'Kota Banjar'),
        (183, 'Kota Bekasi'),
        (179, 'Kota Bogor'),
        (185, 'Kota Cimahi'),
        (182, 'Kota Cirebon'),
        (184, 'Kota Depok'),
        (180, 'Kota Sukabumi'),
        (186, 'Kota Tasikmalaya')
    ]

    # Create a DataFrame
    choices_jabar = pd.DataFrame(data, columns=['value', 'label'])
    choices_jabar['list_name'] = 'KOTA_KAB'  # Assign the same list name to all choices

    # Save choices to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_jabar.xlsx', engine='openpyxl', mode='a') as writer:
        choices_jabar.to_excel(writer, index=False, sheet_name='choices')
        
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    
    # Create a DataFrame for the settings
    settings_df = pd.DataFrame({'form_title': ['QuickCount DPRD Provinsi PKS Jawa Barat'], 
                                'form_id': ['qc_dprdprov_pks_jabar']
                               })
    
    # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    # Save settings to an Excel file
    with pd.ExcelWriter(f'{local_disk}/xlsform_jabar.xlsx', engine='openpyxl', mode='a') as writer:
        settings_df.to_excel(writer, index=False, sheet_name='settings')









# ================================================================================================================
# Functions to process SCTO data (Pilpres)

def scto_process_pilpres(data):

    try:

        # UID
        uid = data['UID']

        # SCTO Timestamp
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p")
        std_datetime = std_datetime + timedelta(hours=7)

        # Retrieve data with this UID from Bubble database
        filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid}]
        filter_json = json.dumps(filter_params)
        params = {"constraints": filter_json}
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
        data_bubble = res_bubble.json()
        data_bubble = data_bubble['response']['results'][0]

        # Get existing validator
        if 'Validator Pilpres' in data_bubble:
            validator = data_bubble['Validator Pilpres']
        else:
            validator = None

        # C1-Form attachments
        c1_a4 = data['pilpres_c1_a4']
        c1_plano = data['pilpres_c1_plano']

        # Selfie attachment
        selfie = data['selfie']

        # OCR C1-Form
        try:
            attachment_url = data['pilpres_c1_a4']
            # Build SCTO connection
            scto = SurveyCTOObject(SCTO_SERVER_NAME, SCTO_USER_NAME, SCTO_PASSWORD)
            ai_votes, ai_invalid = read_form(scto, attachment_url)
        except Exception as e:
            print(f'Process: scto_process endpoint\t Keyword: {e}\n')
            ai_votes = [0] * 3
            ai_invalid = 0

        # Check if SMS data exists
        sms = data_bubble['SMS-1']

        # If SMS data exists, check if they are consistent
        if sms:
            status = 'Not Verified'
        else:
            status = 'SCTO Only'

        # Completeness
        if data_bubble['SMS-1'] and data_bubble['SMS-2'] and data_bubble['SMS-3'] and data_bubble['SCTO-2'] and data_bubble['SCTO-3'] and data_bubble['SCTO-4']:
            complete = True
        else:
            complete = False

        # GPS location
        coordinate = np.array(data['koordinat'].split(' ')[1::-1]).astype(float)
        loc = get_location(coordinate)
        
        # Survey Link
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"

        # Update GPS status
        if (data_bubble['Kab/Kota']==loc['Kab/Kota']) and (data_bubble['Kecamatan']==loc['Kecamatan']) and (data_bubble['Kelurahan']==loc['Kelurahan']):
            gps_status = 'Verified'
        else:
            gps_status = 'Not Verified'

        # Payload
        payload = {
            'Active': True,
            'Complete': complete,
            'SCTO-1 TPS': data['no_tps'],
            'SCTO-1 Address': data['alamat'],
            'SCTO-1 RT': data['rt'],
            'SCTO-1 RW': data['rw'],
            'SCTO-1': True,
            'SCTO-1 Enum Name': data['nama'],
            'SCTO-1 Enum Phone': data['no_hp'],
            'SCTO-1 Timestamp': std_datetime,
            'SCTO-1 Kab/Kota': data['selected_kabkota'].replace('_', ' '),
            'SCTO-1 Kecamatan': data['selected_kecamatan'].replace('_', ' '),
            'SCTO-1 Kelurahan': data['selected_kelurahan'].replace('_', ' '),
            'GPS Kab/Kota': loc['Kab/Kota'],
            'GPS Kecamatan': loc['Kecamatan'],
            'GPS Kelurahan': loc['Kelurahan'],
            'GPS Status': gps_status,
            'Status Pilpres': status,
            'Survey Link 1': link,
            'SCTO-1 AI Votes': ai_votes,
            'SCTO-1 AI Invalid': ai_invalid,
            'SCTO-1 C1 A4': c1_a4,
            'SCTO-1 C1 Plano': c1_plano,
            'SCTO-1 Selfie': selfie,
            'Validator Pilpres': validator
        }

        # Load the JSON file into a dictionary
        with open(f'{local_disk}/uid.json', 'r') as json_file:
            uid_dict = json.load(json_file)

        # Forward data to Bubble Votes database
        _id = uid_dict[uid.upper()]
        out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
        print(out)

    except Exception as e:
        with print_lock:
            print(f'Process: scto_process_pilpres\t Keyword: {e}')





# ================================================================================================================
# Functions to process SCTO data (DPR-RI)

def scto_process_dpr(data):

    try:

        # UID
        uid = data['UID']

        # SCTO Timestamp
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p")
        std_datetime = std_datetime + timedelta(hours=7)

        # Retrieve data with this UID from Bubble database
        filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid}]
        filter_json = json.dumps(filter_params)
        params = {"constraints": filter_json}
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
        data_bubble = res_bubble.json()
        data_bubble = data_bubble['response']['results'][0]

        # Dapil
        dapil = data_bubble['Dapil DPR RI']

        # Caleg
        dapil = int(dapil)
        n_caleg = {1: 7, 2: 10, 3: 9, 4: 6, 5: 9, 6: 6, 7: 10, 8: 9, 9: 8, 10: 7, 11: 10}
        vote_caleg = {f'DPR DP{dapil} C{ic}' : int(data[f'CALEG{dapil}_{ic}']) for ic in range(1, n_caleg[dapil]+1)}

        # C1-Form attachments
        c1_parpol = {f'SCTO-2 C1-{i}': data[f'P_{i}'] for i in range(1,20)}

        # Check if SMS data exists
        sms = data_bubble['SMS-2']

        # If SMS data exists, check if they are consistent
        if sms:
            status = 'Not Verified'
        else:
            status = 'SCTO Only'
        
        # Completeness
        if data_bubble['SMS-1'] and data_bubble['SMS-2'] and data_bubble['SMS-3'] and data_bubble['SCTO-1'] and data_bubble['SCTO-3'] and data_bubble['SCTO-4']:
            complete = True
        else:
            complete = False

        # Survey Link
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"

        # Payload
        payload = {
            'Active': True,
            'Complete': complete,
            'SCTO-2': True,
            'SCTO-2 Timestamp': std_datetime,
            'Status DPR RI': status,
            'Survey Link 2': link,
        }
        payload.update(vote_caleg)
        payload.update(c1_parpol)

        # Load the JSON file into a dictionary
        with open(f'{local_disk}/uid.json', 'r') as json_file:
            uid_dict = json.load(json_file)

        # Forward data to Bubble Votes database
        _id = uid_dict[uid.upper()]
        out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
        print(out)

    except Exception as e:
        with print_lock:
            print(f'Process: scto_process_dpr\t Keyword: {e}')





# ================================================================================================================
# Functions to process SCTO data (DPD I)

def scto_process_dpd(data):

    try:

        # UID
        uid = data['UID']

        # SCTO Timestamp
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p")
        std_datetime = std_datetime + timedelta(hours=7)

        # Retrieve data with this UID from Bubble database
        filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid}]
        filter_json = json.dumps(filter_params)
        params = {"constraints": filter_json}
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
        data_bubble = res_bubble.json()
        data_bubble = data_bubble['response']['results'][0]

        # Caleg
        vote_caleg = {f'Vote DPD {ic}' : int(data[f'CALONDPD_{ic}']) for ic in range(1, 55)}
        total_vote_dpd = np.sum([int(data[f'CALONDPD_{ic}']) for ic in range(1, 55)])

        # Invalid Votes
        invalid_dpd = int(data['TIDAK_SAH'])

        # C1-Form attachments
        c1_caleg = {f'SCTO-3 C1-{i}': data[f'A4_{i}'] for i in range(1,6)}
        
        # Completeness
        if data_bubble['SMS-1'] and data_bubble['SMS-2'] and data_bubble['SMS-3'] and data_bubble['SCTO-1'] and data_bubble['SCTO-2'] and data_bubble['SCTO-4']:
            complete = True
        else:
            complete = False

        # Survey Link
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"

        # Payload
        payload = {
            'Active': True,
            'Complete': complete,
            'SCTO-3': True,
            'SCTO-3 Timestamp': std_datetime,
            'Status DPD I': 'Not Verified',
            'Survey Link 3': link,
            'Total Valid DPD': total_vote_dpd,
            'Vote DPD Invalid': invalid_dpd
        }
        payload.update(vote_caleg)
        payload.update(c1_caleg)

        # Load the JSON file into a dictionary
        with open(f'{local_disk}/uid.json', 'r') as json_file:
            uid_dict = json.load(json_file)

        # Forward data to Bubble Votes database
        _id = uid_dict[uid.upper()]
        out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
        print(out)

    except Exception as e:
        with print_lock:
            print(f'Process: scto_process_dpr\t Keyword: {e}')





# ================================================================================================================
# Functions to process SCTO data (DPRD-jawa Barat)

def scto_process_jabar(data):

    try:

        # UID
        uid = data['UID']

        # SCTO Timestamp
        std_datetime = datetime.strptime(data['SubmissionDate'], "%b %d, %Y %I:%M:%S %p")
        std_datetime = std_datetime + timedelta(hours=7)

        # Retrieve data with this UID from Bubble database
        filter_params = [{"key": "UID", "constraint_type": "text contains", "value": uid}]
        filter_json = json.dumps(filter_params)
        params = {"constraints": filter_json}
        res_bubble = requests.get(f'{url_bubble}/Votes', headers=headers, params=params)
        data_bubble = res_bubble.json()
        data_bubble = data_bubble['response']['results'][0]

        # Dapil
        dapil = data_bubble['Dapil DPRD Jawa Barat']

        # Caleg
        dapil = int(dapil)
        n_caleg = {1: 8, 2: 10, 3: 4, 4: 6, 5: 8, 6: 11, 7: 3, 8: 11, 9: 7, 10: 8, 11: 10, 12: 12, 13: 8, 14: 6, 15: 7}
        vote_caleg = {f'Jabar DP{dapil} C{ic}' : int(data[f'CALEG{dapil}_{ic}']) for ic in range(1, n_caleg[dapil]+1)}

        # C1-Form attachments
        c1_parpol = {f'SCTO-4 C1-{i}': data[f'P_{i}'] for i in range(1,20)}

        # Check if SMS data exists
        sms = data_bubble['SMS-3']

        # If SMS data exists, check if they are consistent
        if sms:
            status = 'Not Verified'
        else:
            status = 'SCTO Only'
        
        # Completeness
        if data_bubble['SMS-1'] and data_bubble['SMS-2'] and data_bubble['SMS-3'] and data_bubble['SCTO-1'] and data_bubble['SCTO-2'] and data_bubble['SCTO-3']:
            complete = True
        else:
            complete = False

        # Survey Link
        key = data['KEY'].split('uuid:')[-1]
        link = f"https://{SCTO_SERVER_NAME}.surveycto.com/view/submission.html?uuid=uuid%3A{key}"

        # Payload
        payload = {
            'Active': True,
            'Complete': complete,
            'SCTO-4': True,
            'SCTO-4 Timestamp': std_datetime,
            'Status DPRD Jabar': status,
            'Survey Link 4': link,
        }
        payload.update(vote_caleg)
        payload.update(c1_parpol)

        # Load the JSON file into a dictionary
        with open(f'{local_disk}/uid.json', 'r') as json_file:
            uid_dict = json.load(json_file)

        # Forward data to Bubble Votes database
        _id = uid_dict[uid.upper()]
        out = requests.patch(f'{url_bubble}/votes/{_id}', headers=headers, data=payload)
        print(out)

    except Exception as e:
        with print_lock:
            print(f'Process: scto_process_jabar\t Keyword: {e}')