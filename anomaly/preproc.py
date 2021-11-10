
import os,json
import pandas as pd
import glob2
import tqdm
import regex
import re
import json

from regex_preproc import type_request_dictionnary
from utils import convertToTimestamp , read_json_insert_csv,  process_json
# read json file

# row level functions

def parse_WALLET_SMS_PAYLOAD_SUCCESS_ROW(text_type_request):

    ''' la fonction permet de parser les types de requete "Okra WebHook", "Wallet success", 
    "SMS Success" et SMS Payload en object json.
    Elle prend en paramètre le text contenu dans le type de requete,
    elle retourne un objet de type JSON.'''

    pattern = regex.compile(r'\{(?:[^{}]|(?R))*}')
    resul_patt = pattern.findall(text_type_request)
    res = resul_patt[0].replace("\\", " ")
    s = json.loads(res)

    # dictionnary vide
    out_dict = {} 
    for key, value in s.items():
        # à la clé on passe chaque valeur, strip() enlève les espaces au début et à la fin.
        out_dict[key.strip()] = value 
    # input est un dictionnaire et ça retourne un json sous forme string
    out_dump = json.dumps(out_dict) 

     # convertir le string json en object json.
    out_wallet_success = json.loads(out_dump)
    return out_wallet_success


def parse_ERROR_ROW(error_row):

    ''' la fonction permet de parser les types de requete "ERROR" en object json.
    Elle prend en paramètre le text contenu dans le type de requete,
    elle retourne un objet de type JSON.'''


    # replace double quotes into single quotes
    error_row = error_row.replace('"', "'")

    # regex correspon,ding to text + selfie
    pattern = regex.compile(r"{?[a-z :A-Z 0-9\\,=_`']+selfie")
    resul_patt = pattern.findall(error_row)
    
    # replace \ in result by space
    res = resul_patt[0].replace("\\", " ")
    res = res.replace("'name'", "name").replace("`", "").\
                                        replace("'18'", "18").\
                                        replace("'monthly'", "monthly")
    res = res + '"}'
    res = res.replace("'", '"')

    s = json.loads(res)
    out_error_dict = {} # dictionnary vide
    for key, value in s.items():
        out_error_dict[key.strip()] = value # à la clé on passe chaque valeur, strip() enlève les espaces au début et à la fin.

        out_error_dump = json.dumps(out_error_dict) # input est un dictionnaire et ça retourne un json sous forme string
        out_error_text = json.loads(out_error_dump) # convertir le string json en object json.
        return out_error_text


# dataframe-level functions

def parse_and_concatenate_LEADWAY_SUCCESS_DF(df_raw):
    '''Cette fonction permet de parser et de concatener le texte qui a LEADWAY SUCCESS
    comme type de requete
    elle prend comme paramètre un dataframe et retourne les valeurs suivantes:
    - un texte concatené
    - l'index de la 1ère ligne qu'on va utiliser ensuite pour l'effacer
    - l'index de la dernière ligne qu'on va utiliser ensuite pour l'effacer '''

    first_index = 0
    last_index = 0
    text_leadway_concat = ''
    for index, row in df_raw.iterrows():  # boucler sur les colonnes de type text
        text_row = row['text']  
        if re.search('LEADWAY SUCCESS', text_row):
            text_leadway_concat = text_row
            first_index = index
            first_index +=1
            new_df = df_raw[first_index:]

        for first_index, new_row in new_df.iterrows():
            xxx = new_row['text']      
            if not xxx.startswith('['):          
                first_index += 1
                text_leadway_concat = text_row + xxx       
            elif xxx.startswith('['):
                last_index = first_index-1
                break
    return text_leadway_concat, first_index, last_index


def parse_ERROR_DF(df_):
    """
    la fonction permet de parser les types de requete "ERROR" sur tout le dataframe
    """

    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_Col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    error_code_col = []
    error_number_col = []
    error_sql_message_col = []
    error_sql_state_col = []
    error_index_col = []
    error_sql_col = []
  
    #list_column_none_level_log_error = []
    # columns set to None
    list_column_none = [message_sms_payload_col, totalsent_col, cost_col, status_col, email_col,
                       phone_col, endpoint_Col, date_col, bvn_col, requestSuccessful_col, responseMessage_col,
                        responseCode_col, account_name_col, account_number_col]
    

    # columns which will be populated
    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col,
                    error_code_col, error_number_col, error_sql_message_col, error_sql_state_col, 
                    error_index_col, error_sql_col]

    for index, row in df_.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        if re.search('error', str_text):
            log_level = re.search('error', str_text)
                
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)

            if re.search('LOAN ERROR', str_text):
                type_of_request = re.search('LOAN ERROR', str_text)
                loan_error = parse_ERROR_ROW(str_text) 
                try:
                    error_code_col.append(loan_error.get('code'))
                    print(error_code_col)
                except AttributeError:
                    error_code_col.append(None)
                try:
                    print(loan_error.get('errno'))
                    error_number_col.append(loan_error.get('errno'))
                    print('error number :')
                    print(error_number_col)
                except AttributeError:
                    error_number_col.append(None)
                try:
                    error_sql_message_col.append(loan_error.get('sqlMessage'))
                except AttributeError:
                    error_sql_message_col.append(None)
                try:
                    error_sql_state_col.append(loan_error.get('sqlState'))
                except AttributeError:
                    error_sql_state_col.append(None)
                try:
                    error_index_col.append(loan_error.get('index'))
                except AttributeError:
                    error_index_col.append(None)
                try:
                    error_sql_col.append(loan_error.get('sql'))
                except AttributeError:
                    error_sql_col.append(None)       
                try:
                    type_request_col.append(type_of_request.group(0))
                except AttributeError:
                    type_request_col.append(None)

                for p in range(len(list_column_none)):
                    list_column_none[p].append(None)

    # set columns to their corresponding list values

    df_['Type_Request'] = type_request_col
    df_['Phone_Number'] =phone_col
    df_['Date'] = date_col
    df_['EndPoint'] = endpoint_Col
    df_['Log_Level'] = log_level_col
    df_['Email'] = email_col
    df_['Message SMS Payload'] = message_sms_payload_col
    df_['Total Sent'] = totalsent_col
    df_['Cost'] = cost_col
    df_['Status'] = status_col
    df_['Account Number'] = account_number_col
    df_['Account Name'] = account_name_col
    df_['BVN'] = bvn_col
    df_['Request Successful'] = requestSuccessful_col
    df_['Response Message'] = responseMessage_col
    df_['Response Code'] = responseCode_col
    df_['Error Code'] = error_code_col
    df_['Error Number'] = error_number_col
    df_['Error Sql Message'] = error_sql_message_col
    df_['Error Sql State'] = error_sql_state_col
    df_['Error Index'] = error_index_col
    df_['Error Sql'] = error_sql_col


    return df_


def parse_API_REQUEST_DF(df_api_request):
    """
    la fonction permet de parser les types de requete "API REQUEST" sur tout le dataframe
    """

    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_Col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []

    list_column_none_api_request = []
    list_column_none_api_request = [message_sms_payload_col, totalsent_col, cost_col, status_col,
                                    bvn_col, requestSuccessful_col, responseMessage_col,
                                    responseCode_col, account_name_col, account_number_col]

    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

    for index, row in df_api_request.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)        

            # check if the row contains an email address 
            # pour tous les types request créer un dictionnaire dans lequel mapper
            # key = type de request et value = les regex définis
            # pour chaque condition IF créer une liste de colonnes auxquelles affecter None

            if 'mailto' in str_text:
                if re.search('API REQUEST', str_text):
                    type_of_request = re.search('API REQUEST', str_text)                
                    phone_or_email_api_req = re.search(type_request_dictionnary['API REQUEST'][0], str_text)                              
                    endpoint = re.search(type_request_dictionnary['API REQUEST'][1], str_text)
                    pattern = type_request_dictionnary['API REQUEST'][2]
                    datepattern = re.compile("(?:%s)"%(pattern))
                    datematcher = datepattern.search(str_text)  # extract date

                    for i in range(len(list_column_none_api_request)):
                        list_column_none_api_request[i].append(None)
                                    
                    try:
                        type_request_col.append(type_of_request.group(0)) # add type request inside type request column
                    except AttributeError:
                        type_request_col.append(None)               
                    try:
                        email_col.append(phone_or_email_api_req.group(0)) # add email inside email column
                        phone_col.append(None)  # in this case there is no phone number
                    except AttributeError:
                        email_col.append(None)
                    try:
                        endpoint_Col.append(endpoint.group(0)) # add endpoint inside endpoint column
                    except AttributeError:
                        endpoint_Col.append(None)
                    try:
                        date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                    except AttributeError:
                        date_col.append(None)
                    

            elif 'mailto' not in str_text:
                if re.search('API REQUEST', str_text):
                    type_of_request = re.search('API REQUEST', str_text)                            
                    # extract a phone number for API REQUEST
                    phone_or_email_api_req = re.search(type_request_dictionnary['API REQUEST'][3], str_text)                              
                    endpoint = re.search(type_request_dictionnary['API REQUEST'][1], str_text)
                    pattern = type_request_dictionnary['API REQUEST'][2]
                    datepattern = re.compile("(?:%s)"%(pattern))
                    datematcher = datepattern.search(str_text)  # extract date

                    for i in range(len(list_column_none_api_request)):
                        list_column_none_api_request[i].append(None)

                    try:
                        phone_col.append(phone_or_email_api_req.group(0)) # add phone number inside phone number column
                        email_col.append(None) # in this case there is no email address
                    except AttributeError:
                       phone_col.append(None)
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        endpoint_Col.append(endpoint.group(0)) # add endpoint inside endpoint column
                    except AttributeError:
                        endpoint_Col.append(None)
                    try:
                        date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                    except AttributeError:
                        date_col.append(None)

    df_api_request['Type_Request'] = type_request_col
    df_api_request['Phone_Number'] =phone_col
    df_api_request['Date'] = date_col
    df_api_request['EndPoint'] = endpoint_Col
    df_api_request['Log_Level'] = log_level_col
    df_api_request['Email'] = email_col
    df_api_request['Message SMS Payload'] = message_sms_payload_col
    df_api_request['Total Sent'] = totalsent_col
    df_api_request['Cost'] = cost_col
    df_api_request['Status'] = status_col
    df_api_request['Account Number'] = account_number_col
    df_api_request['Account Name'] = account_name_col
    df_api_request['BVN'] = bvn_col
    df_api_request['Request Successful'] = requestSuccessful_col
    df_api_request['Response Message'] = responseMessage_col
    df_api_request['Response Code'] = responseCode_col

    return df_api_request


def parse_CLIENT_MOBILE_LOGIN_DF(df_client_mob):
    """
    la fonction permet de parser les types de requete "CLIENT MOBILE" sur tout le dataframe
    """
    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_Col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []

    list_column_none_client_mobile = []
    list_column_none_client_mobile = [message_sms_payload_col, totalsent_col, cost_col, status_col,
                                    account_number_col, bvn_col, requestSuccessful_col, responseMessage_col,
                                    responseCode_col, account_name_col, endpoint_Col]

    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

    for index, row in df_client_mob.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)        
            # check if the row contains an email address 
            # pour tous les types request créer un dictionnaire dans lequel mapper
            # key = type de request et value = les regex définis
            # pour chaque condition IF créer une liste de colonnes auxquelles affecter None
            if 'mailto' in str_text:
                if re.search('CLIENT MOBILE LOGIN', str_text):   # CLIENT MOBILE LOGIN with email address
                        type_of_request = re.search('CLIENT MOBILE LOGIN', str_text)
                        # extract address email for CLIENT MOBILE LOGIN
                        phone_or_email_client_mobile = re.search(type_request_dictionnary['CLIENT MOBILE LOGIN'][0], str_text)                               
                        pattern = type_request_dictionnary['CLIENT MOBILE LOGIN'][1]
                        datepattern = re.compile("(?:%s)"%(pattern))
                        datematcher = datepattern.search(str_text)  # extract date for CLIENT MOBILE LOGIN type request
                        
                        for j in range(len(list_column_none_client_mobile)):
                            list_column_none_client_mobile[j].append(None)

                        try:
                            type_request_col.append(type_of_request.group(0)) # add type request inside type request column
                        except AttributeError:
                            type_request_col.append(None) 
                        try:
                            email_col.append(phone_or_email_client_mobile.group(0)) # add email inside email column
                            phone_col.append(None)  # in this case there is no phone number
                        except AttributeError:
                            email_col.append(None)
                        try:
                            date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                        except AttributeError:
                            date_col.append(None)

            elif 'mailto' not in str_text:
                if re.search('CLIENT MOBILE LOGIN', str_text): # when type request is CLIENT MOBILE LOGIN, there is no EndPoint
                    type_of_request = re.search('CLIENT MOBILE LOGIN', str_text)
                    # extract a phone number for CLIENT MOBILE LOGIN
                    phone_or_email_client_mobile = re.search(type_request_dictionnary['CLIENT MOBILE LOGIN'][2], str_text)                  
                    pattern = type_request_dictionnary['CLIENT MOBILE LOGIN'][1]
                    datepattern = re.compile("(?:%s)"%(pattern))
                    datematcher = datepattern.search(str_text)  # extract date

                    for j in range(len(list_column_none_client_mobile)):
                        list_column_none_client_mobile[j].append(None)

                    try:
                        phone_col.append(phone_or_email_client_mobile.group(0))
                        email_col.append(None)
                    except AttributeError:
                        phone_col.append(None)
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        date_col.append(convertToTimestamp(datematcher.group(0))) # convert date to timestamp and add it inside date column
                    except AttributeError:
                        date_col.append(None) 

    df_client_mob['Type_Request'] = type_request_col
    df_client_mob['Phone_Number'] =phone_col
    df_client_mob['Date'] = date_col
    df_client_mob['EndPoint'] = endpoint_Col
    df_client_mob['Log_Level'] = log_level_col
    df_client_mob['Email'] = email_col
    df_client_mob['Message SMS Payload'] = message_sms_payload_col
    df_client_mob['Total Sent'] = totalsent_col
    df_client_mob['Cost'] = cost_col
    df_client_mob['Status'] = status_col
    df_client_mob['Account Number'] = account_number_col
    df_client_mob['Account Name'] = account_name_col
    df_client_mob['BVN'] = bvn_col
    df_client_mob['Request Successful'] = requestSuccessful_col
    df_client_mob['Response Message'] = responseMessage_col
    df_client_mob['Response Code'] = responseCode_col

    return df_client_mob


def parse_SMS_PAYLOAD_DF(df_sms_payload):
    """
    la fonction permet de parser les types de requete "SMS PAYLOAD" sur tout le dataframe
    """

    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_Col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []

    list_column_none_sms_payload = []
    list_column_none_sms_payload = [totalsent_col, cost_col, status_col,
                                    account_number_col, bvn_col, requestSuccessful_col, responseMessage_col,
                                    responseCode_col, account_name_col, email_col, endpoint_Col, date_col]

    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

    for index, row in df_sms_payload.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('SMS PAYLOAD', str_text):
                    type_of_request = re.search('SMS PAYLOAD', str_text)            
                    sms_payload = parse_WALLET_SMS_PAYLOAD_SUCCESS_ROW(str_text)               
                    for l in range(len(list_column_none_sms_payload)):
                        list_column_none_sms_payload[l].append(None)             
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                       phone_col.append(sms_payload.get('phone'))
                    except AttributeError:
                       phone_col.append(None)
                    try:
                        message_sms_payload_col.append(sms_payload.get('message'))
                    except AttributeError:
                        message_sms_payload_col.append(None)
                            
            elif re.search('OKRA PAYLOAD', str_text): # Nothing
                type_of_request = re.search('OKRA PAYLOAD', str_text)
            elif re.search('OKRA SUCCESS', str_text):   # Nothing
                type_of_request = re.search('OKRA SUCCESS', str_text)
            elif re.search('VTPASS SUCCESS', str_text):   # Nothing
                type_of_request = re.search('VTPASS SUCCESS', str_text)  

    df_sms_payload['Type_Request'] = type_request_col
    df_sms_payload['Phone_Number'] =phone_col
    df_sms_payload['Date'] = date_col
    df_sms_payload['EndPoint'] = endpoint_Col
    df_sms_payload['Log_Level'] = log_level_col
    df_sms_payload['Email'] = email_col
    df_sms_payload['Message SMS Payload'] = message_sms_payload_col
    df_sms_payload['Total Sent'] = totalsent_col
    df_sms_payload['Cost'] = cost_col
    df_sms_payload['Status'] = status_col
    df_sms_payload['Account Number'] = account_number_col
    df_sms_payload['Account Name'] = account_name_col
    df_sms_payload['BVN'] = bvn_col
    df_sms_payload['Request Successful'] = requestSuccessful_col
    df_sms_payload['Response Message'] = responseMessage_col
    df_sms_payload['Response Code'] = responseCode_col

    return df_sms_payload


def parse_SMS_SUCCESS_DF(df_sms_success):
    """
    la fonction permet de parser les types de requete "SMS SUCCESS" sur tout le dataframe
    """

    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_Col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []

    list_column_none_sms_success = []
    list_column_none_sms_success = [message_sms_payload_col, account_number_col, bvn_col, requestSuccessful_col, 
                                    responseMessage_col, responseCode_col, account_name_col, email_col, 
                                   phone_col, endpoint_Col, date_col]

    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

    for index, row in df_sms_success.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('SMS SUCCESS', str_text): 
                    type_of_request = re.search('SMS SUCCESS', str_text)
                    sms_success = parse_WALLET_SMS_PAYLOAD_SUCCESS_ROW(str_text)                
                    for m in range(len(list_column_none_sms_success)):
                        list_column_none_sms_success[m].append(None) 
                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:                 
                        totalsent_col.append(sms_success.get('response').get('totalsent '))
                    except AttributeError:
                        totalsent_col.append(None)
                    try:                 
                        cost_col.append(sms_success.get('response').get('cost '))
                    except AttributeError:
                        cost_col.append(None)
                    try:                 
                        status_col.append(sms_success.get('response').get('status '))
                    except AttributeError:
                        status_col.append(None)
                            
            elif re.search('OKRA PAYLOAD', str_text): # Nothing
                type_of_request = re.search('OKRA PAYLOAD', str_text)
            elif re.search('OKRA SUCCESS', str_text):   # Nothing
                type_of_request = re.search('OKRA SUCCESS', str_text)
            elif re.search('VTPASS SUCCESS', str_text):   # Nothing
                type_of_request = re.search('VTPASS SUCCESS', str_text)    

    df_sms_success['Type_Request'] = type_request_col
    df_sms_success['Phone_Number'] =phone_col
    df_sms_success['Date'] = date_col
    df_sms_success['EndPoint'] = endpoint_Col
    df_sms_success['Log_Level'] = log_level_col
    df_sms_success['Email'] = email_col
    df_sms_success['Message SMS Payload'] = message_sms_payload_col
    df_sms_success['Total Sent'] = totalsent_col
    df_sms_success['Cost'] = cost_col
    df_sms_success['Status'] = status_col
    df_sms_success['Account Number'] = account_number_col
    df_sms_success['Account Name'] = account_name_col
    df_sms_success['BVN'] = bvn_col
    df_sms_success['Request Successful'] = requestSuccessful_col
    df_sms_success['Response Message'] = responseMessage_col
    df_sms_success['Response Code'] = responseCode_col

    return df_sms_success


def parse_WALLET_SUCCESS_DF(df_wallet_success):
    """
    la fonction permet de parser les types de requete "WALLET SUCCESS" sur tout le dataframe
    """
    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []

    list_column_none_wallet_success = []
    list_column_none_wallet_success = [totalsent_col, 
                                        message_sms_payload_col, 
                                        cost_col, 
                                        status_col, 
                                        email_col, 
                                        phone_col, 
                                        endpoint_col, 
                                        date_col]

    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

    for index, row in df_wallet_success.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('WALLET SUCCESS', str_text):
                    wallet_success = parse_WALLET_SMS_PAYLOAD_SUCCESS_ROW(str_text)
                    type_of_request = re.search('WALLET SUCCESS', str_text)

                    try:
                        type_request_col.append(type_of_request.group(0))
                    except AttributeError:
                        type_request_col.append(None)
                    try:
                        account_number_col.append(wallet_success.get('account_number'))
                    except AttributeError:
                        account_number_col.append(None)
                    try:
                        account_name_col.append(wallet_success.get('account_name'))
                    except AttributeError:
                        account_name_col.append(None)
                    try:
                        bvn_col.append(wallet_success.get('bvn'))
                    except AttributeError:
                        bvn_col.append(None)
                    try:
                        requestSuccessful_col.append(wallet_success.get('requestSuccessful'))
                    except AttributeError:
                        requestSuccessful_col.append(None)
                    try:
                        responseMessage_col.append((wallet_success.get('responseMessage')))
                    except AttributeError:
                        responseMessage_col.append(None)
                    try:
                        responseCode_col.append((wallet_success.get('responseCode')))
                    except AttributeError:
                        responseCode_col.append(None)

                    for n in range(len(list_column_none_wallet_success)):
                        list_column_none_wallet_success[n].append(None) 
              
            elif re.search('OKRA PAYLOAD', str_text): # Nothing
                type_of_request = re.search('OKRA PAYLOAD', str_text)

            elif re.search('OKRA SUCCESS', str_text):   # Nothing
                type_of_request = re.search('OKRA SUCCESS', str_text)

            elif re.search('VTPASS SUCCESS', str_text):   # Nothing
                type_of_request = re.search('VTPASS SUCCESS', str_text)

    
    #
    
    df_wallet_success['Type_Request'] = type_request_col
    df_wallet_success['Phone_Number'] = phone_col
    df_wallet_success['Date'] = date_col
    df_wallet_success['EndPoint'] = endpoint_col
    df_wallet_success['Log_Level'] = log_level_col
    df_wallet_success['Email'] = email_col
    df_wallet_success['Message SMS Payload'] = message_sms_payload_col
    df_wallet_success['Total Sent'] = totalsent_col
    df_wallet_success['Cost'] = cost_col
    df_wallet_success['Status'] = status_col
    df_wallet_success['Account Number'] = account_number_col
    df_wallet_success['Account Name'] = account_name_col
    df_wallet_success['BVN'] = bvn_col
    df_wallet_success['Request Successful'] = requestSuccessful_col
    df_wallet_success['Response Message'] = responseMessage_col
    df_wallet_success['Response Code'] = responseCode_col
 
    return df_wallet_success


def parse_OKRA_WEBHOOK_DF(df_okra):
    """
    la fonction permet de parser les types de requete "OKRA WEBHOOK" sur tout le dataframe
    """
    log_level_col = []
    api_request_col = []
    type_request_col = []
    phone_col = []
    date_col = []
    endpoint_Col = []
    email_col = []
    message_sms_payload_col = []
    totalsent_col = []
    cost_col = []
    status_col = []
    account_number_col = []
    account_name_col = []
    bvn_col = []
    requestSuccessful_col = []
    responseMessage_col = []
    responseCode_col = []
    accountId_col = []
    authorization_v_col = []
    authorization_id_col = []
    authorization_customer_col = []
    authorization_account_col = []
    authorization_account_id_col = []
    authorization_account_manual_col = []
    authorization_account_name_col = []
    authorization_account_nuban_col = []
    authorization_account_bank_col = []
    authorization_account_created_at_col = []
    authorization_account_last_updated_col = []
    authorization_account_balance_col = []
    authorization_account_customer_col = []
    authorization_account_type_col = []
    authorization_account_currency_col = []
    authorization_accounts_col = []
    authorization_amount_col = []
    authorization_bank_col = []
    authorization_created_at_col = []
    authorization_currency_col = []
    authorization_customerDetails_col = []
    authorization_disconnect_col = []
    authorization_disconnected_at_col = []
    authorization_duration_col = []
    authorization_env_col = []
    authorization_garnish_col = []
    authorization_initialAmount_col = []
    authorization_initiated_col = []
    authorization_last_updated_col = []
    authorization_link_col = []
    authorization_next_payment_col = []
    authorization_owner_col = []
    authorization_payLink_col = []
    authorization_type_col = []
    authorization_used_col = []
    authorizationId_col = []
    bankId_col = []
    bankName_col = []
    bankSlug_col = []
    bankType_col = []
    callbackURL_col = []
    callback_code_col = []
    callback_type_col = []
    callback_url_col = []
    code_col = []
    country_col = []
    current_project_col = []
    customerEmail_col = []
    customerId_col = []
    ended_at_col = []
    env_col = []
    extras_col = []
    identityType_col = []
    login_type_col = []
    message_col = []
    meta_col = []
    method_col = []
    options_col = []
    owner_col = []
    record_col = []
    recordId_col = []
    started_at_col = []
    status_webhook_col = []
    token_col = []
    type_col = []

    list_column_none_okra_webhook = []
    list_column_none_okra_webhook = [api_request_col, account_number_col, account_name_col, totalsent_col, 
                                    message_sms_payload_col, cost_col, status_col, responseCode_col,
                                    bvn_col, requestSuccessful_col, responseMessage_col, email_col,phone_col, 
                                    endpoint_Col, date_col]

    list_all_colum = []
    list_all_colum = [type_request_col,phone_col, date_col, endpoint_Col, log_level_col, email_col, 
                    message_sms_payload_col, totalsent_col, cost_col, status_col, account_number_col,
                    account_name_col, bvn_col, requestSuccessful_col, responseMessage_col, responseCode_col]

    for index, row in df_okra.iterrows():
        str_text = row['text']

        if not str_text.startswith('['):
            for i in range(len(list_all_colum)):
                list_all_colum[i].append(None)

        # check if the row contains "info" string
        if re.search('info', str_text):
            log_level = re.search('info', str_text)
            try:
                log_level_col.append(log_level.group(0))
            except AttributeError:
                log_level_col.append(None)             
            if 'mailto' not in str_text:
                if re.search('OKRA WEBHOOK', str_text):
                        type_of_request = re.search('OKRA WEBHOOK', str_text)
                        okra_webhook = parse_WALLET_SMS_PAYLOAD_SUCCESS_ROW(str_text)                  
                        accountId_col.append(okra_webhook.get('accountId'))
                        authorization_v_col.append(okra_webhook.get('authorization').get('__v '))
                        authorization_id_col.append(okra_webhook.get('authorization').get('_id '))
                        authorization_customer_col.append(okra_webhook.get('authorization').get('customer '))
                        authorization_account_col.append(okra_webhook.get('authorization').get('account '))
                        authorization_account_id_col.append(okra_webhook.get('authorization').get('account ')[0].get('_id '))
                        authorization_account_manual_col.append(okra_webhook.get('authorization').get('account ')[0].get('manual '))
                        authorization_account_name_col.append(okra_webhook.get('authorization').get('account ')[0].get('name '))
                        authorization_account_nuban_col.append(okra_webhook.get('authorization').get('account ')[0].get('nuban '))
                        authorization_account_bank_col.append(okra_webhook.get('authorization').get('account ')[0].get('bank '))
                        authorization_account_created_at_col.append(okra_webhook.get('authorization').get('account ')[0].get('created_at '))
                        authorization_account_last_updated_col.append(okra_webhook.get('authorization').get('account ')[0].get('last_updated '))
                        authorization_account_balance_col.append(okra_webhook.get('authorization').get('account ')[0].get('balance '))
                        authorization_account_customer_col.append(okra_webhook.get('authorization').get('account ')[0].get('customer '))
                        authorization_account_type_col.append(okra_webhook.get('authorization').get('account ')[0].get('type '))
                        authorization_account_currency_col.append(okra_webhook.get('authorization').get('account ')[0].get('currency '))
                        authorization_accounts_col.append(okra_webhook.get('authorization').get('accounts '))
                        authorization_amount_col.append(okra_webhook.get('authorization').get('amount '))
                        authorization_bank_col.append(okra_webhook.get('authorization').get('bank '))
                        authorization_created_at_col.append(okra_webhook.get('authorization').get('created_at '))
                        authorization_currency_col.append(okra_webhook.get('authorization').get('currency '))
                        authorization_customerDetails_col.append(okra_webhook.get('authorization').get('customerDetails '))
                        authorization_disconnect_col.append(okra_webhook.get('authorization').get('disconnect '))
                        authorization_disconnected_at_col.append(okra_webhook.get('authorization').get('disconnected_at '))
                        authorization_duration_col.append(okra_webhook.get('authorization').get('duration '))
                        authorization_env_col.append(okra_webhook.get('authorization').get('env '))
                        authorization_garnish_col.append(okra_webhook.get('authorization').get('garnish '))
                        authorization_initialAmount_col.append(okra_webhook.get('authorization').get('initialAmount '))
                        authorization_initiated_col.append(okra_webhook.get('authorization').get('initiated '))
                        authorization_last_updated_col.append(okra_webhook.get('authorization').get('last_updated '))
                        authorization_link_col.append(okra_webhook.get('authorization').get('link '))
                        authorization_next_payment_col.append(okra_webhook.get('authorization').get('next_payment '))
                        authorization_owner_col.append(okra_webhook.get('authorization').get('owner '))
                        authorization_payLink_col.append(okra_webhook.get('authorization').get('payLink '))
                        authorization_type_col.append(okra_webhook.get('authorization').get('type '))
                        authorization_used_col.append(okra_webhook.get('authorization').get('used '))
                        authorizationId_col.append(okra_webhook.get('authorizationId'))
                        bankId_col.append(okra_webhook.get('bankId'))
                        bankName_col.append(okra_webhook.get('bankName'))
                        bankSlug_col.append(okra_webhook.get('bankSlug'))
                        bankType_col.append(okra_webhook.get('bankType'))
                        callbackURL_col.append(okra_webhook.get('callbackURL'))
                        callback_code_col.append(okra_webhook.get('callback_code'))
                        callback_type_col.append(okra_webhook.get('callback_type'))
                        callback_url_col.append(okra_webhook.get('callback_url'))
                        code_col.append(okra_webhook.get('code'))
                        country_col.append(okra_webhook.get('country'))
                        current_project_col.append(okra_webhook.get('current_project'))
                        customerEmail_col.append(okra_webhook.get('customerEmail'))
                        customerId_col.append(okra_webhook.get('customerId'))
                        ended_at_col.append(okra_webhook.get('ended_at'))
                        env_col.append(okra_webhook.get('env'))
                        extras_col.append(okra_webhook.get('extras'))
                        identityType_col.append(okra_webhook.get('identityType'))
                        login_type_col.append(okra_webhook.get('login_type'))
                        message_col.append(okra_webhook.get('message'))
                        meta_col.append(okra_webhook.get('meta'))
                        method_col.append(okra_webhook.get('method'))
                        options_col.append(okra_webhook.get('options'))
                        owner_col.append(okra_webhook.get('owner'))
                        record_col.append(okra_webhook.get('record'))
                        recordId_col.append(okra_webhook.get('recordId'))
                        started_at_col.append(okra_webhook.get('started_at'))
                        status_webhook_col.append(okra_webhook.get('status'))
                        token_col.append(okra_webhook.get('token'))
                        type_col.append(okra_webhook.get('type'))

                        try:
                            type_request_col.append(type_of_request.group(0))
                        except AttributeError:
                            type_request_col.append(None)
                        
                        for n in range(len(list_column_none_okra_webhook)):
                            list_column_none_okra_webhook[n].append(None)

                            
            elif re.search('OKRA PAYLOAD', str_text): # Nothing
                type_of_request = re.search('OKRA PAYLOAD', str_text)
            elif re.search('OKRA SUCCESS', str_text):   # Nothing
                type_of_request = re.search('OKRA SUCCESS', str_text)
            elif re.search('VTPASS SUCCESS', str_text):   # Nothing
                type_of_request = re.search('VTPASS SUCCESS', str_text)  

        
    df_okra['Type_Request'] = type_request_col
    df_okra['Phone_Number'] =phone_col
    df_okra['Date'] = date_col
    df_okra['EndPoint'] = endpoint_Col
    df_okra['Log_Level'] = log_level_col
    df_okra['Email'] = email_col
    df_okra['Message SMS Payload'] = message_sms_payload_col
    df_okra['Total Sent'] = totalsent_col
    df_okra['Cost'] = cost_col
    df_okra['Status'] = status_col
    df_okra['Account Number'] = account_number_col
    df_okra['Account Name'] = account_name_col
    df_okra['BVN'] = bvn_col
    df_okra['Request Successful'] = requestSuccessful_col
    df_okra['Response Message'] = responseMessage_col
    df_okra['Response Code'] = responseCode_col
    df_okra['Account Id'] = accountId_col
    df_okra['Authorization_V'] = authorization_v_col
    df_okra['Authorization_Id'] = authorization_id_col
    df_okra['Authorization_Customer'] = authorization_customer_col
    df_okra['Authorization_Owner'] = authorization_owner_col
    df_okra['Authorization_Account'] = authorization_account_col
    df_okra['Authorization_account_Id'] = authorization_account_id_col
    df_okra['Authorization_account_manual'] = authorization_account_manual_col
    df_okra['Authorization_account_name'] = authorization_account_name_col
    df_okra['Authorization_account_nuban'] = authorization_account_nuban_col
    df_okra['Authorization_account_bank'] = authorization_account_bank_col
    df_okra['Authorization_account_created_at'] = authorization_account_created_at_col
    df_okra['Authorization_account_last_updated'] = authorization_account_last_updated_col
    df_okra['Authorization_account_balance'] = authorization_account_balance_col
    df_okra['Authorization_account_customer'] = authorization_account_customer_col
    df_okra['Authorization_account_type'] = authorization_account_type_col
    df_okra['Authorization_account_currency'] = authorization_account_currency_col
    df_okra['Authorization_accounts'] = authorization_accounts_col
    df_okra['Authorization_amount'] = authorization_amount_col
    df_okra['Authorization_bank'] = authorization_bank_col
    df_okra['Authorization_created_at'] = authorization_created_at_col
    df_okra['Authorization_currency'] = authorization_currency_col 
    df_okra['Authorization_customerDetails'] = authorization_customerDetails_col
    df_okra['Authorization_disconnect'] = authorization_disconnect_col
    df_okra['Authorization_disconnected_at'] = authorization_disconnected_at_col
    df_okra['Authorization_duration'] = authorization_duration_col
    df_okra['Authorization_env'] = authorization_env_col
    df_okra['Authorization_garnish'] = authorization_garnish_col
    df_okra['Authorization_initialAmount'] = authorization_initialAmount_col
    df_okra['Authorization_initiated'] = authorization_initiated_col
    df_okra['Authorization_last_updated'] = authorization_last_updated_col
    df_okra['Authorization_link'] = authorization_link_col
    df_okra['Authorization_next_payment'] = authorization_next_payment_col
    df_okra['Authorization_payLink'] = authorization_payLink_col
    df_okra['Authorization_type'] = authorization_type_col
    df_okra['Authorization_used'] = authorization_used_col
    df_okra['AuthorizationId'] = authorizationId_col
    df_okra['BankId'] = bankId_col
    df_okra['BankName'] = bankName_col
    df_okra['bankSlug'] = bankSlug_col
    df_okra['bankType'] = bankType_col
    df_okra['callbackURL'] = callbackURL_col
    df_okra['callback_code'] = callback_code_col
    df_okra['callback_type'] = callback_type_col 
    df_okra['callback_url'] = callback_url_col
    df_okra['code'] = code_col
    df_okra['country'] = country_col
    df_okra['current_project'] = current_project_col
    df_okra['customerEmail'] = customerEmail_col
    df_okra['customerId'] = customerId_col
    df_okra['ended_at'] = ended_at_col
    df_okra['env'] = env_col
    df_okra['extras'] = extras_col
    df_okra['identityType'] = identityType_col
    df_okra['login_type'] = login_type_col
    df_okra['message'] = message_col
    df_okra['meta'] = meta_col
    df_okra['method'] = method_col
    df_okra['options'] = options_col
    df_okra['owner'] = owner_col
    df_okra['record'] = record_col 
    df_okra['recordId'] = recordId_col
    df_okra['started_at'] = started_at_col
    df_okra['status_webhook'] = status_webhook_col
    df_okra['token'] = token_col
    df_okra['type'] = type_col
    return df_okra


def subfilter_DF(df_to_process):

    in_df_error = df_to_process[df_to_process['text'].str.contains('LOAN ERROR')]
    in_df_api_request = df_to_process[df_to_process['text'].str.contains('API REQUEST')]
    in_df_client_mobile_login = df_to_process[df_to_process['text'].str.contains('CLIENT MOBILE LOGIN')]
    in_df_sms_payload = df_to_process[df_to_process['text'].str.contains('SMS PAYLOAD')]
    in_df_sms_success = df_to_process[df_to_process['text'].str.contains('SMS SUCCESS')]
    in_df_wallet_success = df_to_process[df_to_process['text'].str.contains('WALLET SUCCESS')]
    in_df_okra_webhook = df_to_process[df_to_process['text'].str.contains('OKRA WEBHOOK')]

    return in_df_error, \
            in_df_api_request, \
            in_df_client_mobile_login, \
            in_df_sms_payload,\
            in_df_sms_success,\
            in_df_wallet_success,\
            in_df_okra_webhook


def concatenate_info_DF(df_to_process, out_path = "./liberta_leasing"):
    """
    function which creates a clean final  dataframe with all parsed transaction
    """

    
    input_df_error ,input_df_api_request,input_df_client_mobile_login,input_df_sms_payload,\
        input_df_sms_success,input_df_wallet_success,input_df_okra_webhook = subfilter_DF(df_to_process)

    resultat_df_error = parse_ERROR_DF(input_df_error)
    resutat_df_api =  parse_API_REQUEST_DF(input_df_api_request)
    resutat_df_client_mobile_login = parse_CLIENT_MOBILE_LOGIN_DF(input_df_client_mobile_login)
    resutat_df_sms_payload = parse_SMS_PAYLOAD_DF(input_df_sms_payload)
    resutat_df_sms_success = parse_SMS_SUCCESS_DF(input_df_sms_success)
    resutat_df_wallet_success = parse_WALLET_SUCCESS_DF(input_df_wallet_success)
    df_okra_webhook = parse_OKRA_WEBHOOK_DF(input_df_okra_webhook)

    # List of our processed dataframes
    pdList = [resultat_df_error, 
            resutat_df_api, 
            resutat_df_client_mobile_login, 
            resutat_df_sms_payload, 
            resutat_df_sms_success, 
            resutat_df_wallet_success, 
            df_okra_webhook]  

    # concatenate the dataframes on the axis=1
    df_final = pd.concat(pdList)
    df_final.to_csv(os.path.join(out_path,'nirra_log_bot.csv'), index=None, sep= ',')

    return df_final


def get_unique_numbers_DF(final_df):
    """
    function which returns all users phone number ( =identification)
    """
    assert ('Phone_Number' in final_df.columns) == True
    all_users_phone_number = [element for element in final_df['Phone_Number'].unique() if element != None]
    return all_users_phone_number


def save_info_per_phone_number_DF(final_df):
    """
    function which saves all dataframes by phone number ( =identification)
    """
    os.makedirs("./liberta_leasing/files", exist_ok=True)
    all_users_phone_num = get_unique_numbers_DF(final_df)
    for phoneNumber in tqdm(all_users_phone_num):
        df_phone_num = final_df[final_df['Phone_Number'] == phoneNumber]
        df_phone_num.to_csv(f'./liberta_leasing/files/{phoneNumber}.csv', index=None)


if __name__=='__main__':

    """     text_type_request = \
    "[info] - [""[WALLET SUCCESS]:"",""{\""account_number\"":\""9980432021\"",\""account_name\"":\""LIBERTA(Kayode  Ajao)\"",\""bvn\"":\""22213975706\"",\""requestSuccessful\"":true,\""responseMessage\"":\""Reserved Account Generated Successfully\"",\""responseCode\"":\""00\""}""]"
    """ 
    #parse_WALLET_SMS_PAYLOAD_SUCCESS_ROW(text_type_request)


    # step 1: convert json to csv
    process_json(path_file_json="./nirra-log-bot", 
                dest_path="./liberta_leasing")
    
    # step 2: extract info from all csv
    all_csv = glob2.glob("./liberta_leasing/*.csv")

    csv_file_to_process = "./liberta_leasing/2021-03-27.csv"

    df_to_process = pd.read_csv(csv_file_to_process, 
                                sep='|', 
                                error_bad_lines=False)

    finalized_df = concatenate_info_DF(df_to_process, 
                                        out_path = "./liberta_leasing")