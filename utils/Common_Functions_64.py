'''List of Useful Common Functions; '''
''''Developed by JIAJUNLEE'''

import os
from datetime import datetime, timedelta
import time
import shutil

import pandas as pd
from smtplib import SMTP
import uuid


def makeDirs(folder):
    '''create dirs if not exists'''
    if not os.path.exists(folder):
        os.makedirs(folder)

def countDirs(folder):
    '''Parse folder and return count of files in the folder'''
    count = 0
    for filename in os.listdir(folder):
        path = os.path.join(folder, filename)

        if os.path.isfile(path):
            count += 1
        elif os.path.isdir(path):
            count += countDirs(path)
    return count
     
def delete_file(path):
    '''delete everything on the given path'''
    if os.path.isfile(path) or os.path.islink(path):
       os.remove(path)
    elif os.path.isdir(path):
       shutil.rmtree(path)
    else:
        raise ValueError(f"{path} is not a valid file or dir !")

def generate_uuid(string_list, dns = 'jiajunlee.com', delimiter = ' # | # '):
    '''Parse string and generate UUID'''
    data = delimiter.join(string_list)
    my_dns = uuid.uuid5(uuid.NAMESPACE_DNS, dns)
    my_uuid = uuid.uuid5(my_dns, data)
    return str(my_uuid)

def removeExtraDelimiter(S, delimiter = ','):
    '''Parse string and remove leading/trailing delimiter'''
    S = str(S)
    
    if S == delimiter:
        S = ''
        
    else:
    
        if len(S) > 0:
            if S[-1] == delimiter:
                S = S[:-1]
            
            if S[0] == delimiter:
                S = S[1:]
                
        else:
            S = S
    
    return S
            
def split_into_rows(df, column, sep=',', keep=False):
    '''Parse dataframe and column name, return new dataframe with splitted rows'''
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value)
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    
    return new_df

def digit_to_nondigit(string, keep="First"):
    ''' Split string from digit to non-digit, return First or Last part '''
    string = str(string)
    index = len(string)
    for chr in string:
        if not string[0].isdigit():
            index = len(string)
            break
        else:
            if not chr.isdigit():
                index = string.index(chr)
                break
                
    if keep == "First":
        new_string = str(string[0 : index])
    else:
        new_string = str(string[index:])
    
    return new_string

def nondigit_to_digit(string, keep="First"):
    ''' Split string from non-digit to digit, return First or Last part '''
    string = str(string)
    index = len(string)
    for chr in string:
        if string[0].isdigit():
            index = len(string)
            break
        else:
            if chr.isdigit():
                index = string.index(chr)
                break
                
    if keep == "First":
        new_string = str(string[0 : index])
    else:
        new_string = str(string[index:])
    
    return new_string

def extract_num_from_end(string, keep="number"):
    ''' Read string and return Number part from the end of string '''
    
    '''Reverse the string to loop backwards'''
    reversed_str = string[::-1]
    for char in range(0,len(reversed_str)):
        
        ''' scan until it reaches 1st alphabet'''
        if (reversed_str[char].isalpha()):
            
            ''' reverse back the string and it is the letter part '''
            Letter = reversed_str[char:][::-1]
            # print(Letter)
            break
    
    '''Remove letter from splitted part and split into list'''
    Number = string.replace(Letter,'')
    
    if keep == 'number':
        new_string = Number
    else:
        new_string = Letter
    
    return new_string

def string_remove_duplicate(string,delimiter="\n"):
    '''Parse string, split by delimiter into list, remove dups, convert into string_new'''
    string_list = string.split(delimiter)
    string_list_unique = list(dict.fromkeys(string_list))
    string_new = delimiter.join(string_list_unique)
    return string_new

def snowflake_copy_into_statement(stage_table,columnListAll):
    '''parse table and columnList and return snowflake_copy_into statement'''
    
    columns_all = ", ".join(columnListAll)
    
    range_list = list(range(1,len(columnListAll)+1,1))
    range_all = [f"t.${c}" for c in range_list]
    range_all_statement = ", ".join(range_all)
    
    snowflake_copy_into = f"COPY INTO {stage_table} ({columns_all})" \
                            f" FROM (SELECT {range_all_statement} FROM @"
          
    return snowflake_copy_into

def snowflake_merge_statement(stage_table,table,columnListAll,columnListPrimary,columnListNonPrimary, columnDateTime,updateWhenMatched=True):
    '''Parse tables with column lists and return snowflake_merge statement'''

    columns_primary_list = ", ".join(columnListPrimary)
    columns_primary = [f"t.{c} = s.{c}" for c in columnListPrimary]
    columns_primary_statement = " and ".join(columns_primary)

    columns_non_primary = [f"{c} = s.{c}" for c in columnListNonPrimary]
    columns_non_primary_statement = ", ".join(columns_non_primary)
    
    columns_all = [f"s.{c}" for c in columnListAll]
    columns_all_statement = ", ".join(columns_all)
    
    columns = ", ".join(columnListAll)
     
    if updateWhenMatched:
        snowflake_merge = f"MERGE INTO {table} t" \
                    f" using ( with CTE as (SELECT *,ROW_NUMBER() OVER(PARTITION BY {columns_primary_list} ORDER BY {columnDateTime} desc) rn FROM {stage_table}) SELECT {columns} from CTE where rn = 1) s" \
                    f" ON ({columns_primary_statement})" \
                    f" WHEN MATCHED AND s.{columnDateTime} > t.{columnDateTime} THEN UPDATE SET {columns_non_primary_statement}" \
                    f" WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({columns_all_statement});"
    
    else:
        snowflake_merge = f"MERGE INTO {table} t" \
                f" using ( with CTE as (SELECT *,ROW_NUMBER() OVER(PARTITION BY {columns_primary_list} ORDER BY {columnDateTime} desc) rn FROM {stage_table}) SELECT {columns} from CTE where rn = 1) s" \
                f" ON ({columns_primary_statement})" \
                f" WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({columns_all_statement});"
                
    return snowflake_merge

def df_convert_datetime(dataframe,datetime_column_list):
    '''return dataframe with converted datetime'''
    for column in datetime_column_list:
        dataframe[column] = pd.to_datetime(dataframe[column], errors = 'coerce')
  
    return dataframe

def round_minutes(dt, resolutionInMinutes):
    """round_minutes(datetime, resolutionInMinutes) => datetime rounded to lower interval
    Works for minute resolution up to a day (e.g. cannot round to nearest week).
    """

    # First zero out seconds and micros
    dtTrunc = dt.replace(second=0, microsecond=0)

    # Figure out how many minutes we are past the last interval
    excessMinutes = (dtTrunc.hour*60 + dtTrunc.minute) % resolutionInMinutes

    # Subtract off the excess minutes to get the last interval
    return dtTrunc + timedelta(minutes=-excessMinutes)

def convert_time(dt_in_Milliseconds):
    
    #convert milliseconds to local date time format
    converted_dt = time.localtime(dt_in_Milliseconds)
    
    #convert into desired format
    formated_dt = time.strftime('%Y-%m-%d %H:%M:%S', converted_dt)
    
    #convert into datetime object type
    return datetime.strptime(formated_dt, '%Y-%m-%d %H:%M:%S')

def send_email(content,sender,recipient,subject):
    server = 'mail.micron.com'

    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    msg = MIMEMultipart()
    msg.attach(MIMEText(content, 'html'))# typical values for text_subtype are plain, html, xml
    msg['Subject'] = subject
    msg['From'] = sender  
    msg['To'] = ";".join(recipient)
    conn = SMTP(server)

    conn.sendmail(sender, recipient, msg.as_string())
    conn.quit()

def ExpandSeries(String, delimiter = ','):
    '''Parse string (eg: AB1-AB3,AB6) and expand the alphanumeric series (eg: AB1,AB2,AB3,AB6)'''
    S = String
    expanded_S = ''
    error = False
    
    '''skip function if string does not contains dash '''
    if '-' not in S:
        return S
    
    try:
        '''replace and trim all dash and delimiter, then add special char(1) after each of them'''
        S = S.replace(' ','').replace(',',' ').replace(' -','-').replace('- ','-')
        S = S.replace(' ',' ' + chr(1)).replace('-','-' + chr(1))
        S = chr(1) + S
        # print(S)
        
        '''Split parts into list'''
        Parts = S.split()
        
        '''for each of the splitted part'''
        for Part in Parts:
            '''if splitted part contains dash'''
            if Part.find('-') > -1:
                
                '''split dash parts'''
                DashParts = Part.split("-")
                
                '''taking the 1st dash part, reverse it'''
                reversed_DashPart = DashParts[0][::-1]
                
                '''Loop to return ending letter'''
                for char in range(0,len(reversed_DashPart)):
                    
                    ''' scan until it reaches 1st alphabet'''
                    if (reversed_DashPart[char].isdigit()):
                        
                        ''' reverse back the string and return ending letter part '''
                        Letter_ending = reversed_DashPart[:char][::-1]   
                        reversed_DashPart_without_ending_letter = reversed_DashPart[char:]
                        # print(Letter_ending)
                        # print (reversed_DashPart_without_ending_letter)
                        break     
                    
                '''Loop to return leading letter'''
                for char in range(0,len(reversed_DashPart_without_ending_letter)):
                    
                    ''' scan until it reaches 1st alphabet'''
                    if (reversed_DashPart_without_ending_letter[char].isalpha()):
                        
                        ''' reverse back the string and return leading letter part '''
                        Letter = reversed_DashPart_without_ending_letter[char:][::-1]          
                        # print(Letter)
                        break
                    
                '''Remove letter from splitted part and split into list'''
                Numbers = Part.replace(Letter,'').replace(Letter_ending,'').split('-',1)
                # print(Numbers)
                
                '''Remove chr(1) after dash to handle AB1-3 into AB1,AB2,AB3'''
                Numbers[1] = Numbers[1].replace(chr(1),'')
                
                ''' Get number str length before converting to int '''
                Number_len = len(Numbers[0])
                
                '''it works only if number in ascending, else print error'''
                if int(Numbers[1]) - int(Numbers[0]) >= 1:
                    
                    '''Expand the number series'''
                    for Number in range(int(Numbers[0]), int(Numbers[1])+1):
                        
                        ''' Combining them into final expanded string'''
                        expanded_S = expanded_S + delimiter + Letter + str(Number).zfill(Number_len) + Letter_ending
                        
                else:
                    # print('Number not in ascending!!!')
                    error = True
                    print(f"Error expanding designator: {String}")
    
                    
            else:
                '''if splitted part does not contains dash and directly concat them'''
                expanded_S = expanded_S + delimiter + Part
    
        '''Remove 1st delimter and Remove chr(1)'''
        expanded_S = expanded_S[len(delimiter)+1:].replace(chr(1),'') 
        
    except Exception:
        error = True
        print(f"Error expanding designator: {String}")

    if error == True:
        expanded_S = String
    
    return expanded_S