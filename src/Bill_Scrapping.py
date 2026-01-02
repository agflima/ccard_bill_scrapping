import pandas as pd
import pdfplumber as pdf
from loguru import logger
import re
from os import listdir, rename, getenv
from os.path import isfile, join
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

pwd = getenv("PDF_PWD")
path = getenv("PATH")
proc_path = getenv("PROC_PATH")
now_path = getenv("NOW_PATH")
err_path = getenv("ERR_PATH")

structured = []
pdf_path = ''
historical = ''

for i in listdir(path):
    if '.pdf' in i and isfile(join(path,i)):
        next_dues = False
        pdf_path = join(path,i)
        try:
            logger.info(f'Moving file {i} to processing directory {proc_path}.')
            now = join(now_path,i)
            rename(pdf_path,now)
        except Exception as e:
            logger.error(f'Error {e}, while moving file {i} to processing directory. check the input at {err_path}')
            err_dir = rename(pdf_path,join(err_path,i))
            logger.info(f'Moving file {e} to error directory {err_path}')
            rename(pdf_path,err_dir)

        try:
            with pdf.open(now, password=pwd) as pdf:
                logger.info(f"Pages in file: {len(pdf.pages)}\n")
                start = now.rindex("\\")
                end = now.rindex(".")
                file_name = now[(start+1):end]
                month = file_name[4:6]
                year = file_name[7:len(file_name)]
                logger.info(file_name,month, year)
                for pagina in (pdf.pages):
                    text = pagina.extract_words(x_tolerance=0.5,y_tolerance = 0.5)
                    lines = {}
                    left_reg = []
                    right_reg = []
                    x_split = 350
    
                    for tex in text:
                        top = round(tex["top"])
                        if top in lines:
                            lines[top].append(tex)
                        else:
                            lines[top] = []
                            lines[top].append(tex)
    
                    for topster in sorted(lines.keys()):
                        tokens = sorted(lines[topster],key=lambda t:t["x0"])
                        left = []
                        right = []
                        for t in tokens:
                            if t["x0"] < x_split:
                                left.append(t["text"])
                            else:
                                right.append(t["text"])
                        left_lines = left
                        right_lines = right
                        right_line = " ".join(right_lines)
                        left_line = " ".join(left_lines)
    
                        if 'próximas faturas' in left_line:
                            next_dues_left = True
                        if 'próximas faturas' in right_line:
                            next_dues_right = True                            
                        
                        if not next_dues_left:
                            data_left = re.search(r"(\d{2}/\d{2})",left_line)
                            money_left = re.search(r"(-?\d{1,3}(?:\.\d{3})*,\d{2})", left_line)
                            if data_left and money_left:
                                if re.match(r"(\d{2}/\d{2})",left_line):
                                    left_reg.append(left_line)
                        if not next_dues_right:
                            data_right = re.search(r"(\d{2}/\d{2})",right_line)
                            money_right = re.search(r"(-?\d{1,3}(?:\.\d{3})*,\d{2})", right_line)
                            if data_right and money_right:
                                if re.match(r"(\d{2}/\d{2})",right_line):
                                    right_reg.append(right_line)
                        else:
                            continue
                    for token in left_reg:
                        tokens = token.split(" ")
                        date = tokens[0]
                        value = tokens[-1]
                        desc_tokens = tokens[1:-1]
                        desc = " ".join(desc_tokens)
                        dic = {"Year":year,
                                      "Month":month,
                                      "Date":date, 
                                      "Card":"----", 
                                      "Category":"",
                                      "Description":desc, 
                                      "Value":value
                                      }
                        structured.append(dic)
                    for token in right_reg:
                        tokens = token.split(" ")
                        date = tokens[0]
                        value = tokens[-1]
                        desc_tokens = tokens[1:-1]
                        desc = " ".join(desc_tokens)
                        dic = {"Year":year,
                                      "Month":month,
                                      "Date":date, 
                                      "Card":"---", 
                                      "Category":"",
                                      "Description":desc, 
                                      "Value":value
                                      }
                        structured.append(dic)
        except Exception as e:
            logger.error(f'Error {e} for reading {i}. check the input at {err_path}.')        
            err_dir = rename(now,join(err_path,i))
            logger.info(f'Moving the file {i} to {err_dir} directory')
    if '.xlsx' in i and isfile(join(path,i)):
        historical = i

for i in listdir(now_path):        
    logger.warning(f'Moving file {i} to processed directory')    
    archive = join(now_path,i)
    try:
        rename(archive,join(proc_path,i))
    except Exception as e:
        logger.error(f'Error {e} while moving file {i}')

df_new = pd.DataFrame(structured)
df_raw = df_new.copy()
df = df_raw.copy()
df = df.assign(
    Invoice_Date = pd.to_datetime(
        df['Year'].astype(str) + '-' +
        df['Month'].astype(str).str.zfill(2) + '-01'
    )
)
df = df.sort_values('Invoice_Date')
df['Value_clean'] = (
    df['Value']
    .str.replace('.','',regex=False)
    .str.replace(',','.',regex=False)
)
df['Value_clean'] = pd.to_numeric(df['Value_clean'])
df['Value_clean'].dtype
df['Value'] = df['Value_clean']
df.drop(columns=['Value_clean'],inplace=True)
mask = df['Description'].str.strip().str.endswith('-')
df.loc[mask,'Value'] = -df.loc[mask,'Value'].abs()
df.loc[mask,'Description'] = (
    df.loc[mask,'Description']
    .str.replace(r'\s*-$','',regex=True)
    .str.strip()
)