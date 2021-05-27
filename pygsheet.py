# %%
import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
from os import path
import pandas as pd
import pickle
from xlsxwriter.utility import xl_col_to_name, xl_cell_to_rowcol
import socket

credential_dir = path.join(path.dirname(__file__), 'credential') 
socket.setdefaulttimeout(600)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets'
         , 'https://www.googleapis.com/auth/drive']

creds = None
if os.path.exists(f'{credential_dir}/token.pickle'):
    with open(f'{credential_dir}/token.pickle', 'rb') as token:
        creds = pickle.load(token)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(f'{credential_dir}/credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    with open(f'{credential_dir}/token.pickle', 'wb') as token:
        pickle.dump(creds, token)


class WrongA1notationError(Exception):
    def __init__(self, message="Your a1 notation is wrong, please modify it."):
        self.message = message
        super().__init__(self.message)


def read_data_from_gsheet(sheet_id: str, range: str):

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=range).execute()['values']
    result_df = pd.DataFrame(result[1:], columns=result[0]).apply(pd.to_numeric, errors='ignore')

    return result_df


def update_data_to_gsheet(spreadsheet_id: str, range: str, values: list, append=False, sheet_id=''):

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()
    if append == False:
        result = sheet.values().update(spreadsheetId=spreadsheet_id, range=range, body={'values':values}, valueInputOption='USER_ENTERED').execute()
    else:
        requests = [{'appendDimension': {'sheetId':sheet_id, "dimension":"ROWS", "length":len(values)}}]
        params = {'spreadsheetId': spreadsheet_id,
                'body': {'requests': requests}}
        result = service.spreadsheets().batchUpdate(**params).execute()

    return result.get('updatedCells')


def clear_data_from_gsheet(spreadsheet_id: str, range: str):

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()

    sheet.values().clear(spreadsheetId=spreadsheet_id, range=range).execute()


def clear_filters(spreadsheet_id: str, sheet_name: str):

    
    sheet_id = get_sheet_info(spreadsheet_id, sheet_name)['sheet_id']
    requests = [{'clearBasicFilter': {'sheetId': sheet_id}}]
    params = {'spreadsheetId': spreadsheet_id,
              'body': {'requests': requests}}

    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    service.spreadsheets().batchUpdate(**params).execute()


def add_filters(spreadsheet_id: str, start_row_index: int, start_column_index: int, sheet_name: str, end_row_index = 0, end_column_index = 0):
    
    sheet_info = get_sheet_info(spreadsheet_id, sheet_name)
    sheet_id = sheet_info['sheet_id']

    if end_row_index is None:
        end_row_index = sheet_info['grid_properties']['rowCount']
    if end_column_index is None:
        end_column_index = sheet_info['grid_properties']['columnCount']

    my_range = {
        'sheetId': sheet_id,
        'startRowIndex': start_row_index,
        'startColumnIndex': start_column_index,
        'endRowIndex': start_row_index+end_row_index-1,
        'endColumnIndex': start_column_index+end_column_index-1
    }
    addFilterViewRequest = {
        'setBasicFilter':{
            'filter':{
                'range': my_range,
                'sortSpecs': [{
                    'dimensionIndex': 1,
                    'sortOrder': 'DESCENDING'
                }]
            }
        }
    }
    request_body = {'requests': [addFilterViewRequest]}

    __execute_batchUpdate(spreadsheet_id, request_body)


def reset_filters(spreadsheet_id: str, sheet_name: str):
    sheet_info = get_sheet_info(spreadsheet_id, sheet_name)
    if 'filter' not in sheet_info:
        raise ValueError("filter not found")
    filter = sheet_info['filter']
    clear_filters(spreadsheet_id, sheet_name)
    add_filters(spreadsheet_id=spreadsheet_id
                , sheet_name=sheet_name
                , start_row_index=filter['startRowIndex']
                , start_column_index=filter['startColumnIndex']
                , end_row_index=filter['endRowIndex']
                , end_column_index=filter['endColumnIndex'])


def get_sheet_info(spreadsheet_id: str, sheet_name: str):
    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_info = {}
    for sheet in sheets:
        if sheet['properties']['title'] == sheet_name:
            sheet_info['sheet_id'] = sheet['properties']['sheetId']
            sheet_info['grid_properties'] = sheet['properties']['gridProperties']
            if 'basicFilter' in sheet:
                sheet_info['filter'] = sheet['basicFilter']['range']
            return sheet_info
    raise ValueError("Sheet name not found")


def clear_sheet(spreadsheet_id: str, sheet_name: str):
    sheet_id = get_sheet_info(spreadsheet_id, sheet_name)['sheet_id']
    request_body = { 'requests': [
            {
                "updateCells": {
                    "range": {
                    "sheetId": sheet_id
                    },
                    "fields": "*"
                }
            }
        ]
    }

    __execute_batchUpdate(spreadsheet_id, request_body)


def create_sheet(spreadsheet_id: str, sheet_name: str):
    request_body = { 'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
        }

    __execute_batchUpdate(spreadsheet_id, request_body)


def delete_sheet(spreadsheet_id: str, sheet_id: int):
    request_body = { 'requests': [{
            'deleteSheet': {
                'sheetId' : sheet_id
                }
        }]
    }
    __execute_batchUpdate(spreadsheet_id, request_body)


def __execute_batchUpdate(spreadsheet_id :str, request_body :dict):
    service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()


def update_dataframe_to_gsheet(spreadsheet_id: str, sheet_name: str, df:pd.DataFrame, start_cell='A1', clear_filter=True, clear_whole_sheet=False, reset_filter=False):

    try:
        (start_cell_row, start_cell_col) = xl_cell_to_rowcol(start_cell)
    except AttributeError:
        raise WrongA1notationError

    if clear_whole_sheet:
        clear_sheet(spreadsheet_id, sheet_name)
    else:
        clear_data_from_gsheet(spreadsheet_id=spreadsheet_id, range=f"{sheet_name}!{start_cell}:{xl_col_to_name(int(start_cell_col)+df.shape[1]-1)}{int(start_cell_row)+df.shape[0]+1}")

    if reset_filter:
        reset_filters(spreadsheet_id, sheet_name)

    if clear_filter:
        clear_filters(spreadsheet_id, sheet_name)

    update_data_to_gsheet(spreadsheet_id=spreadsheet_id
                        , range=f"{sheet_name}!{start_cell}:{xl_col_to_name(int(start_cell_col)+df.shape[1]-1)}{int(start_cell_row)+df.shape[0]+1}"
                        , values=[df.columns.tolist()] + df.values.tolist())


def download_gsheet_as_excel(file_id: str, path: str):

    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    files = service.files()
    request = files.export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    with open(path, 'wb') as f:
        f.write(request.execute())


def download_gsheet_as_pdf(spreadsheet_id: str, sheet_id: str, path: str):

    import requests
    accessToken = creds.token
    url = ('https://docs.google.com/spreadsheets/d/' + spreadsheet_id + '/export?'
        + 'format=pdf'  # export as PDF
        + '&portrait=false'  # landscape
        + '&top_margin=0.00'  # Margin
        + '&bottom_margin=0.00'  # Margin
        + '&left_margin=0.00'  # Margin
        + '&right_margin=0.00'  # Margin
        # + '&pagenum=RIGHT'  # Put page number to right of footer
        + '&gid=' + sheet_id  # sheetId
        + '&access_token=' + accessToken)  # access token
    r = requests.get(url)
    with open(path, 'wb') as saveFile:
        saveFile.write(r.content)


def download_file_from_gdrive(file_id: str, path: str):

    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    files = service.files()
    request = files.get_media(fileId=file_id)
    with open(path, 'wb') as f:
        f.write(request.execute())


def list_fileids_from_query(query: str):

    service = build('drive', 'v2', credentials=creds, cache_discovery=False)
    return service.files().list(q=f"title contains '{query}'", spaces='drive', fields='items(id, title)').execute()


def __trim(im):

    from PIL import Image, ImageChops
    bg = Image.new(im.mode, im.size, im.getpixel((0,0)))
    diff = ImageChops.difference(im, bg)
    diff = ImageChops.add(diff, diff, 2.0, -100)
    bbox = diff.getbbox()
    if bbox:
        return im.crop(bbox)


def pdf_to_image(pdf_path, image_path):

    import fitz
    from PIL import Image

    pdfDoc = fitz.open(pdf_path)
    for pg in range(pdfDoc.pageCount):
        page = pdfDoc[pg]
        zoom_x = 1
        zoom_y = 1
        mat = fitz.Matrix(zoom_x, zoom_y)
        pix = page.getPixmap(matrix=mat, alpha=False)

        if not os.path.exists(image_path):
            os.makedirs(image_path)

        pix.writePNG(image_path+'/'+'images_%s.png' % pg)

    images = [Image.open(x) for x in [image_path+'/'+'images_%s.png' % pg for pg in range(pdfDoc.pageCount)]]
    widths, heights = zip(*(i.size for i in images))
    max_height = sum(heights)
    new_im = Image.new('RGB', (widths[0], max_height))
    x_offset = 0
    for im in images:
        new_im.paste(im, (0,x_offset))
        x_offset += im.size[1]

    # for img_tmp in [image_path+'/'+'images_%s.png' % pg for pg in range(pdfDoc.pageCount)]:
    #     os.remove(img_tmp)

    new_im = __trim(new_im)
    new_im.save(image_path + 'output.png', subsampling=0, quality=100)