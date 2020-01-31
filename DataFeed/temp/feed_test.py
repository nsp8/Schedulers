"""
import re
import pandas as pd
import pickle
import os
from apiclient import errors
from datetime import datetime as dt
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    # 'https://www.googleapis.com/auth/drive.file',
    # 'https://www.googleapis.com/auth/spreadsheets',
    # 'https://www.googleapis.com/auth/spreadsheets.readonly'
]


def test_drive():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'gdrive_creds.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    # Calling the Drive API
    # results = drive_service.files().list(
    #     pageSize=10, fields="nextPageToken, files(id, name)").execute()
    # items = results.get('files', [])
    #
    # if not items:
    #     print("No Files Found!")
    # else:
    #     print("Files:")
    #     for item in items:
    #         print("{} ({})".format(item['name'], item['id']))
    return drive_service, sheets_service


def get_inputs_id(service, folder_id):
    # query = "name = 'Test Data' and '{}' in parents".format(folder_id)
    folders_mime = "mimeType='application/vnd.google-apps.folder'"
    folders_query = "{} and '{}' in parents".format(folders_mime, folder_id)
    try:
        resp_all_folders = service.files().list(
            q=folders_query).execute()
        all_files = resp_all_folders.get('files', [])
        # print(all_files)
        # response = service.files().list(q=query).execute()
        # files = response.get('files', [])
        if len(all_files) == 0:
            return None
        # if len(all_files) == 1:
        #     return all_files[0]
        else:
            # file_list = list()
            # for file in all_files:
            #     file_list.append(file['id'])
            # return file_list
            return all_files
    except errors.HttpError as error:
        print("ERROR in get_inputs_id: {}".format(error))


def get_folder_id(resultset, service_object, check_parents=True):
    folders = resultset.get('files', [])
    if not folders:
        print("No files found.")
    else:
        print("Found target")
        if len(folders) == 1:
            print("\tFound folder: {}".format(folders[0]['name']))
            return folders[0]['id']
        else:
            print("Many folders")
            file_ids = list()
            for folder in folders:
                print("\tTrying folder: {}".format(folder['name']))
                folder_id = folder['id']
                print("\t\tFile ID: {}".format(folder_id))
                if check_parents:
                    inputs_file_id = get_inputs_id(service_object, folder_id)
                    if inputs_file_id:
                        return folder_id
                else:
                    file_ids.append(folder_id)
            if file_ids:
                return file_ids
            return None


def get_folder(folder_name, drive_service):
    folder_condition = "mimeType='application/vnd.google-apps.folder'"
    folder_query = "name = '{q}' and {t}".format(q=folder_name,
                                                 t=folder_condition)
    print(folder_query)
    # drive_service = test_drive()
    try:
        response = drive_service.files().list(
            q=folder_query, fields="files(id, name)").execute()
        folder_id = get_folder_id(response, drive_service)
        if folder_id:
            inputs_file_id = get_inputs_id(drive_service, folder_id)
            # print(inputs_file_id)
            if inputs_file_id:
                return {"parent_id": folder_id, "children": inputs_file_id}
            else:
                return {"parent_id": folder_id, "children": None}
        else:
            print("No Folder found!")
            return {"parent_id": None, "children": None}
    except errors.HttpError as error:
        print("ERROR in handle_inputs: {}".format(error))
        return {"parent_id": None, "children": None}


def check_file_temp(folder_name, file_name, file_type, service):
    folder_object = None
    if file_type.strip() == "spreadsheet":
        match = False
        print(folder_name)
        if folder_name:
            folder_object = get_folder(folder_name, service)
            for sub_folder in folder_object["children"]:
                print("Sub Folder: {}".format(sub_folder["name"]))
                if file_name == sub_folder["name"]:
                    print("Match of the file was found!")
                    match = True
                    break
        if not match:
            mime_type = "mimeType='application/vnd.google-apps.spreadsheet'"
            if folder_object:
                query_str = "name = '{name}' and {mime} and '{pid}' in parents"
                query = query_str.format(name=file_name,
                                         mime=mime_type,
                                         pid=folder_object["parent_id"])
            else:
                query_str = "name = '{name}' and {mime}"
                query = query_str.format(name=file_name,
                                         mime=mime_type)
            response = service.files().list(q=query,
                                            fields="files(id, name)").execute()
            items = get_folder_id(response, service, check_parents=False)
            s = "{} {} found"
            if items:
                print(items)
                n = len(items)
                msg = s.format(n, "files") if n > 1 else s.format(n, "file")
            else:
                msg = s.format(0, "files")
        return match, msg


def check_file(folder_id, file_name, file_type, service):
    if file_type.strip() == "spreadsheet":
        match = False
        mime_type = "mimeType='application/vnd.google-apps.spreadsheet'"
        if folder_id:
            query_str = "name = '{name}' and {mime} and '{pid}' in parents"
            query = query_str.format(name=file_name,
                                     mime=mime_type,
                                     pid=folder_id)
        else:
            query_str = "name = '{name}' and {mime}"
            query = query_str.format(name=file_name,
                                     mime=mime_type)
        try:
            print("\ncheck-file-query: {}".format(query))
            response = service.files().list(q=query,
                                            fields="files(id, name)").execute()
            items = get_folder_id(response, service, check_parents=False)
            s = "{} {} found"
            if items:
                print(items)
                n = len(items)
                msg = s.format(n, "files") if n > 1 else s.format(n, "file")
                match = True
            else:
                msg = s.format(0, "files")
            return match, msg
        except errors.HttpError as error:
            msg = "ERROR in handle_inputs: {}".format(error)
            return False, msg


def check_internal_folder(folders, name):
    match = False
    for folder in folders:
        if folder["name"] == name:
            match = True
    return match


def df_to_list(data_df):
    data_parts = data_df.to_dict(orient="split")
    cols_ = data_parts["columns"]
    data = data_parts["data"]
    data.insert(0, cols_)
    return data


def move_file(file_id, target_id, service):
    try:
        update_response = service.files().update(fileId=file_id,
                                                 addParents=target_id).execute()
        return update_response
    except errors.HttpError as e:
        print("Could not move file to the parent folder because:\n{}".format(e))
        return None


def create_spreadsheet(title, data_df, drive, sheets, folder_id=None):
    if not data_df.empty:
        pre_exists, msg_ = check_file(folder_id, title, "spreadsheet", drive)
        if pre_exists:
            print(msg_)
            file_pattern_ = r"(.*)-v(\d+)"
            match_ = re.search(file_pattern_, title)
            groups_ = match_.groups() if match_ else []
            new_version = int(groups_[-1]) + 1 if groups_ else 0
            prefix = groups_[0] if groups_ else title
            title = "{}-v{}".format(prefix, new_version)
        # else:
        #     title = "{}-v0".format(title)
        spreadsheet = {
            "properties": {"title": title}
        }
        try:
            new_sheet = sheets.spreadsheets().create(
                body=spreadsheet, fields='spreadsheetId').execute()
            new_sheet_id = new_sheet.get('spreadsheetId')
            if new_sheet_id:
                print("New Spreadsheet-ID: {}".format(new_sheet_id))
                move_response = move_file(new_sheet_id, folder_id, drive)
                print("Response for move object: {}".format(move_response))
                request = {
                    "id": new_sheet_id,
                    "range": "Sheet1!A:B",
                    "body": {"majorDimension": "ROWS",
                             "values": df_to_list(data_df)}
                }
                try:
                    new_sheet = sheets.spreadsheets().values().update(
                        spreadsheetId=request["id"],
                        range=request["range"],
                        body=request["body"],
                        valueInputOption="RAW").execute()
                    return new_sheet
                except errors.HttpError as e:
                    print("Sheet couldn't be updated because of:\n{}".format(e))
                    return None
            else:
                return None
        except errors.HttpError as error:
            print("New Sheet couldn't be created because of:\n{}".format(error))
            return None


def process_geo_specific_data(target_folder, drive, sheets, folders_object):
    print(target_folder)
    if check_internal_folder(folders_object["children"], target_folder):
        geo_folders_ = get_folder(target_folder, drive_)
        print(geo_folders_)
        folder_id = geo_folders_["parent_id"]
        data_ = [{"a": 90, "b": 80}, {"a": 110, "b": 770}]
        df_ = pd.DataFrame(data_, columns=["a", "b"])
        print(df_)
        formatter = lambda s: "0{}".format(s) if int(s) < 10 else "{}".format(s)
        date_today = "{year}-{month}-{day}".format(
            year=dt.now().year, month=formatter(dt.now().month),
            day=formatter(dt.now().day))
        prefix_ = "Data-Collection"
        suffix_ = "v0"
        if target_folder == "it-it":
            prefix_ = "Italy-Collection"
        spreadsheet_name = "{}-{}-{}".format(prefix_, date_today, suffix_)
        print("\nSpreadsheet-name: {}\n".format(spreadsheet_name))
        # flag, msg = check_file(None, spreadsheet_name, "spreadsheet", drive_)
        # print(msg)
        create_sheet_response = create_spreadsheet(title=spreadsheet_name,
                                                   data_df=df_,
                                                   drive=drive,
                                                   sheets=sheets,
                                                   folder_id=folder_id)
        print("Response: \n'{}'".format(create_sheet_response))
        # geo_pattern_ = r"(\w+)-(\w+)"
        # geo_match_ = re.search(geo_pattern_, target_folder_)
        # if geo_match_:
        #     lang, country = geo_match_.groups()
        #     if country == "it":
        #         pass


if __name__ == "__main__":
    drive_, sheets_ = test_drive()
    internal_folders = get_folder("Test Data", drive_)
    if internal_folders["children"]:
        print(internal_folders)
        process_geo_specific_data("it-it", drive_, sheets_, internal_folders)
        # target_folder = "it-it"
        # if check_internal_folder(internal_folders["children"], target_folder):
        #     it_it_folders = get_folder(target_folder, drive)
        #     print(it_it_folders)
        #     data_ = [{"a": 90, "b": 80}, {"a": 110, "b": 770}]
        #     df_1 = pd.DataFrame(data_, columns=["a", "b"])
        #     print(df_1)
        #     spreadsheet_name = "TestData"
        #
        #     # root_folders = get_folder("My Drive", drive)
        #     # if root_folders["children"]:
        #     flag, msg = check_file(None, spreadsheet_name, "spreadsheet", drive)
        #     print(msg)
        #     response = create_spreadsheet(spreadsheet_name, df_1, drive, sheets,
        #                                   target_folder)
            # if response:
            #     print("\tUpdated Cells: {}".format(response["updatedCells"]))
            #     # print("\tUpdated data: \n\t{}".format(response["updatedData"]))
            # else:
            #     print("ERROR: Either the spreadsheet was not created or there "
            #           "was another issue.")

    # selected_folder = "Test Data"
    # targets = [f for f in internal_folders if f["name"] == selected_folder]
    # get_folder("Test Data")

# collector.get_saved_versions("Italy-Collection-2020", "it-it")
# table_cols = db.get_columns(database="search_automation",
#                             table="text_data_reserve")
# print(table_cols)

drive_services = drive.test_drive()
test_folder = drive.get_folder("New Test Directory", drive_services["drive"])
print(test_folder)
if not test_folder["self_id"]:
    new_folder = drive.create_folder("New Test Directory", "Test Data",
                                     drive_services["drive"])
    print(new_folder)
else:
    print("Folder already exists!")
"""
