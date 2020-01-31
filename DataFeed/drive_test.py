from apiclient import errors
from datetime import datetime as dt
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from constants import GOOGLE_MIME_TYPES
import os
import pandas as pd
import pickle
import re

SCOPES = [
    'https://www.googleapis.com/auth/drive',
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
    return {"drive": drive_service, "sheets": sheets_service}


def get_inputs_id(service, folder_id, only_folders=False):
    folders_mime = "mimeType='{}'".format(GOOGLE_MIME_TYPES["folder"])
    if only_folders:
        query = "{} and '{}' in parents".format(folders_mime, folder_id)
    else:
        query = "'{}' in parents".format(folder_id)
    try:
        resp_all_folders = service.files().list(
            q=query).execute()
        all_files = resp_all_folders.get('files', [])
        if len(all_files) == 0:
            return None
        else:
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
    folder_condition = "mimeType='{}'".format(GOOGLE_MIME_TYPES["folder"])
    folder_query = "name = '{q}' and {t}".format(q=folder_name,
                                                 t=folder_condition)
    print(folder_query)
    try:
        response = drive_service.files().list(
            q=folder_query, fields="files(id, name)").execute()
        folder_id = get_folder_id(response, drive_service)
        if folder_id:
            inputs_file_id = get_inputs_id(drive_service, folder_id)
            # print(inputs_file_id)
            if inputs_file_id:
                return {"self_id": folder_id, "children": inputs_file_id}
            else:
                return {"self_id": folder_id, "children": None}
        else:
            print("No Folder found!")
            return {"self_id": None, "children": None}
    except errors.HttpError as error:
        print("ERROR in handle_inputs: {}".format(error))
        return {"self_id": None, "children": None}


def check_file(parent_folder_id, file_name, file_type, service):
    match = False
    if file_type.strip() == "spreadsheet":
        mime_type = "mimeType='{}'".format(GOOGLE_MIME_TYPES["spreadsheet"])
    elif file_type.strip() == "folder":
        mime_type = "mimeType='{}'".format(GOOGLE_MIME_TYPES["folder"])
    if parent_folder_id:
        query_str = "name = '{name}' and {mime} and '{pid}' in parents"
        query = query_str.format(name=file_name,
                                 mime=mime_type,
                                 pid=parent_folder_id)
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


def create_spreadsheet(title, data_df, services, folder_id=None):
    if not data_df.empty:
        drive, sheets = services["drive"], services["sheets"]
        pre_exists, msg_ = check_file(folder_id, title, "spreadsheet", drive)
        if pre_exists:
            print(msg_)
            file_pattern_ = r"(.*)-v(\d+)"
            match_ = re.search(file_pattern_, title)
            groups_ = match_.groups() if match_ else []
            new_version = int(groups_[-1]) + 1 if groups_ else 0
            prefix = groups_[0] if groups_ else title
            title = "{}-v{}".format(prefix, new_version)
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


def process_geo_specific_data(data_df, target_folder, services, folders_object):
    print(target_folder)
    if check_internal_folder(folders_object["children"], target_folder):
        geo_folders = get_folder(target_folder, services["drive"])
        print(geo_folders)
        folder_id = geo_folders["self_id"]
        prefix_ = "Data-Collection"
        suffix_ = "v0"
        if target_folder == "it-it":
            prefix_ = "Italy-Collection"
        # spreadsheet_name = "{}-{}-{}".format(prefix_, date_today, suffix_)
        spreadsheet_name = "{}-{}".format(prefix_, suffix_)
        print("\nSpreadsheet-name: {}\n".format(spreadsheet_name))
        create_sheet_response = create_spreadsheet(title=spreadsheet_name,
                                                   data_df=data_df,
                                                   services=services,
                                                   folder_id=folder_id)
        print("Response: \n'{}'".format(create_sheet_response))
    else:
        print("Parent folder does not match with the target folder")


def create_folder(folder_name, target_folder, drive_service):
    new_folder_id, create_folder_response = None, None
    print("Creating Folder '{}' inside '{}'".format(folder_name, target_folder))
    selected_folders = get_folder(target_folder, drive_service)
    print(selected_folders)
    parent_folder_id = selected_folders["self_id"]
    try:
        folder_metadata = {
            "name": folder_name,
            "mimeType": GOOGLE_MIME_TYPES["folder"],
            "parents": [parent_folder_id]
        }
        if parent_folder_id:
            print("\tSelf ID of Parent Folder: '{}'".format(parent_folder_id))
            folder_exists, msg = check_file(parent_folder_id=parent_folder_id,
                                            file_name=folder_name,
                                            file_type="folder",
                                            service=drive_service)
            print("{}: {}".format(msg, folder_exists))
            if not folder_exists:
                create_folder_response = drive_service.files().create(
                    body=folder_metadata).execute()
        else:
            print("\tCould not trace Parent Folder ID.")
            _ = folder_metadata.pop("parents")
            create_folder_response = drive_service.files().create(
                body=folder_metadata).execute()
        if create_folder_response:
            print(create_folder_response)
            new_folder_id = create_folder_response.get("id")

    except errors.HttpError as e:
        print("\nERROR: Folder couldn't be created because:\n{}".format(e))
    return new_folder_id


"""
if __name__ == "__main__":
    services_ = test_drive()
    internal_folders = get_folder("Test Data", services_["drive"])
    if internal_folders["children"]:
        # print(internal_folders)
        files = [f for f in internal_folders["children"] if
                 f["mimeType"] != "application/vnd.google-apps.folder"]
        print(files)
        # for files in internal_folders["children"]:
        #     if files["mimeType"] != "application/vnd.google-apps.folder":
        #         print("{}:".format(files["name"]))
        process_geo_specific_data("it-it", services_["drive"],
                                  services_["sheets"], internal_folders)
    else:
        print("No folder with the name Test Data found")

"""
