import os 
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
scope = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file('credentials.json', scopes=scope)
service = build('drive', 'v3', credentials=creds)
folder_name = 'BP_download'

def get_folder_id(service, folder_name):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    
    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)'
    ).execute()
    
    items = results.get('files', [])
    if not items:
        return None
    else:
        for item in items:
            return item['id']
        
def upload(service, folder_id, file_path, file_name):
    media = MediaFileUpload(file_path, resumable=True)
    
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]  
    }
    
    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

def upload_to_folder(file_path, file_name):
    folder_id = get_folder_id(service, folder_name=folder_name)
    if folder_id: 
        upload(service=service, folder_id=folder_id, file_path=file_path, file_name=file_name)
        os.remove(file_path)