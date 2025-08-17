import os
import threading
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import time

# Authentication and Google Drive API setup
SCOPES = ['https://www.googleapis.com/auth/drive.file']  # Scope for file access

def authenticate_google_drive():
    """Authenticate and build the Google Drive service."""
    creds = None
    # The token.json file stores the user's access and refresh tokens
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    service = build('drive', 'v3', credentials=creds)
    return service

def get_mime_type(file_path):
    """Return the MIME type for a file."""
    extension = file_path.split('.')[-1].lower()
    mime_types = {
        'mp4': 'video/mp4',
        'txt': 'text/plain',
        'jpg': 'image/jpeg',
        'png': 'image/png',
        'pdf': 'application/pdf',
    }
    return mime_types.get(extension, 'application/octet-stream')

def upload_file(service, file_path, parent_folder_id=None):
    """Upload a single file to Google Drive."""
    try:
        file_metadata = {'name': os.path.basename(file_path)}
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        
        mime_type = get_mime_type(file_path)
        media = MediaFileUpload(file_path, resumable=True, mimetype=mime_type)

        # Request to upload the file
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        response = request.execute()
        print(f"Uploaded: {file_path} with file ID: {response['id']}")
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")
        time.sleep(5)  # Wait before retrying in case of network failure
        upload_file(service, file_path, parent_folder_id)  # Retry on failure

def upload_files_concurrently(service, files, parent_folder_id=None, max_threads=5):
    """Upload multiple files concurrently using a thread pool."""
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(upload_file, service, file, parent_folder_id) for file in files]
        for future in futures:
            future.result()  # Wait for each file to finish uploading

if __name__ == '__main__':
    # Authenticate and get the Google Drive service object
    drive_service = authenticate_google_drive()

    # Define the folder containing the files to upload
    source_folder = r'D:\Projects\Drive_Backup_Threading\Source'  # Update with your folder path
    
    # List the files to upload
    files_to_upload = [str(file) for file in Path(source_folder).rglob('*') if file.is_file()]

    # Upload the files concurrently with a limit of 5 threads
    upload_files_concurrently(drive_service, files_to_upload, max_threads=5)
