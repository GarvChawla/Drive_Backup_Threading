import os
import threading
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pathlib import Path

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

def upload_file(service, file_path, parent_folder_id=None):
    """Upload a file to Google Drive."""
    try:
        file_metadata = {'name': os.path.basename(file_path)}
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        
        media = MediaFileUpload(file_path, resumable=True)

        # Request to upload the file
        request = service.files().create(body=file_metadata, media_body=media, fields='id')
        response = request.execute()
        print(f"Uploaded: {file_path} with file ID: {response['id']}")
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")

def upload_files_concurrently(service, files, parent_folder_id=None):
    """Upload multiple files concurrently using threading."""
    threads = []

    for file_path in files:
        thread = threading.Thread(target=upload_file, args=(service, file_path, parent_folder_id))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    # Authenticate and get the Google Drive service object
    drive_service = authenticate_google_drive()

    # Define the folder containing the files to upload
    source_folder = '/path/to/source_folder'
    
    # List the files to upload
    files_to_upload = [str(file) for file in Path(source_folder).rglob('*') if file.is_file()]

    # Upload the files concurrently
    upload_files_concurrently(drive_service, files_to_upload)


