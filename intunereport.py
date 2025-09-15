import requests
import time
import json
import gzip
import io
import csv
import os
import re

# --- Configuration ---
# TODO: Replace with your actual Azure App registration details.
TENANT_ID = "YOUR_TENANT_ID"
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"

# --- Constants ---
AUTH_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
API_BASE_URL = "https://graph.microsoft.com/beta"
EXPORT_JOB_URL = f"{API_BASE_URL}/deviceManagement/reports/exportJobs"
APPS_URL = f"{API_BASE_URL}/deviceAppManagement/mobileApps"
OUTPUT_FOLDER = "Intune_App_Reports"

def get_access_token():
    """
    Acquires an OAuth2 access token from Microsoft Entra ID.
    """
    print("Requesting access token...")
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    response = requests.post(AUTH_URL, data=token_data)
    response.raise_for_status()  # Will raise an exception for HTTP error codes
    print("Access token acquired successfully.")
    return response.json().get("access_token")

def get_all_apps(access_token):
    """
    Retrieves all mobile apps from Intune.
    """
    print("Fetching list of all applications...")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    apps = []
    url = f"{APPS_URL}?$select=id,displayName"

    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        for app in data.get("value", []):
            apps.append({
                "id": app.get("id"),
                "displayName": app.get("displayName")
            })
        
        url = data.get("@odata.nextLink") # For pagination

    print(f"Found {len(apps)} applications.")
    return apps

def sanitize_filename(filename):
    """
    Removes characters that are invalid for file names.
    """
    return re.sub(r'[\\/*?:"<>|]',"", filename)

def submit_export_job(access_token, application_id, application_name):
    """
    Submits a new report export job to the Microsoft Graph API for a specific app.
    """
    print(f"\nSubmitting report export job for: '{application_name}' (ID: {application_id})...")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "reportName": "DeviceInstallStatusByApp",
        "filter": f"(ApplicationId eq '{application_id}')",
        "select": [
            "DeviceName",
            "UserPrincipalName",
            "Platform",
            "InstallState",
            "InstallStateDetail",
            "LastSyncDateTime",
            "OSVersion",
            "OSDescription",
            "UserName"
        ],
        "format": "csv"
    }
    
    response = requests.post(EXPORT_JOB_URL, headers=headers, data=json.dumps(payload))
    
    # Successful submission returns a 202 Accepted status
    if response.status_code == 202:
        # The URL to poll for job status is in the 'Location' header
        status_url = response.headers.get("Location")
        print("Export job submitted successfully.")
        print(f"Polling status URL: {status_url}")
        return status_url
    else:
        print(f"Error submitting export job for '{application_name}': {response.status_code}")
        print(response.text)
        return None

def poll_for_completion(status_url, access_token):
    """
    Polls the status URL until the export job is completed.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    
    while True:
        response = requests.get(status_url, headers=headers)
        response.raise_for_status()
        job_details = response.json()
        
        status = job_details.get("status")
        print(f"Current job status: {status}...")
        
        if status == "completed":
            print("Export job completed!")
            return job_details.get("url")
        elif status in ["failed", "notSupported"]:
            print(f"Job failed or is not supported. Details: {job_details}")
            return None
            
        # Wait for 30 seconds before polling again
        time.sleep(30)

def download_and_save_report(download_url, access_token, output_file_name):
    """
    Downloads the compressed report data, decompresses it, and saves it to a CSV file.
    """
    print(f"Downloading report from: {download_url}")
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(download_url, headers=headers)
    response.raise_for_status()
    
    print("Decompressing report data...")
    try:
        # The content is gzipped. We need to decompress it in memory.
        gzip_file = io.BytesIO(response.content)
        with gzip.GzipFile(fileobj=gzip_file) as decompressed_file:
            # Decode the bytes to a string
            csv_content = decompressed_file.read().decode('utf-8-sig') # Use utf-8-sig to handle potential BOM
            
        print(f"Saving report to '{output_file_name}'...")
        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        
        # Write the content to a local CSV file inside the folder
        with open(os.path.join(OUTPUT_FOLDER, output_file_name), 'w', newline='', encoding='utf-8') as f:
            f.write(csv_content)
        
        print("Report saved successfully.")

    except Exception as e:
        print(f"An error occurred while processing the report file: {e}")


def main():
    """
    Main function to orchestrate the report fetching process for all apps.
    """
    if any(val.startswith("YOUR_") for val in [TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
        print("Error: Please update the configuration variables (TENANT_ID, CLIENT_ID, CLIENT_SECRET) at the top of the script.")
        return

    try:
        # 1. Get access token
        token = get_access_token()
        if not token:
            return

        # 2. Get all applications
        apps_to_process = get_all_apps(token)
        if not apps_to_process:
            print("No applications found to process.")
            return

        total_apps = len(apps_to_process)
        print(f"\nStarting report generation for {total_apps} applications...")
        
        for index, app in enumerate(apps_to_process):
            app_id = app.get("id")
            app_name = app.get("displayName")
            
            if not app_id or not app_name:
                print(f"Skipping an item due to missing ID or Name: {app}")
                continue

            print(f"--- Processing App {index + 1} of {total_apps} ---")

            # 3. Submit the export job for the current app
            job_status_url = submit_export_job(token, app_id, app_name)
            if not job_status_url:
                print(f"Skipping report for '{app_name}' due to submission failure.")
                continue
            
            # 4. Poll for job completion
            report_download_url = poll_for_completion(job_status_url, token)
            if not report_download_url:
                print(f"Skipping report for '{app_name}' due to job failure.")
                continue
            
            # 5. Download and save the final report
            safe_app_name = sanitize_filename(app_name)
            output_file = f"{safe_app_name}_{app_id}.csv"
            download_and_save_report(report_download_url, token, output_file)

            # Optional: Add a small delay to be considerate to the API
            time.sleep(5)

        print(f"\nProcess finished. All reports are saved in the '{OUTPUT_FOLDER}' directory.")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response Body: {http_err.response.text}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

