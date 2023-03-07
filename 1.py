'''A straightforward job:

Read from a PostgreSQL database when run. If data are present, generate a csv file. 
login to eversign, upload then submit the csv file.  
Update the database.
If the status is in_progress, check if the document has been completed. If completed, download the pdf 
and store in the database.
If no new data are present, end.'''


import psycopg2
import csv
import requests

# PostgreSQL database connection parameters
pg_host = "localhost"
pg_port = "5432"
pg_dbname = "database_name"
pg_user = "username"
pg_password = "password"

# eversign API credentials
eversign_api_key = "your_api_key"
eversign_api_secret = "your_api_secret"

# Connect to PostgreSQL database
conn = psycopg2.connect(host=pg_host, port=pg_port, dbname=pg_dbname, user=pg_user, password=pg_password)
cur = conn.cursor()

# Check if there is new data to process
cur.execute("SELECT COUNT(*) FROM table_name WHERE status = 'new'")
new_data_count = cur.fetchone()[0]
if new_data_count == 0:
    print("No new data to process")
    exit()

# Generate CSV file with new data
cur.execute("SELECT * FROM table_name WHERE status = 'new'")
rows = cur.fetchall()
filename = "data.csv"
with open(filename, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["column1", "column2", "column3"])  # Replace with actual column names
    for row in rows:
        writer.writerow(row)

# Upload CSV file to eversign
url = "https://api.eversign.com/api/document"
headers = {
    "Authorization": f"Basic {eversign_api_key}:{eversign_api_secret}"
}
data = {
    "document_name": "New document",
    "file": open(filename, "rb")
}
response = requests.post(url, headers=headers, files=data)
if response.status_code != 200:
    print("Error uploading CSV file to eversign")
    exit()

# Submit the uploaded document for signature
document_hash = response.json()["document_hash"]
url = f"https://api.eversign.com/api/document/{document_hash}/invite"
data = {
    "type": "send_document",
    "client": {
        "email_address": "client@example.com",
        "name": "Client name"
    }
}
response = requests.post(url, headers=headers, json=data)
if response.status_code != 200:
    print("Error submitting document for signature")
    exit()

# Update database with document hash and status
cur.execute("UPDATE table_name SET document_hash = %s, status = 'in_progress' WHERE status = 'new'", (document_hash,))
conn.commit()

# Check if the document has been completed
cur.execute("SELECT document_hash FROM table_name WHERE status = 'in_progress'")
rows = cur.fetchall()
for row in rows:
    document_hash = row[0]
    url = f"https://api.eversign.com/api/document/{document_hash}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Error checking document status")
        continue
    document_status = response.json()["status"]
    if document_status == "completed":
        # Download the completed document and store in the database
        url = f"https://api.eversign.com/api/document/{document_hash}/download"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print("Error downloading completed document")
            continue
        completed_document = response.content  # Replace with actual handling of the downloaded document
        cur.execute("UPDATE table_name SET status = 'completed' WHERE document_hash = %s", (document_hash,))
        conn.commit()

# Close database connection
cur.close()
conn.close()
