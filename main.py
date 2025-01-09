import requests
import psycopg2
from psycopg2 import sql
from sshtunnel import SSHTunnelForwarder
import json
from datetime import datetime

# Document class to represent each document
class Document:
    documents = []

    def __init__(self, id, name, requester_email, status, recipients, created_at, updated_at):
        self.id = id
        self.name = name
        self.requester_email = requester_email
        self.status = status
        self.recipients = recipients  # List of recipient details (id, name, date)
        self.created_at = created_at
        self.updated_at = updated_at
        Document.documents.append(self)

    @staticmethod
    def get_all_documents():
        return Document.documents

# Function to load configuration from a JSON file
def load_config(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)

# Load server configuration
config_file = 'config.json'  # Replace with your actual configuration file path
server_config = load_config(config_file)

# API endpoint and headers
url = "https://www.signwell.com/api/v1/documents/"
headers = {
    "accept": "application/json",
    "X-Api-Key": "YWNjZXNzOmE0MWU2NmZhMWU4NDdmOTIyNDYzNTQ0ZjdiYjljZGNj"
}

current_page = 1
all_documents = []

while current_page:
    # Make the API request for the current page
    params = {"page": current_page}  # Add the current page as a query parameter
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Append documents from the current page to the list
        all_documents.extend(data.get("documents", []))

        # Check for the next page
        current_page = data.get("next_page")
    else:
        # Exit the loop if the request fails
        break

for doc in all_documents:
    id = doc.get('id')
    name = doc.get('name')
    requester_email = doc.get('requester_email_address')
    status = doc.get('status')

    # Process recipients to gather details
    recipients = []
    for recipient in doc.get("recipients", []):
        recipient_id = recipient.get("id")
        recipient_name = recipient.get("name")
        recipient_status = recipient.get("status")
        recipient_date = None

        # Find the date for the recipient if available
        for field_group in doc.get('fields', []):
            for field in field_group:
                if field.get("recipient_id") == recipient_id and field["type"] == "date":
                    recipient_date = field.get("value")

        recipients.append({
            "name": recipient_name,
            "date": recipient_date,
            "status": recipient_status
        })

    # # Process recipients to gather details
    # recipients = []
    # for recipient in doc.get("recipients", []):
    #     recipient_id = recipient.get("id")
    #     recipient_name = recipient.get("name")
    #     recipient_date = None

    #     # Find the date for the recipient if available
    #     for field_group in doc.get('fields', []):
    #         for field in field_group:
    #             if field.get("recipient_id") == recipient_id and field["type"] == "date":
    #                 recipient_date = field.get("value")

    #     recipients.append({
    #         "name": recipient_name,
    #         "date": recipient_date
    #     })
        # # Find the date for the recipient if available
        # for field_group in doc.get('fields', []):
        #     for field in field_group:
        #         if field.get("recipient_id") == recipient_id and field["type"] == "date":
        #             try:
        #                 print()
        #                 recipient_date = datetime.strptime(field.get("value"), "%Y-%m-%d").date()
        #             except (ValueError, TypeError):
        #                 recipient_date = None

        # recipients.append({
        #     "name": recipient_name,
        #     "date": recipient_date
        # })

    created_at = doc.get('created_at')
    updated_at = doc.get('updated_at')

    # Create Document instances
    Document(
        id,
        name,
        requester_email,
        status,
        recipients,
        created_at,
        updated_at
    )

# SSH and PostgreSQL connection details
ssh_host = server_config['ssh']['host']
ssh_port = server_config['ssh']['port']
ssh_user = server_config['ssh']['user']
ssh_password = server_config['ssh']['password']

pg_host = server_config['database']['host']
pg_port = server_config['database']['port']
pg_user = server_config['database']['user']
pg_password = server_config['database']['password']
pg_name = server_config['database']['name']

# Set up the SSH tunnel
with SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_user,
    ssh_password=ssh_password,
    remote_bind_address=(pg_host, pg_port)
) as tunnel:
    try:
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(
            host='127.0.0.1',  # Localhost because of SSH tunneling
            port=tunnel.local_bind_port,
            user=pg_user,
            password=pg_password,
            database=pg_name
        )
        print("Database connection established.")

        cursor = connection.cursor()

        # Drop existing tables if they exist
        cursor.execute("DROP TABLE IF EXISTS recipients")
        cursor.execute("DROP TABLE IF EXISTS documents")

        # Create the documents table
        cursor.execute('''
        CREATE TABLE documents (
            id TEXT PRIMARY KEY,
            name TEXT,
            requester_email TEXT,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        ''')

        # Create the recipients table
        cursor.execute('''
        CREATE TABLE recipients (
            recipient_id SERIAL PRIMARY KEY,
            document_id TEXT,
            name TEXT,
            status TEXT,
            date TIMESTAMP,
            FOREIGN KEY (document_id) REFERENCES documents(id)
        )
        ''')

        # Insert data into the documents and recipients tables
        for doc in Document.get_all_documents():
            # print((doc.id, doc.name, doc.requester_email, doc.status, type(doc.created_at), type(doc.updated_at)))
            # Insert the document
            cursor.execute(
                '''INSERT INTO documents (id, name, requester_email, status, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s, %s)''',
                (doc.id, doc.name, doc.requester_email, doc.status, doc.created_at, doc.updated_at)
            )

            # Insert recipients for the document
            for recipient in doc.recipients:
                print(recipient)
                # print((doc.id, recipient["name"], recipient["date"]))
                cursor.execute(
                    '''INSERT INTO recipients (document_id, name, status, date) 
                    VALUES (%s, %s, %s, %s)''',
                    (doc.id, recipient["name"], recipient["status"], datetime.strptime(recipient["date"], "%d/%m/%Y").date() if recipient["date"] else None)
                    # (doc.id, recipient["name"], recipient["date"])
                )

        # Commit changes
        connection.commit()
        print("Data successfully saved to the PostgreSQL database.")

    except Exception as e:
        print(f"Database operation error: {e}")
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")

# import requests
# import psycopg2
# from psycopg2 import sql
# from sshtunnel import SSHTunnelForwarder
# import json

# # Document class to represent each document
# class Document:
#     documents = []

#     def __init__(self, id, name, requester_email, status, recipients, created_at, updated_at):
#         self.id = id
#         self.name = name
#         self.requester_email = requester_email
#         self.status = status
#         self.recipients = recipients  # List of recipient details (id, name, date)
#         self.created_at = created_at
#         self.updated_at = updated_at
#         Document.documents.append(self)

#     @staticmethod
#     def get_all_documents():
#         return Document.documents

# # Function to load configuration from a JSON file
# def load_config(config_file):
#     with open(config_file, 'r') as f:
#         return json.load(f)

# # Load server configuration
# config_file = 'config.json'  # Replace with your actual configuration file path
# server_config = load_config(config_file)

# # API endpoint and headers
# url = "https://www.signwell.com/api/v1/documents/"
# headers = {
#     "accept": "application/json",
#     "X-Api-Key": "YWNjZXNzOmE0MWU2NmZhMWU4NDdmOTIyNDYzNTQ0ZjdiYjljZGNj"
# }

# current_page = 1
# all_documents = []

# while current_page:
#     # Make the API request for the current page
#     params = {"page": current_page}  # Add the current page as a query parameter
#     response = requests.get(url, headers=headers, params=params)

#     if response.status_code == 200:
#         # Parse the JSON response
#         data = response.json()

#         # Append documents from the current page to the list
#         all_documents.extend(data.get("documents", []))

#         # Check for the next page
#         current_page = data.get("next_page")
#     else:
#         # Exit the loop if the request fails
#         break

# for doc in all_documents:
#     id = doc.get('id')
#     name = doc.get('name')
#     requester_email = doc.get('requester_email_address')
#     status = doc.get('status')

#     # Process recipients to gather details
#     recipients = []
#     for recipient in doc.get("recipients", []):
#         recipient_id = recipient.get("id")
#         recipient_name = recipient.get("name")
#         recipient_date = None

#         # Find the date for the recipient if available
#         for field_group in doc.get('fields', []):
#             for field in field_group:
#                 if field.get("recipient_id") == recipient_id and field["type"] == "date":
#                     recipient_date = field.get("value")

#         recipients.append({
#             "name": recipient_name,
#             "date": recipient_date
#         })

#     created_at = doc.get('created_at')
#     updated_at = doc.get('updated_at')

#     # Create Document instances
#     Document(
#         id,
#         name,
#         requester_email,
#         status,
#         recipients,
#         created_at,
#         updated_at
#     )

# # SSH and PostgreSQL connection details
# ssh_host = server_config['ssh']['host']
# ssh_port = server_config['ssh']['port']
# ssh_user = server_config['ssh']['user']
# ssh_password = server_config['ssh']['password']

# pg_host = server_config['database']['host']
# pg_port = server_config['database']['port']
# pg_user = server_config['database']['user']
# pg_password = server_config['database']['password']
# pg_name = server_config['database']['name']

# # Set up the SSH tunnel
# with SSHTunnelForwarder(
#     (ssh_host, ssh_port),
#     ssh_username=ssh_user,
#     ssh_password=ssh_password,
#     remote_bind_address=(pg_host, pg_port)
# ) as tunnel:
#     try:
#         # Establish a connection to the PostgreSQL database
#         connection = psycopg2.connect(
#             host='127.0.0.1',  # Localhost because of SSH tunneling
#             port=tunnel.local_bind_port,
#             user=pg_user,
#             password=pg_password,
#             database=pg_name
#         )
#         print("Database connection established.")

#         cursor = connection.cursor()

#         # Drop existing tables if they exist
#         cursor.execute("DROP TABLE IF EXISTS recipients")
#         cursor.execute("DROP TABLE IF EXISTS documents")

#         # Create the documents table
#         cursor.execute('''
#         CREATE TABLE documents (
#             id TEXT PRIMARY KEY,
#             name TEXT,
#             requester_email TEXT,
#             status TEXT,
#             created_at TIMESTAMP,
#             updated_at TIMESTAMP
#         )
#         ''')

#         # Create the recipients table
#         cursor.execute('''
#         CREATE TABLE recipients (
#             recipient_id SERIAL PRIMARY KEY,
#             document_id TEXT,
#             name TEXT,
#             date DATE,
#             FOREIGN KEY (document_id) REFERENCES documents(id)
#         )
#         ''')

#         # Insert data into the documents and recipients tables
#         for doc in Document.get_all_documents():
#             print(doc.id, doc.name, doc.requester_email, doc.status, doc.created_at, doc.updated_at)
#             # Insert the document
#             cursor.execute(
#                 '''INSERT INTO documents (id, name, requester_email, status, created_at, updated_at) 
#                 VALUES (%s, %s, %s, %s, %s, %s)''',
#                 (doc.id, doc.name, doc.requester_email, doc.status, doc.created_at, doc.updated_at)
#             )

#             # Insert recipients for the document
#             for recipient in doc.recipients:
#                 cursor.execute(
#                     '''INSERT INTO recipients (document_id, name, date) 
#                     VALUES (%s, %s, %s)''',
#                     (doc.id, recipient["name"], recipient["date"])
#                 )

#         # Commit changes
#         connection.commit()
#         print("Data successfully saved to the PostgreSQL database.")

#     except Exception as e:
#         print(f"Database operation error: {e}")
#     finally:
#         # Close the cursor and connection
#         if connection:
#             cursor.close()
#             connection.close()
#             print("Database connection closed.")
