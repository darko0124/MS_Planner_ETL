import msal
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import numpy as np


def read_azure_config(config_file_path_azure: str):
    try:
        with open(config_file_path_azure, 'r') as config_file:
            config_lines = config_file.read().splitlines()

        if len(config_lines) != 4:
            raise Exception("Invalid configuration file format. There should be exactly 4 lines.")

        return (
            config_lines[0].strip(),
            config_lines[1].strip(),
            config_lines[2].strip(),
            config_lines[3].strip()
        )
    except FileNotFoundError:
        raise Exception(f"Configuration file '{config_file_path_azure}' not found.")

def read_db_config(config_file_path: str):
    try:
        with open(config_file_path, 'r') as config_file:
            config_lines = config_file.read().splitlines()

        if len(config_lines) != 5:
            raise Exception("Invalid configuration file format. There should be exactly 5 lines.")

        return (
            config_lines[0].strip(),
            config_lines[1].strip(),
            config_lines[2].strip(),
            int(config_lines[3].strip()),
            config_lines[4].strip()
        )
    except FileNotFoundError:
        raise Exception(f"Configuration file '{config_file_path}' not found.")

def initialize_azure_ad_app(CLIENT_ID, CLIENT_SECRET, AUTHORITY, SCOPES):
    # Initialize your Azure AD app
    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=AUTHORITY
    )

    # Acquire token for Microsoft Azure
    result = app.acquire_token_for_client(SCOPES)

    return app, result

def insert_data_to_table(df, table_name, schema_name, engine):
    try:
        with engine.connect() as connection:
            df.to_sql(table_name, engine, if_exists="replace", index=False, schema=schema_name)
            print(f"Data inserted successfully for table {table_name}!")
    except SQLAlchemyError as error:
        print(f"Error inserting data into table: {error}")

def fetch_planner_data(app, result, PLAN_ID):
    if 'access_token' in result:
        access_token = result['access_token']

        # Define the Planner tasks endpoint for the specified plan
        planner_endpoint = f'https://graph.microsoft.com/v1.0/planner/plans/{PLAN_ID}/tasks'

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        # Make a request to retrieve Planner tasks
        response = requests.get(planner_endpoint, headers=headers)
        response.raise_for_status()
        planner_tasks = response.json()
        tasks_normalized = pd.json_normalize(planner_tasks['value'])
        return tasks_normalized
    else:
        raise Exception('Failed to obtain access token')
    
def fetch_planner_buckets(app, result, PLAN_ID):
    if 'access_token' in result:
        access_token = result['access_token']

        # Define the Planner buckets endpoint for the specified plan
        planner_endpoint = f'https://graph.microsoft.com/v1.0/planner/plans/{PLAN_ID}/buckets'

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        # Make a request to retrieve Planner buckets
        response = requests.get(planner_endpoint, headers=headers)
        response.raise_for_status()
        planner_buckets = response.json()

        # Create a dictionary to map bucket IDs to names
        bucket_name_mapping = {bucket['id']: bucket['name'] for bucket in planner_buckets['value']}

        return bucket_name_mapping
    else:
        raise Exception('Failed to obtain access token for fetching buckets')

def process_planner_data(tasks_normalized):

    # Normalize the JSON response into a DataFrame
    df_normalized = pd.DataFrame(tasks_normalized)

    # Remove leading and trailing spaces from column names
    df_normalized.columns = df_normalized.columns.str.strip()

    # Remove leading and trailing spaces from all values in the DataFrame
    df_normalized = df_normalized.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Rename columns (change the transformed names with underscores)
    df_normalized = df_normalized.rename(columns={
        '@odata.etag': 'etag',
        'createdBy.user.displayName': 'Created By User Name',
        'planId':'ID Of Corresponding Plan',
        'id':'Task ID',
        'bucketId':'ID Of Corresponding Bucket',
        'title' :'Task Name',
        'createdBy.user.id': 'Created By User ID',
        'percentCompleted':'Percent of completion',
        'startDateTime' : 'Start Date',
        'createdDateTime':'Date and Time of Task Creation',
        'dueDateTime':'Deadline of the Task',
        'completedDateTime':'Date and Time of Task Completion',
        'createdBy.application.displayName': 'Created By Application Name',
        'createdBy.application.id': 'Created By Application ID',
        'assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.@odata.type': 'Assignment Type',
        'assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedDateTime': 'Assignment Date and Time',
        'assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.orderHint': 'Assignment Priority',
        'assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.user.displayName': 'Assigned By User Name',
        'assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.user.id': 'Assigned By User ID',
        'assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.application.displayName': 'Assigned By Application Name',
        'assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.application.id': 'Assigned By Application ID',
        'appliedCategories.category5': 'Category 5 Applied',
        'appliedCategories.category1': 'Category 1 Applied',
        'conversationThreadId': 'Conversation Thread ID',
        'appliedCategories.category15':'Requested By TBI Bank',
        'appliedCategories.category9': 'Requested By DE',
        'appliedCategories.category11': 'Requested By Countour Global',
        'appliedCategories.category4': 'Requested By BA',
        'appliedCategories.category2': 'Requested By Takeda',
        'appliedCategories.category21' : 'Requested By AC Compressor',
        'appliedCategories.category3' : 'Requested By PBI',
        'appliedCategories.category7': 'Requested By FTE',
        'appliedCategories.category10': 'Requested By PM',
        'appliedCategories.category19': 'Requested By AC Vacuum Technique',
        'appliedCategories.category1': 'Awating approval',
        'appliedCategories.category5': 'Awating review',
        'completedBy.user.displayName':'Completed By User Name',
        'completedBy.user.id': 'Completed By User ID',
        'completedBy.application.id': 'Completed By Application ID',
        'completedBy.application.displayName': 'Completed By Application Name',
    })

    # Unnest the regular df(Remove the dicts from the df)
    df_normalized_without_columnname_transf = pd.DataFrame(tasks_normalized)
    df_normalized_without_columnname_transf.rename(columns={
        "assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.@odata.type": "@odata_type",
        "assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedDateTime": "assignedDateTime",
        "assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.orderHint": "orderhint",
        "assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.user.displayName": "assignedBy_user_displayName",
        "assignments.2a5a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.user.id": "assignedBy_user_id",
        "assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.application.displayName": "assignedBy_application_displayName",
        "assignments.2a5a7a21-1cae-4c94-8bf3-754c7cf983c7.assignedBy.application.id": "assignedBy_application_id"
    }, inplace=True)

    df_normalized_without_columnname_transf.to_csv("C://VS Code Projects//Microsoft_Planner_Connection//Df_normalized_without_columnname_transf.csv")

  
    # Split the 'createdDateTime' column into 'Date_of_Creation' and 'Time_of_Creation'
    df_normalized[['Date_of_Creation', 'Time_of_Creation']] = df_normalized['Date and Time of Task Creation'].str.split('T', expand=True)

    # Update the 'Time_of_Creation' column to remove milliseconds
    df_normalized['Time_of_Creation'] = df_normalized['Time_of_Creation'].str.split('.').str[0]

    # Rename the columns
    df_normalized = df_normalized.rename(columns={'Date_of_Creation': 'Date of Creation', 'Time_of_Creation': 'Time of Creation'})
    # Convert the "Start Date" and "Deadline of the Task" columns to datetime objects
    df_normalized['Start Date'] = pd.to_datetime(df_normalized['Start Date'], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
    df_normalized['Start Date of the Task'] = df_normalized['Start Date'].dt.date
    df_normalized['Deadline of the Task'] = pd.to_datetime(df_normalized['Deadline of the Task'], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
    

    # Filter or handle invalid datetime values (NaT) if necessary
    invalid_start_date = df_normalized['Start Date of the Task'].isna()
    invalid_deadline = df_normalized['Deadline of the Task'].isna()

    # Now you can drop the 'Date and Time of Task Creation' column
    df_normalized = df_normalized.drop(['Date and Time of Task Creation', 'Start Date'], axis=1)

    #Add a new column to the df that is calculated from other two columns (Priority column)
    conditions = [
        (df_normalized['priority'] == 5) & (df_normalized['activeChecklistItemCount'] == 0),
        (df_normalized['priority'] == 5) & (df_normalized['activeChecklistItemCount'] != 0),
        (df_normalized['priority'] == 3),
        (df_normalized['priority'].isin([2, 1]))
        ]   
    choices = ['Not a priority really', 'High priority', 'Mid priority', 'Low priority']
    df_normalized['Priority_Rank'] = np.select(conditions, choices, default='Unknown')


    # Specify the folder path and the CSV file name for df_with_JSON
    folder_path = "C://VS Code Projects//Microsoft_Planner_Connection"
    file_name = "CSV_JSON_NORMAL.csv"
    file_path = folder_path + "/" + file_name

    # Extract the DataFrame as CSV to your local computer
    df_normalized.to_csv(file_path, index=False)

    return df_normalized, df_normalized_without_columnname_transf

#Split the big table into two other tables. One for the tasks and one for the tags, and connect them with keys


def main():
    DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME = read_db_config(f"C:\\VS Code Projects\\Microsoft_Planner_Connection\\DB_Config.txt")

    if not all([DATABASE_USER, DATABASE_PASSWORD, DATABASE_HOST, DATABASE_PORT, DATABASE_NAME]):
        print("Database configuration values are missing.")
        return

    CLIENT_ID, CLIENT_SECRET, TENANT_ID, PLAN_ID = read_azure_config(f"C:\\VS Code Projects\\Microsoft_Planner_Connection\\Azure_config.txt")

    if not all([CLIENT_ID, CLIENT_SECRET, TENANT_ID, PLAN_ID]):
        print("Azure details values are missing")
        return

    AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'

    SCOPES = [
        'https://graph.microsoft.com/.default'
    ]

    DATABASE_URI = f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
    engine = create_engine(DATABASE_URI)
    app, token_result = initialize_azure_ad_app(CLIENT_ID, CLIENT_SECRET, AUTHORITY, SCOPES)

    if "access_token" in token_result:
        token = token_result["access_token"]
        print(f"Token acquired successfully: {token}")
    else:
        print(f"Error acquiring token: {token_result.get('error')} - {token_result.get('error_description')}")

    tasks_normalized = fetch_planner_data(app, token_result, PLAN_ID)

    df_normalized, df_normalized_without_columnname_transf = process_planner_data(tasks_normalized)

    TABLE_NAME = 'mario_tasks'
    TABLE_NAME2 = 'mario_tasks_raw'
    SCHEMA_NAME = 'Planner_test'

    # Fetch bucket names and map them to task data
    bucket_name_mapping = fetch_planner_buckets(app, token_result, PLAN_ID)
    df_normalized['Bucket Name'] = df_normalized['ID Of Corresponding Bucket'].map(bucket_name_mapping)
    
    
    # Insert the first DataFrame into its table
    insert_data_to_table(df_normalized, TABLE_NAME, SCHEMA_NAME, engine)

    # Insert the second DataFrame into its table
    insert_data_to_table(df_normalized_without_columnname_transf, TABLE_NAME2, SCHEMA_NAME, engine)

if __name__ == "__main__":
    main()
