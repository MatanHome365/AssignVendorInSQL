'''
-------------------------------------------------------------------------------------------------------------------------
Assign Vendors in SQL by Auto ML - LAST UPDATE: 2022-02-16

This script assigns service provider to a new project when the following conditions are met:
- Project was created by tenant
- Project was yet to be assigned to service provider
- Auto ML process successfully identifies category with confidence of more than 80%
- A valid service provider was found

Please note that throughout the script, we call several external APIs.
Any failure can be found in CloudWatch / Kibana.

-------------------------------------------------------------------------------------------------------------------------
'''

# Import the required libraries
import pyodbc
import psycopg2

import requests
import boto3
import pandas as pd
import json

from datetime import datetime, timezone, date

from urllib.parse import urlencode

today = datetime.now(timezone.utc)
import os
import botocore

import VendorRadius as vr
import db_connections as db

# Initialize Variables
client = boto3.client('s3', 'us-east-1', config=botocore.config.Config(s3={'addressing_style': 'path'}))
BUCKET = os.environ['BUCKET']
server = os.environ['datasource']
database = os.environ['database']
port = 3306
user = os.environ['user']
password = os.environ['password']
send_mail_queue_url = os.environ['send_mail_queue_url']
sqs_client = boto3.client('sqs')
HOST = os.environ['host_data']
DBNAME = os.environ['database_data']
USER = os.environ['user_data']
PASSWORD = os.environ['password_data']
is_test = os.environ['is_test']
URL = os.environ['URL']
keywords_url = os.environ['keywords_url']
emer_url = os.environ['emer_url']
is_keywords = os.environ['is_keywords']
is_emergency = os.environ['is_emergency']
not_assigned_statuses = ['NEW_PROJECT', 'AWAITING_VENDOR_ASSIGNMENT']


def logObject(comment, function, subject, service='AWS Lambda', app_name='AssignVendorInSQL'):
    """Gets metadata about the code and return JSON object that will be used for Kibana

    Parameters
    ----------
    comment : str
        Comment to print out
    function : str
        Name of the current function
    subject: str
        A general subject for the current log
    service: str, opt
        AWS service (AWS Lambda)
    app_name: str, opt
        Application name (AssignVendorInSql)

    Returns
    -------
    json
        a summary of the log
    """

    return json.dumps({
        'SERVICE': service,
        'APP': app_name,
        'SUBJECT': subject,
        'FUNCTION': function,
        'COMMENT': comment
    })


def lambda_handler(event, lambda_context):
    """Gets the source event and wrap the process

    Parameters
    ----------
    event : dict
        details of the new event (project data in S3)

    """

    # print(logObject('[INFO]: Starting Lambda Function', 'lambda_handler', 'Start Lambda'))
    print(logObject('[INFO]: Project source key : %s' % event['source_key'], 'lambda_handler', 'Start Lambda'))
    assign_vendor(event['source_key'], event['text'])
    print(logObject('[INFO]: Complete Lambda Process for project source key : %s' % event['source_key'],
                    'lambda_handler', 'Complete Lambda'))


class Video:
    ''' Video class will be used to store the relevant data of the new video '''

    def __init__(self, date, key, folder, predictions):
        self.date = date
        self.key = key
        self.folder = folder
        self.created_on = today.date()
        self.json_predictions = predictions
        self.category = None
        self.vendor = None
        self.incident_id = None
        self.location_id = None
        self.project_number = None
        self.address = None
        self.video_url = None


def check_plan(plan):
    """Gets property plan id and returns a dataframe with the property details if the property is active and has no home warranty

    Parameters
    ----------
    plan : str
        property plan id

    Returns
    -------
    dataframe
        table with property details
    """

    query = '''
        select distinct pr.presented_address, p.property_plan_id, pr.property_additional_info, pr.pm_id location_id, lr.display_name "location"
    from "Plans" P
             inner join "Properties" pr on pr.property_id=p.property_id
    inner join "Location_Rules" lr on lr.pm_account_id=pr.pm_id
    where  (home_warranty = 'null' or home_warranty is null) and pr.active=1 and p.property_plan_id='{}'
    '''.format(plan)
    return db.importDataFromPG(query)


def assign_vendor(key, text, confidence=0.7, multi_thresh=10, gap_thresh=0.3):
    """Gets video key and run the entire process of vendor assignment if all conditions are met

    Parameters
    ----------
    key : str
        video key

    confidence : float, opt
        threshold for category confidence

    Returns
    -------
    Video : object
        video object with the required details
    """

    print(text)
    print(logObject('[INFO]: Trying to Assign Vendor, get prediction details...', 'assign_vendor', 'Get Predictions'))
    BUCKET = os.environ['BUCKET']
    try:
        # Try to pull out the prediction results from S3
        prediction_response = client.get_object(Bucket=BUCKET, Key=key + '.prediction')
        print(logObject('[INFO]: Predictions Were Found!', 'assign_vendor', 'Get Predictions'))
        prediction = eval(prediction_response["Body"].read().decode())

        # Create a hew video object with the prediction results
        new_video = Video(prediction_response['LastModified'], key, key.split('.')[-1], prediction)
    except Exception as e:
        print(e)
        return 'Not Found'

    # Pull out the project id based on the video source key
    results = get_incident_id(key)

    # return None if project id was not found
    if results is None:
        return None

    if is_keywords == 'True':
        # calling keywords api
        project_id = results['incident_id'].lower()
        api_data = {'text': text, 'project_id': project_id}
        headers = {'Content-Type': 'application/json'}
        res_keywords = requests.post(url=keywords_url, data=json.dumps(api_data))
        print(res_keywords.text)

    if is_emergency == 'True':
        # calling emergency api
        res_emer = requests.post(url=emer_url, data=json.dumps(api_data))
        print(res_emer.text)

    # Update Video object
    print(logObject('[INFO]: Video: %s ' % results['video_url'], 'assign_vendor', 'Get Video'))
    new_video.incident_id = results['incident_id']
    new_video.location_id = results['location_id']
    new_video.project_number = results['project_number']
    new_video.address = results['address']
    new_video.video_url = results['video_url']

    # In the next batch, we check if the model found a valid category with confidence above 70%
    list_of_predictions = list(new_video.json_predictions['probabilites'].items())
    if new_video.json_predictions['probabilites'][new_video.json_predictions['best']] >= confidence:
        # if new_video.json_predictions['probabilites'][new_video.json_predictions['best']] >= confidence or list_of_predictions[0][1] >= list_of_predictions[1][1] * multi_thresh or list_of_predictions[0][1] - gap_thresh >= list_of_predictions[1][1]:
        new_video.category = new_video.json_predictions['best']
        # new_video.category = 'Plumber'  # DEV
        new_video.category = pattern_replace_for_API(new_video.category)
        print(logObject('[INFO]: A valid prediction has been found: %s' % (new_video.category), 'assign_vendor',
                        'Found Category'))

        # Try to find the best available vendor
        try:
            data = {"category": new_video.category,
                    "location_id": new_video.location_id,
                    "incident_id": new_video.incident_id
                    }
            print('[INFO]: Invoking vendor assignment API to get vendors')
            # URL = 'https://q23qxjvdl4.execute-api.us-east-1.amazonaws.com/vendors_assignment/cat'
            # Listing possible vendors
            data["possible_vendors"] = list(find_vendors(data))
            print(data)
            headers = {'Content-Type': 'application/json'}
            API_response = requests.post(url=URL, data=json.dumps(data))
            print(API_response)
            if API_response is not None:
                API_response = API_response.json()

            print(API_response)
            print(len(API_response))

            # Select the best available vendor
            # new_video.vendor = API_response.json()['1']

            # if os.environ['flag_test'] == 'True':
            #     temp_json = {'Vendor': 'QA Vendor', 'Vendor_ID': '7A2D4656-2012-4E4B-AD0A-E273F1B4EDE8',
            #                  'Weighted Average': 5.0, 'Completed Projects': 5, 'Average Vendor Cost': 170.0,
            #                  'Email': 'tal@home365.co',
            #                  'Category ID': '6C9C57B3-C246-4D4B-A4F4-AA69A3FF3486',
            #                  'Average Category Cost': 329.995267489712}
            #     new_video.vendor = temp_json
            # else:
            new_video.vendor = API_response['1']
            print('[INFO]: A valid Vendor has been found: %s' % new_video.vendor['Vendor'])
        except KeyError:
            print('[WARNING]: No similar vendors \n Done.')
            return 'Not Found'
    else:
        print('[INFO]: No prediction with more 70% confidence has been identified')
        return 'Not Found'
    new_video.category = pattern_replace_for_SQL(new_video.category)
    if is_test == 'False':
        assigned = update_project_type(new_video)
        if assigned == False:
            return None
        conn = psycopg2.connect(host=HOST, database=DBNAME, user=USER, password=PASSWORD)
        category_id = load_category_id(conn, new_video.category)[0].lower()

        assigned = assign_project_to_pro(new_video, category_id)
        if assigned == False:
            return None
        if assigned == True:
            # pass
            # update_paid_by(new_video.incident_id)
            update_auto_assignment(new_video)
            # send_email_to_vendor(new_video)
    return new_video


def assign_project_to_pro(new_video, category_id):
    user_id = 'F9966362-6EDB-4C87-805A-8ED66F70D54A'
    try:
        print('[INFO]: Assign Vendor in SQL and UPDATE Project Status')
        new_url = 'https://app-prod.home365.co/projects-service-prod/projects/assign-project-to-pro?userId=%s' % user_id
        headers = {
            'nptoken': 'A57AA3CC-F94B-4A1B-85A4-C360F7',
            'Content-Type': 'application/json'
        }

        data = {"categoryId": category_id,
                "proId": new_video.vendor['Vendor_ID'].lower(),
                "incidentId": new_video.incident_id.lower()
                }
        print(json.dumps(data))
        response = requests.post(new_url, headers=headers, data=json.dumps(data))
        print('[INFO]: Response Status Code: ' + str(response.status_code))
        print('[INFO]: Assignment Success')
        print('[INFO]: Logging Out from DB')

        print('[INFO]: Vendor %s has been assigned successfully.' % new_video.vendor['Vendor_ID'])
        if response.status_code != 200:
            print('ERROR: %s' % response.status_code)
            return False
        else:
            print('[INFO]: 200 - API CALL SUCCESS')
            return True
    except Exception as e:
        print('[ERROR]: There has been an error:')
        print(e)
        return False


def update_project_type(new_video,
                        user_id='F9966362-6EDB-4C87-805A-8ED66F70D54A',
                        project_type="b57b300e-f0ce-4f3f-b01d-872ac6c4d1d0",
                        change_reason='Auto ML Process'):
    print('[INFO]: Update Project Type')
    try:
        data = {"changeReason": change_reason, "projectTypeId": project_type}
        incident_id = new_video.incident_id

        new_url = 'https://app-prod.home365.co/projects-service-prod/projects/project/%s/projecttype?userId=%s' % (
            incident_id, user_id)

        headers = {
            'nptoken': 'A57AA3CC-F94B-4A1B-85A4-C360F7',
            'Content-Type': 'application/json'
        }

        response = requests.post(new_url, headers=headers, data=json.dumps(data))
        print('[INFO]: Response Status Code: ' + str(response.status_code))
        if response.status_code != 200:
            print('ERROR: %s' % response.status_code)
            return False
        else:
            print('[INFO]: 200 - API CALL SUCCESS')
            return True
    except Exception as e:
        print('[ERROR]: There has been an error:')
        print(e)
        return False


def pattern_replace_for_API(category):
    res = category
    res = res.replace('Pool/Hot Tub', 'Pool Hot Tub')
    res = res.replace('Appliance Installer / Repair', 'Appliance Installer Repair')
    res = res.replace('Garage Door Installer / Repair', 'Garage Door Installer Repair')
    return res


def pattern_replace_for_SQL(category):
    res = category
    res = res.replace('Pool Hot Tub', 'Pool/Hot Tub Installer /Repair')
    res = res.replace('Appliance Installer Repair', 'Appliance Installer / Repair')
    res = res.replace('Garage Door Installer Repair', 'Garage Door Installer / Repair')
    return res


def get_incident_id(key):
    print('[INFO]: Getting incident ID from SQL...')
    key2 = 'propertydoc_qa/' + key.split('/')[-1]

    query = f''' select
        upper(p.project_id::text) project_id,
               project_status,
               upper(property_plan_id::text) property_plan_id,
               project_number,
               p.project_status_string,
               pf.s3_url "video_url"
        from "Projects" p
        inner join "Project_Files" pf on pf.project_id = p.project_id
        where (pf.s3_url ilike '%%{key}%%' or pf.s3_url ilike '%%{key2}%%') and created_by_tenant=1
        order by date_created desc'''

    # '''.format('%%' + key + '%%')
    print(query)
    incident = db.importDataFromPG(query, host=server, database=database, user=user, password=password)
    # incident = db.ImportDataFromSql(query)
    results = None
    print('[INFO]: Checking if project was created by tenant')
    if len(incident) == 0:
        print('[WARNING]: Project was not found in DB.')
        #         print('[WARNING]: Project was not created by tenant or project was not found or tenant or property has Home Warranty. \n Done.')
        return results
    plan = incident['property_plan_id'].values[0]
    property_details = check_plan(plan)
    if len(property_details) == 0:
        print('[WARNING]: Property has Home Warranty or is not active')
        return results
    incident = pd.merge(incident, property_details, on='property_plan_id', how='left')
    if incident['property_additional_info'].values[0] is not None:
        jsonize = json.loads(incident['property_additional_info'].values[0])
        if jsonize['autoAssign'] == False:
            print('[INFO]: Auto assignment has been disabled for this property')
            return results
        else:
            print('[INFO]: Auto assignment has not been disabled')
    if incident.iloc[0]['project_status_string'] not in not_assigned_statuses:
        print('[WARNING]: Project was already assigned by PM.')
        return results
    else:
        print('[INFO]: Project %s (%s) was created by a tenant in %s which located at %s.' % (
            incident['project_id'].values[0], incident['project_number'].values[0],
            incident['presented_address'].values[0],
            incident['location'].values[0]))
        qp = {'video': incident['video_url'].values[0]}
        qp_to_url = urlencode(qp)
        qp_to_url = qp_to_url.replace('http', 'https')
        qp_to_url = qp_to_url.replace('httpss', 'https')
        results = {
            'incident_id': incident['project_id'].values[0],
            'location_id': incident['location_id'].values[0],
            'project_number': incident['project_number'].values[0],
            'address': incident['presented_address'].values[0],
            'video_url': 'https://app-prod.home365.co/videoVendor/?' + qp_to_url
        }
        print('[INFO]: final results:')
        print(results)
    return results


def update_auto_assignment(video):
    print('[INFO]: Update auto assignment audit in DB')
    sql = """update "AUDIT_VIDEOS_TO_S3" 
    set automatic_assignment = true
    where key like  '%s' """
    conn = None
    updated_rows = 0
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(host=HOST,
                                database=DBNAME, user=USER, password=PASSWORD)
        # create a new cursor
        cur = conn.cursor()
        print('[INFO]: New key is being updated:  %s' % video.key)
        # execute the UPDATE  statement
        cur.execute(sql % (video.key + '%'))
        print('[INFO]: SQL query Execution using psycopg2')
        # get the number of updated rows
        updated_rows = cur.rowcount
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def find_vendors(data):
    print("FINDING VENDORS")
    new_data = data
    new_data["category"] = new_data["category"].replace("%20", " ")
    new_data["category"] = new_data["category"].replace("Appliance Installer Repair", "Appliance Installer / Repair")
    new_data["category"] = new_data["category"].replace("Pool Hot Tub", "Pool/Hot Tub Installer /Repair")
    new_data["category"] = new_data["category"].replace("Garage Door Installer Repair",
                                                        "Garage Door Installer / Repair")
    conn = psycopg2.connect(host=HOST,
                            database=DBNAME, user=USER, password=PASSWORD)
    category_id = load_category_id(conn, new_data["category"])
    # category_id =['DB9ADA2F-081C-E111-B6BD-001517D1792A']
    possible_vendors = ''
    try:
        print(new_data)
        possible_vendors = vr.return_related_vendors(category_id[0], new_data["incident_id"])
        if possible_vendors is None:
            possible_vendors = ''
    except Exception as e:
        print('[Warning]: failed to call vendors api...')
        return None
    print(possible_vendors)
    return possible_vendors


def load_category_id(conn, category):
    print('[INFO]: Loading category id from DB')
    query = """select category_id from "Categories" C where name like '%s'""" % category
    cur = conn.cursor()
    cur.execute(query)
    category_id = cur.fetchall()
    print('[INFO]: Category ID: %s' % category_id[0])
    return category_id[0]


def send_email_to_vendor(new_video):
    # template = {"from":"support@home365.co","subject":"New Project Assigned","recipients":[{"name":"Eyal Eyal","email":"eyal@home365.co"}],"contentTemplate":{"VENDOR": new_video.vendor['Vendor'], "PROJECT_NUMBER":new_video.project_number,"ADDRESS":new_video.address, "LINK_URL": new_video.video_url},"templateName":"vendor-assignment-notification"}
    template = {"from": "support@home365.co", "subject": "Project assigned",
                "recipients": [{"name": new_video.vendor['Vendor'], "email": new_video.vendor['Email']}],
                "contentTemplate": {"PRO_NAME": new_video.vendor['Vendor'],
                                    "PROJECT_NUMBER": str(new_video.project_number), "PRO_CATEGORY": new_video.category,
                                    "ADDRESS": new_video.address, "CHAT_URL": new_video.video_url},
                "templateName": "Home - Pro - New Project"}
    json_to_sqs = json.dumps(template)
    print('[INFO]: Sending email notice to Vendor...')
    response = sqs_client.send_message(
        QueueUrl=send_mail_queue_url,
        MessageBody=json_to_sqs,
        DelaySeconds=0
    )
    print(f'[INFO]: {response}')


assign_vendor('tenants/eb61488e-b3bb-46c8-8964-0ccb62206265/projects/1708234445562/REC6275475664516882933', None)