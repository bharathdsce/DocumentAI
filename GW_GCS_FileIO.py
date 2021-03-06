# The code is aimed towards uploading a batch of pdf files from local to GCS and if there are any duplicate files present in any of the batch
# the code should throw an exception
# If there are no duplicate files in a given batch that are already present in the GCS, the files will be uploaded to the new batch folder in the bucket
# The code can also be used to download multiple files as zip using a valid batch id
# The code also has an API to get the current number of files that are present in the GCS
# The entire opertaion is made possible using Flask API, GCS, CLoud SQL

from flask import Flask, jsonify, request, redirect, make_response, send_file
from functools import wraps
import uuid
from google.cloud import storage
import datetime
import sqlalchemy
import pandas as pd
import jwt
import platform
import os
import tempfile
import shutil

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024

# Add a new secret key for flask to work. Keep it confidential
app.config['SECRET_KEY'] = <secret_key>

# give the CLoud SQL Connection parameters
app.config["MYSQL_DATABASE_USER"] = <username>
app.config["MYSQL_DATABASE_PASSWORD"] = <password>
app.config["MYSQL_DATABASE_DB"] = <database>
app.config["MYSQL_DATABASE_HOST"] = <host_ip>
app.config["MYSQL_CONNECTION_NAME"] = <cloud sql_connection name>
app.config["INPUT_GCS_BUCKET"] = <gcs_bucket>

ALLOWED_EXTENSIONS = ['pdf']
platform_type = platform.system()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def create_db_conn():
    if platform_type == "Windows":
        sql_engine = sqlalchemy.create_engine("mysql+mysqldb://{user}:{pw}@{host}/{db}"
                                              .format(user=app.config["MYSQL_DATABASE_USER"],
                                                      pw=app.config["MYSQL_DATABASE_PASSWORD"],
                                                      host=app.config["MYSQL_DATABASE_HOST"],
                                                      db=app.config["MYSQL_DATABASE_DB"]))

    else:
        sql_engine = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL.create(
                drivername="mysql+pymysql",
                username=app.config["MYSQL_DATABASE_USER"],
                password=app.config["MYSQL_DATABASE_PASSWORD"],
                database=app.config["MYSQL_DATABASE_DB"],  # e.g. "my-database-name"
                query={
                    "unix_socket": "{}/{}".format(
                        "/cloudsql", app.config["MYSQL_CONNECTION_NAME"])
                }
            ),
        )
    return sql_engine

def duplicate_data_struct(db_data):
    data_dict = {}
    for data in db_data:
        temp_batch_id = data[0]
        temp_file_id = data[1]
        if temp_batch_id not in data_dict.keys():
            data_dict[temp_batch_id] = [temp_file_id]
        else:
            data_dict[temp_batch_id].append(temp_file_id)
    return data_dict

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get('token')
        if not token:
            return jsonify({'Message': 'Unauthorized',
                            "Description": "Token is missing"}), 401
        try:
            jwt.decode(token, app.config['SECRET_KEY'])
        except Exception as e:
            return jsonify({'message': 'Token is invalid.', "Error Description": str(e)}), 403
        return f(*args, **kwargs)
    return decorated


@app.route('/api/upload_gcs', methods=['POST'])
@token_required
def upload_file_gcs():
    if request.method == 'POST':
        if 'files' not in request.files:
            return redirect(request.url)

        files = request.files.getlist('files')
        batch_id = str(uuid.uuid1())

        storage_client = storage.Client()
        storage_bucket = storage_client.get_bucket(app.config["INPUT_GCS_BUCKET"])
        batch_id_list = []
        file_id_list = []
        status_id_list = []
        process_start_dttm_list = []
        process_end_dttm_list = []
        batch_start_dttm = datetime.datetime.now()
        file_count = 0
        file_names = []
        gcs_uri_list = []

        FactFileStatusTable = "FactFileStatus"
        FactFileInfoTable = "FactFileInfo"
        BatchStatusTable = "FactBatchStatus"
        BatchInfoTable = "FactBatchInfo"

        for file in files:
            file_names.append(file.filename)

        file_names_str = "'" + "','".join(file_names) + "'"
        db_conn = create_db_conn().connect()
        db_data = db_conn.execute(
            "select count(*) as file_num from {} where FileID in ({})".format(FactFileStatusTable, file_names_str))
        num_file_db = [row for row in db_data][0][0]
        db_conn.close()

        if num_file_db == 0:
            print("batch_id = {}".format(batch_id))
            file_names = []
            batch_id_uri = "gs://{}/".format(app.config["INPUT_GCS_BUCKET"])
            for file in files:
                file_count += 1
                if file and allowed_file(file.filename):
                    file_names.append(file.filename)
                    start_time = datetime.datetime.now()
                    blob = storage_bucket.blob(batch_id + "/" + file.filename)
                    blob.upload_from_string(file.read(), content_type=file.content_type)
                    end_time = datetime.datetime.now()

                    gcs_uri_list.append(batch_id_uri+blob.name)
                    batch_id_list.append(batch_id)
                    file_id_list.append(file.filename)
                    status_id_list.append(1)
                    process_start_dttm_list.append(start_time)
                    process_end_dttm_list.append(end_time)

                else:
                    return jsonify({'message': 'Invalid Filetype'}), 415

            upload_file_data = {"BatchID": batch_id_list,
                                "FileID": file_id_list,
                                "StatusID": status_id_list,
                                "ProcessStartDtTm": process_start_dttm_list,
                                "ProcessEndDtTm": [None]*file_count,
                                "LastModifiedDtTm": [datetime.datetime.now()]*file_count,
                                "LastModifiedBy": ["API"]*file_count,
                                "GcsUri": gcs_uri_list}

            upload_batch_status_data = {"BatchID": [batch_id],
                                        "StatusID": [1],
                                        "BatchStartDtTm": [batch_start_dttm],
                                        "BatchEndDtTm": [None],
                                        "TotalFiles": [file_count],
                                        "LastModifiedDtTm": [datetime.datetime.now()],
                                        "LastModifiedBy": ["API"],
                                        "GcsUri": [batch_id_uri+batch_id]}

            upload_batch_info_data = {"BatchID": [batch_id],
                                      "ProcessStartDtTm": [batch_start_dttm],
                                      "ProcessEndDtTm": [end_time],
                                      "StatusID": [1],
                                      "LastModifiedDtTm": [datetime.datetime.now()],
                                      "LastModifiedBy": ["API"],
                                      "Description": ["Upload Completed"]}

            fact_file_df = pd.DataFrame(data=upload_file_data)
            batch_status_df = pd.DataFrame(data=upload_batch_status_data)
            batch_info_df = pd.DataFrame(data=upload_batch_info_data)

            try:
                db_conn = create_db_conn().connect()
            except Exception as e:
                return jsonify({"Message": "Internal Server Error",
                                "Description": str(e)}), 500

            try:
                batch_status_df.to_sql(BatchStatusTable, db_conn, if_exists='append', index=False)
                fact_file_df.to_sql(FactFileStatusTable, db_conn, if_exists='append', index=False)
                fact_file_df["Description"] = ["Upload Completed"]*file_count
                fact_file_df["ProcessEndDtTm"] = process_end_dttm_list
                fact_file_df.drop(["GcsUri"], axis=1, inplace=True)
                fact_file_df.to_sql(FactFileInfoTable, db_conn, if_exists='append', index=False)
                batch_info_df.to_sql(BatchInfoTable, db_conn, if_exists='append', index=False)

            except Exception as e:
                print(e)
            else:
                print("Table %s loaded successfully." % FactFileStatusTable)
            finally:
                db_conn.close()

        else:
            file_names_str = "'"+"','".join(file_names)+"'"
            db_conn = create_db_conn().connect()
            db_data = db_conn.execute("select BatchID, FileID from {} where FileID in ({})".format(FactFileStatusTable, file_names_str))
            db_conn.close()
            # data_dict = ["Previous BatchID = {}, FIleID = {}".format(row[0], row[1]) for row in db_data]
            data_dict = duplicate_data_struct(db_data)

            return jsonify({"Exception": "Duplicate Files",
                            "Message": "Following files are already present in the system",
                            "Files": data_dict}), 409

        return jsonify({"status": "Success", "Batch ID": batch_id, "Files Uploaded": file_count}), 200

@app.route('/api/download_gcs', methods=['GET'])
@token_required
def download_file_gcs():
    batch_id = request.args["batch_id"]
    print("batch_id = ", batch_id)
    batch_status_tb = "FactBatchStatus"

    db_conn = create_db_conn().connect()
    db_data = db_conn.execute(
        "select count(*) as batch_num from {} where BatchID='{}' and StatusID<>3".format(batch_status_tb, batch_id))
    num_batch_db = [row for row in db_data][0][0]
    db_conn.close()

    if num_batch_db != 0:
        temp_folder1 = tempfile.mkdtemp()
        temp_folder2 = tempfile.mkdtemp()

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(app.config["INPUT_GCS_BUCKET"])
        blobs = bucket.list_blobs(prefix=batch_id)
        for blob in blobs:
            filename = blob.name.split("/")[-1]
            blob.download_to_filename(os.path.join(temp_folder1, filename))

        shutil.make_archive(os.path.join(temp_folder2, batch_id), 'zip', temp_folder1)

        return send_file(os.path.join(temp_folder2, batch_id)+".zip")

    else:
        return jsonify({"message": "The given Batch ID is not valid",
                        "Batch ID": batch_id,
                        "Suggestion": "The Batch might be deleted"}), 404

@app.route('/api/delete_gcs', methods=['DELETE'])
@token_required
def delete_blob_gcs():
    batch_id = request.args["batch_id"]
    print("batch_id = ", batch_id)
    file_status_tb = "FactFileStatus"
    file_info_tb = "FactFileInfo"
    batch_status_tb = "FactBatchStatus"
    batch_info_table = "FactBatchInfo"

    db_conn = create_db_conn().connect()
    db_data = db_conn.execute(
        "select count(*) as batch_num from {} where "
        "BatchID='{}' and StatusID=1".format(batch_status_tb, batch_id))
    num_batch_db = [row for row in db_data][0][0]
    db_conn.close()

    if num_batch_db != 0:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(app.config["INPUT_GCS_BUCKET"])

        blobs = bucket.list_blobs(prefix='{}/'.format(batch_id))
        counter = 0
        db_conn = create_db_conn().connect()
        batch_start_dt_tm = datetime.datetime.now()
        for blob in blobs:
            file_start_dt_tm = datetime.datetime.now()
            file_id = blob.name.split("/")[-1]
            blob.delete()
            file_end_dt_tm = datetime.datetime.now()
            counter += 1
            db_conn.execute(
                "update {} "
                "set StatusID=3, ProcessEndDtTm='{}' "
                "where "
                "FileID='{}' and StatusID=1".format(file_status_tb, file_end_dt_tm, file_id))

            db_conn.execute(
                "insert into {}(BatchID, FileID, ProcessStartDtTm, "
                "ProcessEndDtTm, StatusID, LastModifiedDtTm, LastModifiedBy, Description) "
                "values('{}', '{}', '{}', '{}', {}, '{}', '{}', '{}')".format(file_info_tb, batch_id, file_id,
                                                        file_start_dt_tm, file_end_dt_tm,
                                                        3, datetime.datetime.now(), "API",
                                                        "Data purged"))

        batch_end_dt_tm = datetime.datetime.now()
        db_conn.execute(
            "update {} "
            "set StatusID=3, BatchEndDtTm='{}' "
            "where "
            "BatchID='{}' and StatusID=1".format(batch_status_tb, batch_end_dt_tm, batch_id))

        db_conn.execute(
            "insert into {}(BatchID, ProcessStartDtTm, "
            "ProcessEndDtTm, StatusID, LastModifiedDtTm, LastModifiedBy, Description) "
            "values('{}', '{}', '{}', {}, '{}', '{}', '{}')".format(batch_info_table, batch_id,
                                                                          batch_start_dt_tm, batch_end_dt_tm,
                                                                          3, datetime.datetime.now(), "API",
                                                                          "Data purged"))
        db_conn.close()

        return jsonify({"Message": "Files Deleted",
                        "Batch ID": batch_id,
                        "Files Deleted": counter}), 200

    else:
        return jsonify({"Message": "Invalid Batch ID",
                        "Batch ID": batch_id,
                        "Description": "Batch ID: {} not found".format(batch_id),
                        "Suggestion": "The Batch might be deleted"}), 404

@app.route("/api/current_doc", methods=["GET"])
@token_required
def current_doc():
    db_conn = create_db_conn().connect()
    data = db_conn.execute("select count(1) as current_docs from FactFileStatus where StatusID=1")
    data_dict = [(dict(row.items())) for row in data][0]

    doc_count = data_dict["current_docs"]

    db_data = db_conn.execute("select BatchID, FileID from FactFileStatus where StatusID=1")
    data_dict = duplicate_data_struct(db_data)
    db_conn.close()
    resp = jsonify({"Current Docs": doc_count,
                    "File Info": data_dict})

    resp.status_code = 200
    return resp

@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({"message": 'File Too Large', "Error": error}), 413

@app.route('/api/login')
def apiLogin():
    auth = request.authorization
    if auth and auth.password == app.config['SECRET_KEY']:
        token = jwt.encode({'user': auth.username, 'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=120)},
                           app.config['SECRET_KEY'])
        return jsonify({'token': token.decode("utf-8")})
    else:
        return make_response("Could not authenticate", 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

@app.route('/')
def home():
    return jsonify({'message': 'Success'}), 200

if __name__ == "__main__":
    app.run(debug=False)
