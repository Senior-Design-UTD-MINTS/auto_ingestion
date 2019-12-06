import json
import http.client
import os
import sys
import schedule
import datetime
import subprocess
import checkSensors

DRUID_INSTALL_PATH_ENV_VAR = "DRUID_INSTALL_PATH"


def get_druid_install():
    install_path = os.environ.get(DRUID_INSTALL_PATH_ENV_VAR)
    if install_path is None:
        sys.exit("Error: System needs to have the environment variable {} set to where druid is installed on your system".format(
            DRUID_INSTALL_PATH_ENV_VAR))
    else:
        return install_path


# have to split the URL like this because of pythons HTTP API
MINTS_BASE_URL = "mintsdata.utdallas.edu:4200"
PHANTOMJS_INSTALL = checkSensors.get_phantomJS_path()
DRUID_INSTALL = get_druid_install()
DRUID_UPLOAD_SCRIPT = "bin/post-index-task"
DRUID_URL = "http://localhost"
DRUID_PORT = "8081"
DRUID_JSON_SPEC = "update-spec.json"
OUTPUT_JSON_FILENAME = "updates.json"
BASE_DIR = os.getcwd()


def setup_upload_spec():
    # updating the update-spec.json to be located in our present directory
    update_json_fh = open(DRUID_JSON_SPEC, "r")
    update_json = json.loads(update_json_fh.read())
    update_json_fh.close()

    update_json["spec"]["ioConfig"]["firehose"]["baseDir"] = BASE_DIR
    update_json_fh = open(DRUID_JSON_SPEC, "w")
    update_json_fh.write(json.dumps(update_json))
    update_json_fh.close()


def update_table_name(table_name, updates_filename, update_spec_filename):
    # updating the update-spec.json to be located in our present directory
    update_json_fh = open(DRUID_JSON_SPEC, "r")
    update_json = json.loads(update_json_fh.read())
    update_json_fh.close()

    update_json["spec"]["dataSchema"]["dataSource"] = table_name
    update_json["spec"]["ioConfig"]["firehose"]["filter"] = updates_filename
    update_json_fh = open(update_spec_filename, "w")
    update_json_fh.write(json.dumps(update_json))
    update_json_fh.close()


def mints_sensors():
    return checkSensors.get_sensors(PHANTOMJS_INSTALL)


def job():
    print("ran at time {}".format(datetime.datetime.now()))
    downloaded_entries = dict()
    # download latest data from MINTS
    mints_conn = http.client.HTTPConnection(MINTS_BASE_URL)

    # get all of the sensors located on the mints website using the checkSensors script.
    sensors = mints_sensors()

    # initialize an emtpy list for each sensor
    for sensor in sensors:
        downloaded_entries[sensor] = list()

    for sensor in sensors:
        mints_conn.request("GET", "/api/{}/latestData.json".format(sensor))
        raw_response_body = mints_conn.getresponse().read().decode("utf-8")
        try:
            entries_json = json.loads(raw_response_body)
        except ValueError:
            continue
        for entry in entries_json["entries"]:
            downloaded_entries[sensor].append(entry)
    mints_conn.close()
    # create a new json file to upload to druid

    for sensor_id, entries in downloaded_entries.items():
        table_name = "MINTS_" + sensor_id
        updates_filename = "updates_{}.json".format(sensor_id)
        update_spec_filename = "update-spec-{}.json".format(sensor_id)
        # write entries into updates.json file
        update_file = open(updates_filename, "w")
        for entry in entries:
            string = json.dumps(entry)
            update_file.write(string)
            update_file.write("\n")
        update_file.close()
        # change the table name of our updateSpec.json
        update_table_name(table_name, updates_filename, update_spec_filename)
        # run the script provided by druid to append to our datasource, as specified by the spec
        script = "{}/{} --file {}/{} --url {}:{}".format(
            DRUID_INSTALL, DRUID_UPLOAD_SCRIPT, BASE_DIR, update_spec_filename, DRUID_URL, DRUID_PORT)
        print("attempting to execute the following script:")
        print(script)
        print("\n")
        subprocess.Popen(script, shell=True)


# start of script
setup_upload_spec()

schedule.every().minute.do(job)

print("starting at time {}".format(datetime.datetime.now()))
job()
while True:
    schedule.run_pending()
