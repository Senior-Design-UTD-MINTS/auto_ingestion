import json
import http.client
import os
import sys
import schedule
import datetime

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
MINTS_RESOURCE_URL = "/api/001e06305a12/latestData.json"
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


def job():
    print("ran at time {}".format(datetime.datetime.now()))
    # download latest data from MINTS
    mints_conn = http.client.HTTPConnection(MINTS_BASE_URL)
    mints_conn.request("GET", MINTS_RESOURCE_URL)
    raw_response_body = mints_conn.getresponse().read().decode("utf-8")
    mints_conn.close()
    entries_json = json.loads(raw_response_body)

    # create a new json file to upload to druid
    update_file = open(OUTPUT_JSON_FILENAME, "w")
    for entry in entries_json["entries"]:
        string = json.dumps(entry)
        update_file.write(string)
        update_file.write("\n")

    update_file.close()

    # run the script provided by druid to append to our datasource, as specified by the spec
    script = "{}/{} --file {}/{} --url {}:{}".format(
        DRUID_INSTALL, DRUID_UPLOAD_SCRIPT, BASE_DIR, DRUID_JSON_SPEC, DRUID_URL, DRUID_PORT)
    print("attempting to execute the following script:")
    print(script)
    print("\n")
    os.system(script)


# start of script
setup_upload_spec()

schedule.every().minute.do(job)

print("starting at time {}".format(datetime.datetime.now()))
job()
while True:
    schedule.run_pending()
