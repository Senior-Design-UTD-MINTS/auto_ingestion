Keeps the DB up to date with the latest data from the sensors.

**Setup**
- clone the check-sensors repo and copy that script into the same folder as the auto ingestion script
  - you also need to properly setup the dependencies for checkSensors, naturally, so look at the README for that
- setup the following environment variables (or hardcode their corresponding variables in the script)
  ```
  DRUID_INSTALL_PATH = the path of the base folder where Druid is installed
  PHANTOMJS_INSTALL = the path of the phantomJS executable
  ```
- perform `pip install schedule` if you don't have that dependency already
- run the script with python3. Only works if you already have Druid running (otherwise you're uploading to nothing).
  
