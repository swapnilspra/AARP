import airflow
from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python_operator import PythonOperator
import pysftp as sftp
import requests
import json, yaml
from aarp.common.utils import loadYAMLEnvVariables

CONFIG=loadEnvVariables()['r4g']


def filelanding():
    s = sftp.Connection(host=CONFIG['host'],username=CONFIG['ingestuser'],password=CONFIG['ingestpwd'])
    localpath = CONFIG['localpath']

    remote_dir = CONFIG['remotedir']

    filelist = s.listdir(remote_dir)

    yesterday = (datetime.now() - timedelta(days = 1)).strftime("%Y%m%d")
    print(yesterday)

    print("Download Initiated...")

    for filename in filelist:
        filedate = filename[:8]
        if(filedate == yesterday):
            s.get(remote_dir + "/" + filename,localpath + "/" + filename)

    print("Files downloaded Successfully!")
    s.close()

def jobrun(jobname):
    print(paramjson["clusterid"])
      
    postdata = {
      "run_name": "r4g_job_"+jobname,
      "existing_cluster_id":paramjson["clusterid"],
      "timeout_seconds": CONFIG["r4g"]['timeoutsecs'],
      "notebook_task": {
        "notebook_path": CONFIG["r4g"]['notebookpath'],
        "base_parameters":{"file_type":CONFIG["r4g"][jobname]['file'],"schema_file_name":CONFIG["r4g"][jobname]['schema']}
      }
    }
    url = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/runs/submit"
    res = requests.post(url, auth=(CONFIG["r4g"]['user'],CONFIG["r4g"]['pwd']), json=postdata)
    runid = json.loads(res.text)["run_id"]
    print(runid)
    monitorJob(runid)
