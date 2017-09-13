from aarp.common.utils import createCluster, monitorJob, destroyCluster
import requests

def startAdobeLakeJob():
    clusterMetaData = createCluster(name='adobe_job', num_workers=6)
    print clusterMetaData
    jobURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/get?job_id=1884"
    res = requests.get(url=jobURL, auth=('production@aarp.com', 'C@serta!23'))
    print res
    print res.json()
    notebookDetails = res.json()
    notebookDetails['settings']['existing_cluster_id'] = clusterMetaData['cluster_id']
    resetUrl = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/reset"
    resetData = {
        "job_id": 1884,
        "new_settings": notebookDetails['settings']
    }
    res = requests.post(url=resetUrl, json=resetData, auth=('production@aarp.com', 'C@serta!23'))
    print res.status_code
    print res
    print res.content
    runURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/run-now"
    runData = {
        "job_id": 1884,
    }
    res = requests.post(url=runURL, json=runData, auth=('production@aarp.com', 'C@serta!23'))
    if res.status_code == 200:
        print 'job launched successfully, will start monitoring'
        print res.json()
        monitorJob(str(res.json()['run_id']))
    else:
        raise ReferenceError(
            'The notebook id does not match the dobule_lake_click job name, please update job or notebook to match')

    destroyCluster(name='adobe_job')


def startUTCJob():
    clusterMetaData = createCluster(name='adobe_job', num_workers=6)
    print clusterMetaData
    jobURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/get?job_id=1697"
    res = requests.get(url=jobURL, auth=('production@aarp.com', 'C@serta!23'))
    print res
    print res.json()
    notebookDetails = res.json()
    notebookDetails['settings']['existing_cluster_id'] = clusterMetaData['cluster_id']
    resetUrl = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/reset"
    resetData = {
        "job_id": 1697,
        "new_settings": notebookDetails['settings']
    }
    res = requests.post(url=resetUrl, json=resetData, auth=('production@aarp.com', 'C@serta!23'))
    print res.status_code
    print res
    print res.content
    runURL = "https://dbc-db50c5d5-5ae4.cloud.databricks.com/api/2.0/jobs/run-now"
    runData = {
        "job_id": 1697,
    }
    res = requests.post(url=runURL, json=runData, auth=('production@aarp.com', 'C@serta!23'))
    if res.status_code == 200:
        print 'job launched successfully, will start monitoring'
        print res.json()
        monitorJob(str(res.json()['run_id']))
    else:
        raise ReferenceError(
            'The notebook id does not match the dobule_lake_click job name, please update job or notebook to match')
    destroyCluster(name='adobe_job')