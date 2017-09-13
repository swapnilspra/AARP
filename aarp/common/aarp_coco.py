#coco = common code
import psycopg2
from psycopg2 import errorcodes
from psycopg2.extras import DictCursor
from datetime import datetime
import ConfigParser
import requests
import json
import collections


def Config():
    """
    reads config file and returns a dictionary of config objects
    """
    config = ConfigParser.ConfigParser()
    config.read('etl.cfg')
    dictionary = {}
    for section in config.sections():
        dictionary[section] = {}
        for option in config.options(section):
            dictionary[section][option] = config.get(section, option)
    return dictionary

def dict_update(source, updates):
    """Update a nested dictionary or similar mapping.

    Modify ``source`` in place.
    """
    for key, value in updates.iteritems():
        if isinstance(value, collections.Mapping) and value:
            returned = dict_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = updates[key]
    return source



class PGInteraction (object):
    """
    """
    def __init__(self, dbname, host, user, password, port, autocommit = True):
        """
        """
        if not dbname or not host or not user or password is None:
            raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host     = host
        self.user     = user
        self.password = password
        self.dbname   = dbname
        self.port = port

        self.con = psycopg2.connect("dbname="+ self.dbname + " host=" + self.host +
                                    " user=" + self.user + " password=" + self.password+" port="+self.port)
        if autocommit:
            self.con.autocommit = True

    def get_conn(self):
        """
        Return already established connection
        """

        return self.con


    def batchCommit(self):
        """
        """
        try:
            self.con.commit()
        except Exception as e:
            pgError =errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
            #Logger().l("pgError")

    def fetch_all_sql(self, sql, dict_return=False):
        """

        :param sql:
        :param dict_return: Return result in dictionary like format
        :return:
        """
        try:
            if dict_return:
                curs = self.con.cursor(cursor_factory=DictCursor)
            else:
                curs = self.con.cursor()
            curs.execute(sql)
            results = curs.fetchall()
            curs.close()
        except Exception as e:
            pgError =errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
            curs.close()
        return results

    def fetch_one_sql(self, sql, dict_return=False):
        """

        :param sql:
        :param dict_return: Return result in dictionary like format
        :return:
        """
        try:
            if dict_return:
                curs = self.con.cursor(cursor_factory=DictCursor)
            else:
                curs = self.con.cursor()
            curs.execute(sql)
            results = curs.fetchone()
            curs.close()
        except Exception as e:
            pgError =errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
            curs.close()
        return results

    def exec_sql(self, sql):
        """
        :param sql:
        :return:
        """
        try:
            curs = self.con.cursor()
            results = curs.execute(sql)
            curs.close()
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
            curs.close()
        return results

    def bulkDictionaryInsert(self, table_name, col_dict):
        """
        """
        if len(col_dict) == 0:
            return

        placeholders = ', '.join(['%s'] * len(col_dict))
        columns = ', '.join(col_dict.keys())

        sql = "INSERT into %s ( %s ) VALUES ( %s )" % (table_name, columns, placeholders)

        try:
            self.cur.execute(sql, col_dict.values())
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)


    def bulkPostCleanup(self,table_name):
        """

        """
        sql = """
          delete from {0}
          where etl_updated=0
          and nk in (select nk from {0} where etl_updated = 1);

          update {0} set etl_updated = 0 where etl_updated = 1;""" .format(table_name)

        try:
            self.cur.execute(sql)
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)



class DBC_REST_API (object):
    """
    This class acts as a wrapper for making calls to Databricks REST API
    Methods:
    reset_job - done
    get_job - done
    list_jobs - done
    get_jobID - done
    run_job - done
    list_runs - done
    get_run - done
    cancel_run - done
    list_clusters - done
    get_clusterID - done
    """
    def __init__(self, user, password, host):
        """
        """

        if not user or not password or not host:
            raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host     = host
        self.user     = user
        self.password = password


    def reset_job(self, job_id, new_settings):
        """
        Desc: Overwrites the settings of a job with the provided settings.
        URL: POST /2.0/jobs/reset
        :param job:
        :param new_settings:
        :return:
        """

        #first get parameters of job
        data = self.get_job(job_id)
        #convert return to dict
        params_dict = json.loads(data.text)
        settings_only = params_dict['settings']
        #update dict with new setting
        updated_params = dict_update(settings_only, new_settings)

        api_call = '/2.0/jobs/reset'

        payload = {}
        payload["job_id"]=job_id
        payload["new_settings"]=updated_params

        req = self.__post_request(api_call, payload)

        return req


    def get_job(self, job_id):
        """
        Desc: Retrieves information about a single job.
        URL: GET /2.0/jobs/get
        :param job_id:
        :return:
        """
        api_call ='/2.0/jobs/get'

        payload = {}
        payload["job_id"]=job_id
        req = self.__get_request(api_call, payload)
        return req

    def list_jobs(self):
        """
        Desc: Lists all jobs.
        URL: GET /2.0/jobs/list
        :return:
        """
        api_call ='/2.0/jobs/list'
        payload={}
        req = self.__get_request(api_call, payload)
        return req

    def get_jobID(self, job_name):
        """
        Desc: Searches for Job by name and returns Job ID
        :param job_name:
        :return:
        """

        data = self.list_jobs()

        for job in data.json()['jobs']:
            if  job['settings']['name'] == job_name:
                id = job['job_id']
                return id

        return -1


    def run_job(self, job_id, notebook_params):
        """
        Desc: Runs the job now, and returns the run_id of the triggered run
        URL: POST /2.0/jobs/run-now
        :param job_id:
        :param notebook_params:
        :return:
        """
        api_call='/2.0/jobs/run-now'

        payload = {}
        payload["job_id"]=job_id
        payload["notebook_params"]=notebook_params

        req = self.__post_request(api_call, payload)

        return req

    def list_runs(self, job_id = -1, active_only=False, limit=20, offset=0):
        """
        Desc: Lists runs from most recently started to least
        URL: GET /2.0/jobs/runs/list
        :param job_id: Int - The job for which to list runs. If omitted, the Jobs service will list runs from all jobs.
        :param active_only: Bool - If true, lists active runs only; otherwise, lists both active and inactive runs.
        :param limit: Int - The number of runs to return. 0 means no limit. The default value is 20.
        :param offset: Int - The offset of the first run to return, relative to the most recent run.
        :return:
        """
        api_call ='/2.0/jobs/runs/list'

        payload = {}
        if job_id > -1:
          payload["job_id"]=job_id
        payload["active_only"]=active_only
        payload["limit"]=limit
        payload["offset"]=offset

        req = self.__get_request(api_call, payload)

        return req

    def get_run(self, run_id):
        """
        Desc: Retrieves the metadata of a run. Including Run Status.
        URL: GET /2.0/jobs/runs/get
        :param run_id:
        :return:
        """
        api_call='/2.0/jobs/runs/get'
        payload = {}
        payload["run_id"]=run_id

        req = self.__get_request(api_call, payload)

        return req

    def cancel_run(self, run_id):
        """
        Desc:Cancels a run. The run is canceled asynchronously, so when this request completes,
        the run may still be running. The run will be terminated shortly. If the run is already
        in a terminal life_cycle_state, this method is a no-op.
        URL:  /2.0/jobs/runs/cancel
        :param run_id:
        :return:
        """
        api_call='/2.0/jobs/runs/cancel'
        payload = {}
        payload["run_id"]=run_id

        req = self.__post_request(api_call, payload)

        return req

    def get_clusterID(self, cluster_name, comp_type ='eq', state='RUNNING'):
        """

        :param cluster_name:
        :param comp_type: default is 'eq'
        :param state: default is 'RUNNING'
        :return:
        """
        data = self.list_clusters()
        comp = comp_type.lower()

        for cluster in data.json()['clusters']:
          if comp == 'in':
            if  cluster_name in cluster['cluster_name'] and cluster['state'] == state:
              id = cluster['cluster_id']
              return id
          elif comp == 'eq':
            if  cluster['cluster_name'] == cluster_name and cluster['state'] == state:
              id = cluster['cluster_id']
              return id
          else:
            raise RuntimeError("Invalid parameter \'{0}\' entered in 'get_clusterID_by_name'".format(comp_type))

        return -1

    def list_clusters(self):
        """
        Desc: Returns information about all clusters which are currently active or which were terminated within the past hour.
        URL:  GET /2.0/clusters/list
        :param run_id:
        :return:
        """
        api_call ='/2.0/clusters/list'
        payload={}
        req = self.__get_request(api_call, payload)
        return req

    def create_cluster(self, cluster_specs):
        """
        Desc: Create a cluster -
        URL:  POST /2.0/clusters/create
        :param cluster_specs: Specifications of cluster to create
        :return: Cluster ID of newly created cluster
        """

        api_call='/2.0/clusters/create'
        payload = cluster_specs
#         payload["cluster_id"]=cluster_id

        req = self.__post_request(api_call, payload)
        return req


    def delete_cluster(self, cluster_name):
        """
        Desc: Deletes a cluster by name
        URL:  POST /2.0/clusters/delete
        :param cluster_name: Name of cluster to delete
        :return:
        """

        cluster_id = self.get_clusterID(cluster_name)

        api_call='/2.0/clusters/delete'
        payload = {}
        payload["cluster_id"]=cluster_id

        req = self.__post_request(api_call, payload)
        return req

    def __post_request(self, api_call, payload):
        """

        :param api_call:
        :param payload:
        :return:
        """


        url = self.host + api_call

        # Convert dict to json
        payload_json=json.dumps(payload, ensure_ascii=False)

        req = requests.post(url, auth=(self.user,self.password), data=payload_json)
        return req


    def __get_request(self, api_call, payload):
        """

        :param api_call:
        :param payload:
        :return:
        """


        url = self.host + api_call

        # Convert dict to json
        payload_json=json.dumps(payload, ensure_ascii=False)

        req = requests.get(url, auth=(self.user,self.password), data=payload_json)

        return req

class Batch_Table (object):
    """
    This Class is used to interact with the batch table to track job status and execution
    It is initiated with an instance of the PGInteration Object.
    One PG Object can be shared amongst mutiple batch objects since one connection can support
    multiple cursors
    """
    def __init__(self, PGInteraction_obj):
        """

        """

        self.db_conn     = PGInteraction_obj
        self.batch_id    = -1


    def start_batch(self):
        """

        :return:
        """
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Create Batch ID
        create_ID = 'insert into data_catalog.job_batch_table(start_time) values (\'{0}\');'.format(start_time)
        self.db_conn.exec_sql(create_ID)

        #Now Retrieve it
        retrieve_ID = 'select batch_id from data_catalog.job_batch_table where start_time = \'{0}\''.format(start_time)
        batch_id = self.db_conn.fetch_one_sql(retrieve_ID)

        self.batch_id = batch_id[0]
        self.start_time = start_time

        return batch_id[0], start_time

    def set_batch(self, id_to_set):
        """

        :return:
        """
        #Verify ID Exists
        verify_batch_sql = 'select batch_id from data_catalog.job_batch_table where batch_id = \'{0}\''.format(id_to_set)
        verify_batch = self.db_conn.fetch_one_sql(verify_batch_sql)

        if len(verify_batch) == 1:
            self.batch_id = id_to_set
            # Id verified, Return False
            return False

        # Id doesn't exist, Return True
        return True

    def update_batch_table(self, job_name='', job_id=-1, number_in_job=-1, run_id=-1, status='', error='', end_time=''):
        """

        :param job_name:
        :param job_id:
        :param number_in_job:
        :param run_id:
        :param status:
        :param error:
        :param end_time:
        :return:
        """

        if self.batch_id < 0:
            raise RuntimeError("Invalid Batch ID.  Must run 'get_batch_id' before updating the batch")
        params = list()
        if job_name != '':
            params.append('job_name=\'{0}\''.format(job_name))
            self.job_name = job_name
        if job_id > 0:
            params.append('job_id={0}'.format(job_id))
            self.job_id = job_id
        if number_in_job > 0:
            params.append('number_in_job={0}'.format(number_in_job))
            self.number_in_job = number_in_job
        if run_id > 0:
            params.append('run_id={0}'.format(run_id))
            self.run_id = run_id
        if status != '':
            params.append('status=\'{0}\''.format(status))
            self.status = status
        if error != '':
            params.append('error=\'{0}\''.format(error))
            self.error = error
        if end_time != '':
            params.append('end_time=\'{0}\''.format(end_time))
            self.end_time = end_time

        if params:
            sql = list()
            sql.append('update data_catalog.job_batch_table set ')
            sql.append(', '.join(params))
            sql.append('where batch_id={0};'.format(self.batch_id))
            sql_str = ' '.join(sql)
            self.db_conn.exec_sql(sql_str)
        return

    def end_batch(self):
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Create Batch ID
        end_batch = 'insert into data_catalog.job_batch_table(end_time) values (\'{0}\');'.format(end_time)
        self.db_conn.exec_sql(end_batch)

    def get_start(self):
        return self.__does_exist(self.start_time)

    def get_batch_id(self):
        return self.__does_exist(self.batch_id)

    def get_job_id(self):
        return self.__does_exist(self.job_id)

    def get_number_in_job(self):
        return self.__does_exist(self.number_in_job)

    def get_job_name(self):
        return self.__does_exist(self.job_name)

    def get_run_id(self):
        return self.__does_exist(self.run_id)

    def get_status(self):
        return self.__does_exist(self.status)

    def get_error(self):
        return self.__does_exist(self.error)

    def get_end_time(self):
        return self.__does_exist(self.end_time)

    def __does_exist(self, variable):
        if variable is not None:
            return variable
        else:
            return None
