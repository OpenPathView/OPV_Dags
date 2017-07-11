"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""

# Airflow import
from airflow import DAG
from airflow.models import Variable, XCOM_RETURN_KEY
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
# OPV Import
from opv_directorymanagerclient import DirectoryManagerClient, Protocol
from opv_api_client import RestClient
from opv_tasks.utils import find_task

import json
import logging
from datetime import datetime, timedelta


TASK_TO_DO = ["rotate", "cpfind", "autooptimiser", "stitchable", "stitch", "tiling"]


def launchOPVTask(ds, **kwargs):
    """
    Launch an OPV Task
    """
    logging.info("launchOPVTask(%s, %s)" % (ds, kwargs))

    # Get the task name to execute
    task_name = None
    if "OPV_Task_name" in kwargs:
        task_name = kwargs['OPV_Task_name']
    else:
        raise Exception("No OPV_Task_name found!")

    # Get the option to launch the task with
    options = None

    # Chek are deirectly passed to the task
    if "OPV_Option" in kwargs:
        logging.info("Get t")
        options = kwargs['OPV_Option']
    elif "previous_task" in kwargs and kwargs['previous_task'] is not None:
        previous_task = kwargs['previous_task']
        logging.info("Get options from xcom with previous_task=%s" % previous_task)
        ti = kwargs["task_instance"]
        # Pull it from xcom
        options = ti.xcom_pull(
            dag_id=ti.dag_id, task_ids=kwargs['previous_task'],
            key=XCOM_RETURN_KEY
        )
        logging.info(type(options))
        options = json.loads(options)
        logging.info("Found options=%s" % options)
        id_malette = options['id']['id_malette']
        del options['id']['id_malette']
        id_ressource = next(iter(options['id'].values()))
        id_task = (id_ressource, id_malette)
        options = {"id": (id_ressource, id_malette)}

    else: # Get the option given with the trigger_run
        options = kwargs['dag_run'].conf

    # Check the option
    if isinstance(options, str):
        options = json.loads(options)
        logging.error("Found Options %s" % options)
    elif not isinstance(options, dict):
        logging.error("No option found to pass to the task %s" % options)
        raise Exception("No option found to pass to the task %s" % options)

    # Get the address to Directory Manager
    # Variable.setdefault("OPV-DM", "http://OPV_Master:5005")
    # opv_dm = Variable.get("OPV-DM")
    opv_dm = "http://OPV_Master:5005"

    # Get the address to DB rest API
    # Variable.setdefault("OPV-API", "http://OPV_Master:5000")
    # opv_api = Variable.get("OPV-API")
    opv_api = "http://OPV_Master:5000"

    dir_manager_client = DirectoryManagerClient(
        api_base=opv_dm, default_protocol=Protocol.FTP
    )

    db_client = RestClient(opv_api)

    Task = find_task(task_name)
    if not Task:
        raise Exception('Task %s not found' % task_name)

    task = Task(
        client_requestor=db_client,
        opv_directorymanager_client=dir_manager_client
    )
    logging.info("Run '%s' with options=%s" % (task_name, options))
    return task.run(options=options)


def launchOPVTaskAll(ds, **kwargs):
    """
    Launch all an OPV Task
    """
    logging.info("launchAllOPVTask(%s, %s)" % (ds, kwargs))

    def interpret_options(string):
        options = json.loads(string)
        logging.info("Found options=%s" % options)
        id_malette = options['id']['id_malette']
        del options['id']['id_malette']
        id_ressource = next(iter(options['id'].values()))
        id_task = (id_ressource, id_malette)
        return {"id": (id_ressource, id_malette)}

    options = None

    # Chek are deirectly passed to the task
    if "OPV_Option" in kwargs:
        logging.info("Get t")
        options = kwargs['OPV_Option']

    # Check the option
    if isinstance(options, str):
        options = json.loads(options)
        logging.error("Found Options %s" % options)
    elif not isinstance(options, dict):
        logging.error("No option found to pass to the task %s" % options)
        raise Exception("No option found to pass to the task %s" % options)

    # Get the address to Directory Manager
    # Variable.setdefault("OPV-DM", "http://OPV_Master:5005")
    # opv_dm = Variable.get("OPV-DM")
    opv_dm = "http://OPV_Master:5005"

    # Get the address to DB rest API
    # Variable.setdefault("OPV-API", "http://OPV_Master:5000")
    # opv_api = Variable.get("OPV-API")
    opv_api = "http://OPV_Master:5000"

    dir_manager_client = DirectoryManagerClient(
        api_base=opv_dm, default_protocol=Protocol.FTP
    )

    db_client = RestClient(opv_api)

    try:
        for task_name in TASK_TO_DO:

            Task = find_task(task_name)
            if not Task:
                raise Exception('Task %s not found' % task_name)

            task = Task(
                client_requestor=db_client,
                opv_directorymanager_client=dir_manager_client
            )

            logging.info("Run '%s' with options=%s" % (task_name, options))
            options = interpret_options(task.run(options=options))
    except Exception as e:
        print(str(e))
    return "Ok"


def create_make_all_panorama_tasks(dag, id_lot, id_malette, args, dag_name=None):
    """
    Create all tasks for making a panorama
    :param id_lot:
    :param id_malette:
    :param dag_name: The dag name if it's a subdag
    :return:
    """
    subdag_name = "%s." % dag_name if dag_name is not None else ""

    name = "%s%s_%s_%s" % (subdag_name, "MakePanorama_%s_%s" % (id_malette, id_lot), id_malette, id_lot)

    opt = {
        "OPV_Option": {"id": [id_lot, id_malette]}
    }

    task = PythonOperator(
        task_id=name,
        provide_context=True,
        python_callable=launchOPVTaskAll,
        default_args=args,
        op_kwargs=opt,
        dag=dag
    )

    return task

def create_make_panorama_tasks(dag, id_lot, id_malette, args, dag_name=None):
    """
    Create all tasks for making a panorama
    :param id_lot:
    :param id_malette:
    :param dag_name: The dag name if it's a subdag
    :return:
    """

    default_options = {"id": [id_lot, id_malette]}
    previous_task_name = None

    subdag_name = "%s." % dag_name if dag_name is not None else ""

    start = DummyOperator(
        task_id='%sstart_panorama_%s_%s' % (subdag_name, id_malette, id_lot),
        default_args=args,
        dag=dag,
    )

    end = DummyOperator(
        task_id='%send_panorama_%s_%s' % (subdag_name, id_malette, id_lot),
        default_args=args,
        dag=dag,
    )

    previous_task = start

    for task in TASK_TO_DO:
        name = "%s%s_%s_%s" % (subdag_name, task, id_malette, id_lot)
        opt = {
            'OPV_Task_name': task,
            'previous_task': previous_task_name
        }
        if default_options is not None:
            opt["OPV_Option"] = default_options

        t = PythonOperator(
            task_id=name,
            provide_context=True,
            python_callable=launchOPVTask,
            op_kwargs=opt,
            dag=dag
        )
        previous_task_name = name
        if previous_task is not None:
            t.set_upstream(previous_task)
        previous_task = t
        default_options = None
        del opt

    end.set_upstream(previous_task)
    # previous_task.set_downstream(end)

    return start, end


def create_dag_make_panorama(parent_dag_name, dag_name, id_lot, id_malette, args):
    """
    Create a MakePanorama DAG
    The model is the following

    ##########       ##########       ##########
    # TASK_0 # ----> # TASK_2 # ----> # TASK_3 #
    ##########       ##########       ##########

    :param parent_dag_name: The parent dag name
    :param id_lot: The id of lot to build
    :param id_malette: The id of the malette to seek the correct id_lot
    :param args: Some args to use to create dag
    :return: The new dag
    """

    # dag_name = "MakePanorama_%s_%s" % (
    #     id_malette, id_lot
    # )
    #print("Creating dag %s" % dag_name)

    dag = DAG(
        dag_id='%s.%s' % (parent_dag_name, dag_name),
        default_args=args,
        schedule_interval=None,
    )

    create_make_panorama_tasks(dag, id_lot, id_malette, args, dag_name="%s.%s" % (parent_dag_name, dag_name))

    return dag
