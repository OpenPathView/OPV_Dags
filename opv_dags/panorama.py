
# Airflow import
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# OPV Import

from opv_api_client import RestClient
from opv_tasks.utils import find_task
from opv_tasks.__main__ import run
from opv_directorymanagerclient import DirectoryManagerClient, Protocol

import json
import logging
from datetime import datetime, timedelta


def launchAllOPVTask(ds, **kwargs):
    """
    This function is definied to be use with a PythonOperator. It will launch
    all necessary OPV tasks to make a panorama.
    """
    logging.info("launchAllOPVTask(%s, %s)" % (ds, kwargs))

    options = None

    # Chek are deirectly passed to the task
    if "OPV_Option" in kwargs:
        options = kwargs['OPV_Option']

    # Check the option
    if isinstance(options, str):
        options = json.loads(options)
        logging.error("Found Options %s" % options)
    elif not isinstance(options, dict):
        logging.error("No option found to pass to the task %s" % options)
        raise Exception("No option found to pass to the task %s" % options)

    logging.info("launchAllOPVTask with options %s" % options)

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
        run(dir_manager_client, db_client, "makeall", options)
    except Exception as e:
        print(str(e))
    return "Ok"


def make_panorama(dag, id_lot, id_malette, args, priority_weight=1, dag_name=None):
    """
    Create an python operator to launch all necessary task to make a panorama
    :param id_lot: The lot id
    :param id_malette: The malette id
    :param priority_weight: Priority weight of the task
    :param dag_name: The dag name if it's a subdag
    :return:
    """
    subdag_name = "%s." % dag_name if dag_name is not None else ""

    name = "%s%s_%s_%s" % (subdag_name, "MakePanorama_%s_%s" % (id_malette, id_lot), id_malette, id_lot)

    opt = {
        "OPV_Option": {"id_lot": id_lot, "id_malette": id_malette}
    }

    logging.debug("Creating the PythonOperator %s with OPV_Options=%s" % (
        name, opt
    ))

    task = PythonOperator(
        task_id=name,
        provide_context=True,
        python_callable=launchAllOPVTask,
        default_args=args,
        op_kwargs=opt,
        priority_weight=priority_weight,
        dag=dag
    )

    return task
