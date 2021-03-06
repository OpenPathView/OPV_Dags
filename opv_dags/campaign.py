# Author: nouchet.christophe@gmail.com
# Description: Just some quick code to make all panorama of a campagn

# Airflow import
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.sequential_executor import SequentialExecutor

# OPV Import
from opv_dags import make_panorama
from opv_directorymanagerclient import DirectoryManagerClient, Protocol
from opv_api_client import RestClient
from opv_tasks.utils import find_task
from  opv_api_client import RestClient, Filter
from opv_api_client.ressources import Campaign

import logging
from operator import attrgetter


def make_campaign(name, id_malette, id_campaign, args):
    """
    Create a MakePanorama operators in a new DAG
    The purpose of this function is to create a dag as followed:


                    #############################
                ---># MakePanorama_Id-malette_1 #-----
                |   #############################    |
    #########   |                                    |    #######
    # Start # ---                                    ---> # End #
    #########   |                                    |    #######
                |   #############################    |
                ---># MakePanorama_Id-malette_2 #-----
                    #############################

    :param name: The name of the campaign
    :param id_malette: The id_malette to use
    :param id_campaign: The id_campaign to use
    :param args: Some args to use to create dags
    :return: The new dag
    """
    dag_name = "%s_%s_%s" % (name, id_malette, id_campaign)

    logging.debug("Creating the dag %s for id_malette=%s and id_campaign=%s" % (
        dag_name, id_malette, id_campaign
    ))

    dag = DAG(
        dag_id=dag_name,
        default_args=args,
        schedule_interval=None,
    )

    start = DummyOperator(
        task_id='%s_start' % dag_name,
        default_args=args,
        dag=dag,
    )

    end = DummyOperator(
        task_id='%s_end' % dag_name,
        default_args=args,
        dag=dag,
    )

    # Get all sorted lot
    db_client = RestClient("http://OPV_Master:5000")
    lots = db_client.make(Campaign, id_campaign, id_malette).lots
    lots = sorted(lots, key=attrgetter('id_lot'))

    priority = len(lots) + 1

    for lot in lots:
        # Create the operator to make the panorama and link it with the start
        # and end operator
        task = make_panorama(
            dag, lot.id_lot, lot.id_malette, args,
            priority_weight=priority
        )
        start.set_downstream(task)
        task.set_downstream(end)
        priority -= 1

    return dag
