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
from opv_dags import create_dag_make_panorama, create_make_panorama_tasks, create_make_all_panorama_tasks
from opv_directorymanagerclient import DirectoryManagerClient, Protocol
from opv_api_client import RestClient
from opv_tasks.utils import find_task
from  opv_api_client import RestClient, Filter
from opv_api_client.ressources import Campaign

from operator import attrgetter


# def create_simplify_version(dag_name, id_malette, id_campaign, args):
#     dag = DAG(
#         dag_id=dag_name,
#         default_args=args,
#         schedule_interval=None,
#     )

def create_dag_make_compaign(name, id_malette, id_campaign, args, nb_worker, subdag=False):
    """
    Create a MakePanorama DAG
    The purpose of this function is to create a dag as followed:


                    #############################     #############################
                ---># MakePanorama_Id_malette_1 #----># MakePanorama_Id_malette_2 #----
                |   #############################     #############################   |
    #########   |                                                                     |    #######
    # Start # ---                                                                     ---> # End #
    #########   |                                                                     |    #######
                |   #############################     #############################   |
                ---># MakePanorama_Id_malette_1 #----># MakePanorama_Id_malette_1 # ---
                    #############################     #############################

    :param name: The name of the campaign
    :param id_malette: The id_malette to use
    :param id_campaign: The id_campaign to use
    :param args: Some args to use to create dags
    :param nb_worker: The number of worker
    :param subdag: Use subdag
    :return: The new dag
    """

    if nb_worker <= 0:
        raise Exception("The nb_worker must greater than 0, actual value %s" % nb_worker)

    # dag_name = '%s.%s' % (parent_dag_name, "MakeCampaign_%s_%s" % (id_malette, id_campaign))
    dag_name = "%s_%s_%s" % ("subdag_%s" % name if subdag else name, id_malette, id_campaign)
    #print("Creating the dag %s" % dag_name)

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

    # Get the lots from RestClient
    db_client = RestClient("http://OPV_Master:5000")
    lots = db_client.make(Campaign, id_campaign, id_malette).lots
    lots = sorted(lots, key=attrgetter('id_lot'))

    # The last dag created
    last_dags = [start for _ in range(0, nb_worker)]

    # Some initialisation
    lots_length = len(lots)
    nb_splits = lots_length / nb_worker
    nb_loop = 0
    index = 0

    while index < lots_length:
        # Increment the number of loop cycle
        nb_loop += 1

        # check if we hit last list element
        nb_element = nb_worker if nb_loop < nb_splits else lots_length % nb_worker

        index_list = [index + i for i in range(0, nb_element)]

        i = 0
        for my_index in index_list:

            if subdag:
                # Create the dag
                sub_dag_name = "MakePanorama_%s_%s" % (
                    lots[my_index].id_malette, lots[my_index].id_lot
                )
                sub_dag = SubDagOperator(
                    task_id=sub_dag_name,
                    subdag=create_dag_make_panorama(
                        dag_name, sub_dag_name, lots[my_index].id_lot, lots[my_index].id_malette,
                        args
                    ),
                    default_args=args,
                    executor=SequentialExecutor,
                    dag=dag
                )
                last_dags[i].set_downstream(sub_dag)
                last_dags[i] = sub_dag
            elif subdag == "MARCHE_PAS_CAR_C_EST_LENT":
                p_start, p_stop = create_make_panorama_tasks(
                    dag, lots[my_index].id_lot, lots[my_index].id_malette, args
                )
                last_dags[i].set_downstream(p_start)
                last_dags[i] = p_stop
            else:
                task = create_make_all_panorama_tasks(
                    dag, lots[my_index].id_lot, lots[my_index].id_malette, args
                )
                last_dags[i].set_downstream(task)
                last_dags[i] = task

            i += 1

        # Increment the index of the lots
        index += nb_element

        if nb_element == 0:
            break

    for last_dag in last_dags:
        try:
            last_dag.set_downstream(end)
        except airflow.exceptions.AirflowException as e:
            print(str(e))
    return dag
