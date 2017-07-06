# -*- coding: utf-8 -*-

# email: nouchet.christophe@gmail.com
# Desciption: This script will generate all the dags necessary to build a campaign

import airflow

from airflow import DAG
from opv_dags import create_dag_make_compaign
from  opv_api_client import RestClient, Filter
from opv_api_client.ressources import Campaign

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1
}

NB_WORKER = 4

# Get all the campaigns
db_client = RestClient("http://OPV_Master:5000")
campaigns = db_client.make_all(Campaign)

for campaign in campaigns:
    print(
        "Create dag for the campaign %s, id_malette=%s and id_campaign=%s" % (
            campaign.name, campaign.id_malette, campaign.id_campaign
        )
    )

    # Sub_dag version
    globals()[
        "subdag_%s_%s_%s" % (campaign.name, campaign.id_malette, campaign.id_campaign)
    ] = create_dag_make_compaign(
        campaign.name, campaign.id_malette, campaign.id_campaign, args, NB_WORKER, subdag=True
    )
    globals()[
        "%s_%s_%s" % (campaign.name, campaign.id_malette, campaign.id_campaign)
        ] = create_dag_make_compaign(
        campaign.name, campaign.id_malette, campaign.id_campaign, args, NB_WORKER
    )
