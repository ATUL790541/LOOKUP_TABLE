from pyspark.sql import SparkSession
from pyspark.sql import functions
import os
from google.cloud import bigquery
import argparse
import logging
import random

def main():
    try:
        #lookup_indicator_col = 'cust_id'
        #lkp_asset_id = '1018'
        db_args, pipeline_args = get_args()
        # Reading arguments
        task_type = db_args.task_type
        # db_source = db_args.db_source
        driver_name = db_args.drivernamevalue
        host = db_args.hostvalue
        port = str(db_args.portvalue)
        #query = db_args.queryvalue
        project = db_args.project
        db_table = db_args.tablename
        database = db_args.databasename
        username = db_args.usernamevalue
        password = db_args.passwordvalue
        job_type = db_args.job_type
        group_name = db_args.group_name
        source_name = db_args.source_name
        #metadata_dataset = db_args.metadata_dataset
        #metadata_table = db_args.metadata_table
        #incremental_column = db_args.incremental_column
        #incremental_column_value = db_args.incremental_column_value
        #dw_load_ind=db_args.dw_load_ind
        #bigquery_table=db_args.bigquery_table
        temporary_gcs_bucket=db_args.temporary_gcs_bucket
        db_source = ""
        if "mysql" in driver_name:
            db_source = "mysql"
        elif "postgresql" in driver_name:
            db_source = "postgresql"
        print(db_source)

        if db_source == "mysql":
            spark = SparkSession.builder.master("yarn") \
                            .appName(task_type) \
                            .getOrCreate()
        elif db_source == "postgresql":
            jar_path = 'gs://tdf-tdlcomponents/components/jars/jars_postgresql-42.2.18.jar'+',gs://test_bucket_1d/spark-3.3-bigquery-0.32.0.jar'
            spark = SparkSession.builder.master("yarn") \
                            .appName(task_type) \
                            .config("spark.jars", jar_path) \
                            .config('spark.driver.extraClassPath', jar_path) \
                            .config("driver", driver_name) \
                            .getOrCreate()
        #Modified fetch lkp_asset_from_asset_id
        need_target_query = "SELECT lkp_asset_id FROM data_asset_vldtd_lookup_prop WHERE asset_id='{}'".format(source_name)
        print(need_target_query)
        need_val_query = "(" + need_target_query + ")""as " + "new_refer_table"
        print(need_val_query)
        need_url_required = "jdbc:" + db_source + "://" + host + ":" + port + "/" + "tdf_metadata"
        print(need_url_required)
        need_postgre_df_src_lookup = spark.read.format("jdbc").options(url=need_url_required, driver=driver_name, dbtable=need_val_query,
                                                                 user=username, password=password).load()
        need_postgre_df_src_lookup.show()
        need_pd = need_postgre_df_src_lookup.toPandas()
        need_column = need_pd.columns[0]
        lkp_asset_id =need_pd.loc[0,need_column]
        #End
            
        #Lookup Query
        
        target_query = "SELECT col_nm,lkp_col_nm FROM data_asset_vldtd_lookup_prop WHERE lkp_asset_id='{}'".format(lkp_asset_id)
        print(target_query)
        val_query = "(" + target_query + ")""as " + "refer_table"
        print(val_query)
        url_required = "jdbc:" + db_source + "://" + host + ":" + port + "/" + "tdf_metadata"
        print(url_required)
       
        postgre_df_src_lookup = spark.read.format("jdbc").options(url=url_required, driver=driver_name, dbtable=val_query,
                                                         user=username, password=password).load()
        postgre_df_src_lookup.createOrReplaceTempView("lookup_prop")
        postgre_df_src_lookup.show()
        look_res = postgre_df_src_lookup.toPandas()
        
        #Modified
        lookup_indicator = look_res.iloc[:,0].tolist()
        lookup_column = look_res.iloc[:,1].tolist()
        #End
        
        #SELECTING SALES TABLE from DB exctraction config
        db_target_query = "SELECT src_table_nm FROM db_extraction_config WHERE asset_id='{}'".format(lkp_asset_id)
        print(db_target_query)
        new_val_query = "(" + db_target_query + ")""as " + "new_table"
        print(new_val_query)
        exctraction_url_required = "jdbc:" + db_source + "://" + host + ":" + port + "/" + "tdf_metadata"
        print(exctraction_url_required)
        exctraction_df_src_lookup = spark.read.format("jdbc").options(url=exctraction_url_required, driver=driver_name, dbtable=new_val_query,
                                                         user=username, password=password).load()
        exctraction_df_src_lookup.show()
        sales_df = exctraction_df_src_lookup.toPandas()
        
        col = [sales_df.columns][0][0]
        tab_val = sales_df.loc[0,col]
        print(tab_val)
        
        #Value from Sales Table
        sales_query_target_lookup = 'SELECT * FROM {}'.format(tab_val)
        sales_queryvalue = "(" + sales_query_target_lookup + ")""as " + tab_val
        print(sales_queryvalue)
        sales_source_url_required = "jdbc:" + db_source + "://" + host + ":" + port + "/" + database
        print(sales_source_url_required)
        sales_postgre_df_src_lookup = spark.read.format("jdbc").options(url=sales_source_url_required, driver=driver_name, dbtable=sales_queryvalue,
                                                         user=username, password=password).load()
        sales_postgre_df_src_lookup.createOrReplaceTempView("sales")
        sales_postgre_df_src_lookup.show()
        sales_val_df = sales_postgre_df_src_lookup.toPandas()
        print(sales_val_df)
        #SOURCE TABLE QUERY
        query_target_lookup = 'SELECT * FROM {}'.format(db_table)
        queryvalue = "(" + query_target_lookup + ")""as " + db_table
        print(queryvalue)
        source_url_required = "jdbc:" + db_source + "://" + host + ":" + port + "/" + database
        print(source_url_required)
        source_postgre_df_src_lookup = spark.read.format("jdbc").options(url=source_url_required, driver=driver_name, dbtable=queryvalue,
                                                         user=username, password=password).load()
        spark_df = source_postgre_df_src_lookup
        spark_df.show()
        target_table = 'source_modified_table'
        spark_df.createOrReplaceTempView(target_table)
        print(target_table)
        print(tab_val)
        #print(lkp_tab_val)
        #Modified
        final_join_dataframe = []
        for i in range (len(lookup_indicator)):
            lk_indic = lookup_indicator[i]
            lkc_column = lookup_column[i]
            joining_query = "SELECT * from"+ " " + target_table + " "+ "LEFT JOIN" + " "+ tab_val + " "+ "ON" + " " + "{}.{} = {}.{}".format(target_table,lk_indic,tab_val,lkc_column)
            print(joining_query)
            final_join = spark.sql(joining_query)
            final_join = final_join.toPandas()
            if lk_indic == lkc_column:
                final_join = final_join.iloc[:, ~final_join.columns.duplicated(keep='first')]
            else:
                final_join = final_join.drop(lk_indic,axis = 1)
            final_join_dataframe.append(final_join)
        print(len(final_join_dataframe))
        print("Completed till here")
        print(final_join_dataframe)
        print("Completed till here")
        for i in final_join_dataframe:
            print(i.columns)
            print(len(i.columns))
        print("Finally")
        #End
    except Exception as e:
        logging.error("Exception thrown in main: ", exc_info=e)
        raise

    


def get_args():
    try:
        logging.info("In get_args() to fetch arguments")
        parser = argparse.ArgumentParser()
        # adding expected database args
        parser.add_argument('--drivername', dest='drivernamevalue', default=None)
        parser.add_argument('--host', dest='hostvalue', default=None)
        parser.add_argument('--port', type=int, dest='portvalue', default=None)
        parser.add_argument('--database', dest='databasename', default=None)
        parser.add_argument('--username', dest='usernamevalue', default=None)
        parser.add_argument('--password', dest='passwordvalue', default=None)
        parser.add_argument('--table', dest='tablename', default=None)
        # parser.add_argument('--dataflow_gcs_location', dest='dataflow_gcs_location', default='gs://rdbms/dataflow')
        parser.add_argument('--project', dest='project', default=None)
        # parser.add_argument('--region', dest='region', default=None)
        #parser.add_argument('--outputlocation', dest='outputlocation', default=None)
        # parser.add_argument('--service_account_email', dest='service_account_email', default="425387173015-compute@developer.gserviceaccount.com")
        #parser.add_argument('--outputformat', dest='outputformat', default=None)
        #parser.add_argument('--query', dest='queryvalue', default=None)
        # parser.add_argument('--setup_file_path', dest='setupfilepath', default=None)
        # parser.add_argument('--job_name', dest='job_name', default='rdbms-to-bq')
        # parser.add_argument('--db_source', dest='db_source', default=None)
        parser.add_argument('--jar_path', dest='jar_path', default=None)
        #parser.add_argument('--incremental_column', dest='incremental_column', default=None)
        #parser.add_argument('--incremental_column_value', dest='incremental_column_value', default=None)
        parser.add_argument('--task_type', dest='task_type', default=None)
        parser.add_argument('--job_type', dest='job_type', default=None)
        parser.add_argument('--group_name', dest='group_name', default=None)
        parser.add_argument('--source_name', dest='source_name', default=None)
        #parser.add_argument('--metadata_dataset', dest='metadata_dataset', default=None)
        #parser.add_argument('--metadata_table', dest='metadata_table', default=None)
        #parser.add_argument('--column_partition', dest='column_partition', default=None)
        #parser.add_argument('--dw_load_ind', dest='dw_load_ind', default=None)
        #parser.add_argument('--bigquery_table', dest='bigquery_table', default=None)
        parser.add_argument('--temporary_gcs_bucket', dest='temporary_gcs_bucket', default=None)
        parsed_db_args, pipeline_args = parser.parse_known_args()
        logging.info("Read arguments from composer")
        return parsed_db_args, pipeline_args


    except Exception as e:
        logging.error("Exception in get_args(): ", exc_info=e)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("In main: Job Started RDBMS to gcs via dataproc")
    main()
