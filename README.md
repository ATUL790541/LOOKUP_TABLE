# LOOKUP_TABLE


This is lookup table implemented in GCS cloud.Here we are fetching source and target table from postgre table present in backend based on asset_id and lookup_asset_id respectively.Then a left join is performed based on
source_column in source_table and lookup required column to fetch final lookup table.
The lookup file is implemented inside dag present in airflow on GCS Cloud.
