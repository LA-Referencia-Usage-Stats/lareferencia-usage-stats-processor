from processorpipeline import AbstractUsageStatsPipelineStage, UsageStatsData
from configcontext import ConfigurationContext
import pandas as pd
import awswrangler as wr
from lareferenciastatsdb import SOURCE_TYPE_NATIONAL, SOURCE_TYPE_REGIONAL, SOURCE_TYPE_REPOSITORY


class S3ParquetInputStage(AbstractUsageStatsPipelineStage):
    
    def __init__(self, configContext: ConfigurationContext):
        super().__init__(configContext)

        # get the s3 bucket and type of parquet from the configuration
        self.visits_path = configContext.getConfig('S3_STATS', 'VISITS_PATH')
        self.events_path = configContext.getConfig('S3_STATS', 'EVENTS_PATH')

        # get the labels from the configuration
        self.COUNTRY_LABEL = configContext.getLabel('COUNTRY')
        self.OAI_IDENTIFIER_LABEL = configContext.getLabel('OAI_IDENTIFIER')

        # Keep a printable DB URI reference for diagnostics, regardless of ini format.
        self.usage_stats_db_uri = str(configContext.getDBHelper().engine.url)

        self.db_helper = configContext.getDBHelper()



    def _partition_filter(idsite, year, month, day):
        
        idsite = str(idsite)
        year = str(year)
    
        if month is None:
            return lambda x: (x['idsite'] == idsite) and (x['year'] ==  year )

        month = str(month)

        if day is None:
            return lambda x: (x['idsite'] == idsite) and (x['year'] ==  year ) and (x['month'] == month)  
        else:
            day = str(day)
            return lambda x: (x['idsite'] == idsite) and (x['year'] ==  year ) and (x['month'] == month) and (x['day'] == day)
        
    def _partition_description(idsite, year, month, day):
        parts = [f"idsite={idsite}", f"year={year}"]
        if month is not None:
            parts.append(f"month={month}")
        if day is not None:
            parts.append(f"day={day}")
        return ", ".join(parts)
        
        
    
    def _read_parquet_file(bucket_path, columns, partition_filter, partition_description):
            
        try: 
            df = wr.s3.read_parquet(
            path='s3://' + bucket_path,
            dataset=True,
            partition_filter = partition_filter,
            columns=columns
            )
            return df
        
        except Exception as exc:
            print(
                "Error reading parquet file s3://%s for partition [%s]: %s"
                % (bucket_path, partition_description, exc)
            )
            
    
    
    def run(self, data: UsageStatsData) -> UsageStatsData:

        year = self.getCtx().getArg('year')
        month = self.getCtx().getArg('month')
        day = self.getCtx().getArg('day')
        idsite = self.getCtx().getArg('site')

        source = self.db_helper.get_source_by_site_id(int(idsite))

        if source is None:
            raise Exception("Site not found in database: %s" % idsite)

        ## add the source to the data object
        data.source = source

        type = source.type

        partition_filter = S3ParquetInputStage._partition_filter(idsite, year, month, day)
        partition_description = S3ParquetInputStage._partition_description(idsite, year, month, day)

        # set the custom_var column name based on the type
        identifier_custom_var = 'custom_var_v1' if type == SOURCE_TYPE_REPOSITORY else 'custom_var_v6'

        # set the custom var for the record info
        record_info_custom_var = 'custom_var_v2'

        # set the columns to read based on the type
        events_columns = ['idlink_va', 'idvisit','server_time', identifier_custom_var, 'action_type', 'action_url', 'action_url_prefix']
        
        ## add the record info column if the source is regional
        if type == SOURCE_TYPE_REGIONAL:
            events_columns.append(record_info_custom_var)
        
        # read the events file       
        data.events_df = S3ParquetInputStage._read_parquet_file(
            self.events_path,
            events_columns,
            partition_filter,
            partition_description,
        )
        
        # if the events file is empty, create an empty dataframe
        if data.events_df is None:
            data.events_df = pd.DataFrame(columns= events_columns)
            # set server_time to datetime
            data.events_df['server_time'] = pd.to_datetime(data.events_df['server_time'])
        
        # rename the custom_var_v1 column to oai_identifier
        data.events_df = data.events_df.rename(columns={ identifier_custom_var: self.OAI_IDENTIFIER_LABEL })

        if type == SOURCE_TYPE_REGIONAL:
            ## parse the first two letters of the record info if the patter is XX_XXXXX if the field is not empty
            data.events_df[record_info_custom_var] = data.events_df[record_info_custom_var].apply(lambda x: x[:2] if x is not None and not pd.isna(x) and len(x) > 2 else None)
            ## rename the record info column to country
            data.events_df = data.events_df.rename(columns={record_info_custom_var: self.COUNTRY_LABEL})
        else:
            data.events_df[self.COUNTRY_LABEL] = source.country_iso


        # set the columns to read from the visits file
        visits_columns = ['idvisit', 'visit_last_action_time', 'visit_first_action_time', 'visit_total_actions', 'location_country']

        # read the visits file
        data.visits_df = S3ParquetInputStage._read_parquet_file(
            self.visits_path,
            visits_columns,
            partition_filter,
            partition_description,
        )
        
        # if the visits file is empty, create an empty dataframe
        if data.visits_df is None:
            data.visits_df = pd.DataFrame(columns=visits_columns)
        
        # rename the location_country column to country
        data.visits_df = data.visits_df.rename(columns={'location_country': self.COUNTRY_LABEL})
        
        return data
  


     
