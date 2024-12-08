# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import os
import pandas as pd
from delta import * 
import pyspark
import pyspark.sql.functions as F
import data_processing

@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path())
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    conf = (
        pyspark.conf.SparkConf()
        .setAppName("LinkedInProcessor")
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
        .set("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY"))
        .set("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY"))
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.sql.shuffle.partitions", "4")
      
        .setMaster(
            "spark://localhost:7077"
        )
    )
    
    extra_packages = [    "org.apache.hadoop:hadoop-aws:3.3.4",    "org.apache.hadoop:hadoop-common:3.3.4",    "com.amazonaws:aws-java-sdk-bundle:1.12.262",]
    
    builder = pyspark.sql.SparkSession.builder.appName("LinkedInProcessor").config(conf=conf)
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=extra_packages
    ).getOrCreate()
    
    logger.info("Creating connections table")
    data_processing.create_connections_table(spark)
    
    logger.info("Creating messages table")
    data_processing.create_messages_table(spark)

    logger.info("Creating invitations table")
    data_processing.create_invitations_table(spark)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
