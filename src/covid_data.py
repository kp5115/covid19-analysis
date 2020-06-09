"""Module to query covid APIs."""

import logging
import requests
import time

import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode, col, split

SPARK_CONTEXT =SparkContext()
SPARK_SESSION = SparkSession(SPARK_CONTEXT)

logging.getLogger(__name__).addHandler(logging.NullHandler())

API_BASE_URL="https://api.covid19api.com/"

def _get_data(api):
    """Retrieves the covid data"""
    url = API_BASE_URL + api
    logging.info("Querying URL: %s", url)
    response = requests.get(url)
    count = 0
    retry_count = 50
    if response.status_code == 404:
        logging.error("Received 404")
        return None
    while response.status_code != 200:
        count += 1
        if count == retry_count:
            break
        logging.error("Received status code %s, expected 200",
                      response.status_code)
        time.sleep(2)
        try:
            response = requests.get(url)
        except:
            pass
    if response.status_code != 200:
        # Send emoty dataframe. Remove None
        return None
    rdd = SPARK_CONTEXT.parallelize([response.json()])
    data_frame = SPARK_SESSION.read.json(rdd)
    return data_frame

def get_global_info():
    """Get covid19 info in global level.

    Returns dictionary contains 'NewConfirmed', 'NewDeaths', 'NewRecovered',
        'TotalConfirmed', 'TotalDeaths', 'TotalRecovered'  
    """
    data_frame = _get_data("summary")
    return data_frame.select("Global")

def get_countries_info():
    data_frame = _get_data("summary")
    new_df = data_frame.select(explode("Countries"))
    country_df = new_df.select("col.Country","col.Date","col.NewConfirmed",
                               "col.NewDeaths", "col.NewRecovered",
                               "col.TotalConfirmed", "col.TotalDeaths",
                               "col.TotalRecovered")
    return country_df

def get_top_countries_totalconfirmed(country_count=5):
    df = get_countries_info()
    new_df = df.sort(col("TotalConfirmed").desc()).limit(country_count)
    return new_df.select("Country", "TotalConfirmed")

def get_top_countries_totalrecovered(country_count=5):
    df = get_countries_info()
    new_df = df.sort(col("TotalRecovered").desc()).limit(country_count)
    return new_df.select("Country", "TotalRecovered")

def get_top_countries_recover_rate(country_count=5):
    df = get_countries_info()
    new_df = df.withColumn("RecoverRate", df.TotalRecovered / df.TotalConfirmed)
    ratio_df = new_df.select("Country", "RecoverRate", "TotalRecovered",
                             "TotalConfirmed")
    return ratio_df.sort(col("RecoverRate").desc()).limit(country_count)  

def get_top_countries_deaths_rate(country_count=5):
    df = get_countries_info()
    new_df = df.withColumn("DeathsRate", df.TotalDeaths / df.TotalConfirmed)
    ratio_df = new_df.select("Country", "DeathsRate", "TotalDeaths",
                             "TotalConfirmed")
    return ratio_df.sort(col("DeathsRate").desc()).limit(country_count)  

def get_countries_less_recover_rate(country_count=5):
    ndf = get_countries_info()
    df = ndf.filter(ndf.TotalConfirmed>0) 
    new_df = df.withColumn("RecoverRate", df.TotalRecovered / df.TotalConfirmed)
    ratio_df = new_df.select("Country", "RecoverRate", "TotalRecovered",
                             "TotalConfirmed")
    return ratio_df.sort(col("RecoverRate").asc()).limit(country_count)  

def get_countries_less_deaths_rate(country_count=5):
    ndf = get_countries_info()
    df = ndf.filter(ndf.TotalConfirmed>0) 
    new_df = df.withColumn("DeathsRate", df.TotalDeaths / df.TotalConfirmed)
    ratio_df = new_df.select("Country", "DeathsRate", "TotalDeaths",
                             "TotalConfirmed")
    return ratio_df.sort(col("DeathsRate").asc()).limit(country_count)  

def get_country_status(country):
    api = "dayone/country/{}".format(country)
    return _get_data(api)

def get_country_status_timebased(country, start_time, end_time):
    """Returns country status based on time.

    start_time and end_time must use YYYY-MM-DD format.
    """
    api = "country/{}?from={}T00:00:00Z&to={}T00:00:00Z".format(
        country, start_time, end_time)
    return _get_data(api)

def get_world_status_timebased(start_time, end_time):
    api = "world?from={}T00:00:00Z&to={}T00:00:00Z".format(
        start_time, end_time)
    return _get_data(api)

def get_all_status():
    api="all"
    return _get_data(api)
