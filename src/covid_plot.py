"""Modules for plotting graph."""

from pyspark.sql.functions import explode, col, split

import covid_data

def plot_country_status(country):
    """Prints data for specified country"""
    df = covid_data.get_country_status(country)
    new_df = df.select("Country","Date","Confirmed","Active","Deaths",
                       "Recovered")
    pdf = new_df.toPandas()
    # Remove last line - always it prints 0 values.
    pdf.drop(pdf.tail(1).index,inplace=True)
    print("Recent 20 data in {}".format(country))
    print(pdf.tail(20))
    pdf.set_index('Date',inplace=True)
    print("Plot")
    lines = pdf.plot()

def plot_country_status_timebased(country, start_time, end_time):
    """Prints data for specified country

    start_time and end_time must use YYYY-MM-DD format.
    """
    df = covid_data.get_country_status_timebased(country, start_time, end_time)
    new_df = df.select("Country","Date","Confirmed","Active","Deaths",
                       "Recovered")
    pdf = new_df.toPandas()
    # Remove last line - always it prints 0 values.
    pdf.drop(pdf.tail(1).index,inplace=True)
    print("Recent 20 data in {}".format(country))
    print(pdf.tail(20))
    pdf.set_index('Date',inplace=True)
    print("Plot")
    lines = pdf.plot()

def plot_world_status():
    df = covid_data.get_all_status()
    new_df = df.select("Country","Date","Confirmed","Active","Deaths",
                       "Recovered")
    pdf = new_df.toPandas()
    print(pdf.tail(20))
    pdf.set_index('Date',inplace=True)
    print("Plot")
    lines = pdf.plot()

def plot_top_countries_totalconfirmed(country_count=5):
    df = covid_data.get_top_countries_totalconfirmed(country_count)
    pdf = df.toPandas()
    print("Table")
    print(pdf.tail(country_count))
    print("Plot")
    pdf.plot(kind='bar',x='Country',y='TotalConfirmed')

def plot_top_countries_totalrecovered(country_count=5):
    df = covid_data.get_top_countries_totalrecovered(country_count)
    pdf = df.toPandas()
    print("Table")
    print(pdf.tail(country_count))
    print("Plot")
    pdf.plot(kind='bar',x='Country',y='TotalRecovered')

def print_world_status_timebased(start_time, end_time):
    """Prints world status data"""
    df = covid_data.get_world_status_timebased(start_time, end_time)
    new_df = df.select("NewConfirmed","NewDeaths","NewRecovered")
    pdf = new_df.toPandas()
    print(pdf.tail())

def print_world_status():
    df = covid_data.get_global_info()
    df.show()
