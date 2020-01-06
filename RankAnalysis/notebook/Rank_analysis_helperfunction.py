import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import pyplot
import plotly.express as px
import plotly.graph_objects as go
from dataclasses import dataclass, field
from typing import Dict, List
from pyspark.sql.functions import lit
import warnings
warnings.filterwarnings('ignore')

import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
import pyspark


@dataclass
class Dataset:
    df: pyspark.sql.dataframe.DataFrame
    store_item_concept: List[tuple] = field(default_factory=list) # collect (SKU, Store, concept) to list 
    week: List[str] = field(default_factory=list) # collect the week to list 
    concept: List[str] = field(default_factory=list) # collect the new concept to list 

##############
## Clean Data
##############
def clean_dataset(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    
    ## Select the target features
    df = df.select('Date Detail', 'Company', 'Business Unit', 'Concept_NEW', 'Product Category',
               'Company and Cost Centre', 'SKU', 'POS Net Sales', 'rank')
    
    ## Reanme columns
    df = df.withColumnRenamed("POS Net Sales", "NetSales")
    df = df.withColumnRenamed("Date Detail", "Date")
    df = df.withColumnRenamed("Product Category", "Category")
    df = df.withColumnRenamed("Company and Cost Centre", "Store")
    df = df.withColumnRenamed("Business Unit", "BusinessUnit")
    
    ## Column type cast
    columns = ['NetSales','rank']
    df = convertColumn(df, columns, FloatType())
    # Replace none to 0
    df = df.na.fill(0)
    return df 


def convertColumn(df, names, newType) -> pyspark.sql.dataframe.DataFrame:
    """
    Convert the data type of DataFrame columns
    """
    for name in names: 
        df = df.withColumn(name, df[name].cast(newType))
    return df

#############################
### Aanlysis
############################

def get_store_item_concept_list(df: pyspark.sql.dataframe.DataFrame, spark) -> list:
    """
    Get the list of combinations of SKU, concept in stores
    """
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("dfView")
    # Query and create new dataframe
    sqlDF = spark.sql("SELECT DISTINCT SKU, Store, Concept_NEW FROM dfView")
    store_item_list = sqlDF.rdd.map(tuple).collect()
    return store_item_list

def get_week_list(df: pyspark.sql.dataframe.DataFrame) -> list:
    # Query and create new dataframe
    week_list = [row.Date for row in df.select("Date").distinct().collect()]
    week_list.sort() # sort date by increasing order
    return week_list

def get_concept_list(df: pyspark.sql.dataframe.DataFrame) -> list:
    # Query and create new dataframe
    concept_list = [row.Concept_NEW for row in df.select("Concept_NEW").distinct().collect()]
    return concept_list

def calculate_network_expansion(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    For the item by Store in W2, but not in W1, network_expansion = True, otherwise = False
    It is also sub_condition for material change column                            
    """
    last_week_data = dataset.df.filter(col('Date') == dataset.week[-1])
    old_week_data = dataset.df.filter(col('Date') == dataset.week[-2]).withColumnRenamed("Date", "Date_before") 
    output = old_week_data.join(last_week_data, on = ["SKU", 'Store', 'NetSales'], how = 'right')
    
    # network_expension = True if not in W1 and Net sale in W2 !=0
    output = output.withColumn('network_expansion', 
                                           when((col('Date_before').isNull())&(col('NetSales')!=0), 'True')
                                           .otherwise('False'))
    output = output.withColumn('in_W1_not_W2', 
                                     when((col('Date').isNull())&(col('NetSales')!=0), 'True')
                                     .otherwise('False'))
    output = output.select('SKU', 'Store', 'network_expansion', 'in_W1_not_W2')
    output = dataset.df.join(output, on = ['SKU', 'Store'], how = 'left')
    return output
    

def calculate_average_sale_last_week(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    For each SKU, calculate the average of total sale overall stores given the concept, 
    in the week before the last week
    
    Output df has three columns: SKU, Concept_NEW, avgSales_oldWeek 
    """
    last_week_data = dataset.df.filter(col('Date') == dataset.week[-1]) # based on sort list 
    
    # get average of sum for each (SKU, concept)
    last_week_data = last_week_data.groupBy("SKU",'Concept_NEW').agg({"NetSales":"avg"}) 
    last_week_data = last_week_data.withColumnRenamed("avg(NetSales)", "avgSales_lastWeek")
    last_week_data = last_week_data.na.fill(0) # fill none as 0
    return last_week_data






