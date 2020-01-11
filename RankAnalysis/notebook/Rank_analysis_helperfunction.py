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
    df = df.select('Index','Date Detail', 'Company', 'Business Unit', 'Concept_NEW', 'Product Category',
               'Company and Cost Centre', 'SKU', 'POS Net Sales', 'Rank Total')
    
    ## Reanme columns
    df = df.withColumnRenamed("POS Net Sales", "NetSales")
    df = df.withColumnRenamed("Date Detail", "Date")
    df = df.withColumnRenamed("Product Category", "Category")
    df = df.withColumnRenamed("Company and Cost Centre", "Store")
    df = df.withColumnRenamed("Business Unit", "BusinessUnit")
    df = df.withColumnRenamed("Rank Total", "rank")
    
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

def calculate_material_change(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    For each SKU in each store, compare the total sale with 
    the average of total sale overall stores given the same concept in the week before the last week
    
    return df: with column 'material_change':
            Blank:
                sumSales_oldWeek = 0
            True:
                1. for the item by Store in W1, but not in W2
                2. avgSales_lastWeek < sumSales_oldWeek (e.g w1 > w2)
            False:
                the left cases. 
            
            Note: 
                1. For the case: sumSales_oldWeek = 0, avgSales_lastWeek<0, in_W1_not_W2, material_change = ''
    """
    
    # get the average sales of each (SKU, Concept, average sale) in the week before last week
    average_sale_last_week = calculate_average_sale_last_week(dataset)
    
    # calculate last week infor
    old_week_data = dataset.df.filter(col('Date') == dataset.week[-2])
    
    ## net sale 
    old_week_sale = old_week_data.withColumnRenamed("NetSales", "sumSales_oldWeek").select('SKU',
                                                                                           'Store',
                                                                                           'sumSales_oldWeek')
    # merge dataset 
    output = dataset.df.join(average_sale_last_week, on = ['SKU', 'Concept_NEW'], how = 'full')
    output = output.join(old_week_sale, on = ['SKU', 'Store'], how = 'full')   
    output = output.na.fill(0)
    # test dataset
    #test_calculate_material_change(merge_data, last_week_sale)
    
    # generate 'material_change' based on given condition
    output = output.withColumn('material_change', 
                                       when(col('sumSales_oldWeek') ==0, '')
                                       .when((col("sumSales_oldWeek") > col('avgSales_lastWeek')) 
                                            & (col("in_W1_not_W2") =='True'), 'True') 
                                       .otherwise('False'))
    return output

#### add more test function 
def test_calculate_material_change(merge_data: pyspark.sql.dataframe.DataFrame, 
                                   last_week_sale: pyspark.sql.dataframe.DataFrame):
    assert merge_data.count() == last_week_sale.count(), 'we want ' + str(last_week_sale.count()) + \
    ". But we get "+str(merge_data.count())# test joined dataset
    
def calculate_unadressed_gap(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    unadressed_gap = True if: Not shown in both W1 and W2
            1. network_expansion = False
            2. and in_W1_not_W2 = False
            3. NetSale = 0
    """
    output = dataset.df.withColumn('unadressed_gap', 
                                       when((col('range_expansion') =='False')&\
                                            (col('in_W1_not_W2') =='False')&\
                                            (col('NetSales') ==0), 'True')
                                       .otherwise('False'))
    return output

def get_top_50(dataset: Dataset, weekChoice: int) -> pyspark.sql.dataframe.DataFrame:
    """
    Get top 50 rank (SKU, Store) of last week or the week before last week
    weekChoice: 
         -1: last week
         -2: the week before last week
    """
    week_data = dataset.df.filter(col('Date') == dataset.week[weekChoice])  
    top_50_week = week_data.filter(col('rank') <=50) # top 50
    return top_50_week.select('SKU', 'Store', 'Date')
    
def calculate_newcomer(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    calculate_newcomer = True if:
        1. Not in top 50 in the week before last week (e.g. w1)
        2. in top 50 in the last week (e.g. w2)
    Otherwise False
    """
    top_50_last_week = get_top_50(dataset, -1)
    top_50_week_before_last_week = get_top_50(dataset, -2).withColumnRenamed("Date", "Date_before")
    # Do left outer join to find new items occured in the last week
    newcomer = top_50_last_week.join(top_50_week_before_last_week, on = ['SKU', 'Store'], how = 'left')
    newcomer = newcomer.withColumn('newcomer',
                                    when(col('Date_before').isNull(), 'True')
                                   .otherwise('False')).drop("Date_before").drop('Date')
    
    output = dataset.df.join(newcomer, on = ['SKU', 'Store'], how = 'left').fillna("False", subset=['newcomer'])
    return output

def get_full_outer_join_data(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    Get the full outer join datasets: W1data full join W2data
    
    Date: the date of W2
    Date_before: the date of W1
    """
    last_week_data = dataset.df.filter(col('Date') == dataset.week[-1])
    old_week_data = dataset.df.filter(col('Date') == dataset.week[-2]).withColumnRenamed("Date", "Date_before") 
    joinData = old_week_data.join(last_week_data.drop("NetSales"), on = ["SKU", 'Store'], how = 'full')
    return joinData


def calculate_in_W1_not_W2(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    For the sold item by Store in W1, but not in W2, in_W1_not_W2 = True, otherwise Empty
    """
    joinData = get_full_outer_join_data(dataset)
    # Calculate 'in_W1_not_W2', = True if in W1 but not W2 
    output2 = joinData.withColumn('in_W1_not_W2', 
                                     when((col('Date_before') == dataset.week[-2])&(col('Date').isNull())&(col('NetSales')!=0), 'True')
                                     .otherwise('False'))
    output2 = output2.select('SKU', 'Store', 'Date','in_W1_not_W2', 'NetSales',"Date_before")
    
    ####################
    
    output2 = output2.select('SKU', 'Store', 'Date_before','in_W1_not_W2').withColumnRenamed("Date_before", "Date")
    output2 = output2.filter(col('in_W1_not_W2')=='True')
    output = dataset.df.join(output2, on = ['SKU', 'Store','Date'], how = 'left')
    
    ## testcase
    #test = output.filter(col('SKU') == 'AULS126353').filter(col('Store') == 'AULS.AVV102')
    #test.show()
    
    return output

def calculate_range_expansion(dataset: Dataset) -> pyspark.sql.dataframe.DataFrame:
    """
    For the sold item by Store in W2, but not in W1, range_expansion = True, otherwise Empty
    It is also sub_condition for material change column                            
    """
    joinData = get_full_outer_join_data(dataset)
    # Calculate 'range_expension' = True if not in W1 and Net sale in W2 !=0
    output1 = joinData.withColumn('range_expansion', 
                                           when((col('Date_before').isNull())&(col('NetSales').isNull())&(col('Date') == dataset.week[-1]), 'True')
                                           .otherwise('False'))
    
    output1 = output1.select('SKU', 'Store','Date','range_expansion')
    output1 = output1.filter(col('range_expansion')=='True')
    #output1.show()
    output1 = output1.fillna("None", subset=['Date'])
    output1 = dataset.df.join(output1, on = ['SKU', 'Store','Date'], how = 'left')
    return output1
    




