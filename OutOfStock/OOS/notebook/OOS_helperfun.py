## Functions for out of stock check for Lagardere Travel Retail
### Author: Yiran Jing
### Date: Jan 2020
import pandas as pd
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
import pyspark
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List
import warnings
warnings.filterwarnings('ignore')

def get_store_item_list(spark) -> list:
    """
    Get the list of combinations of SKU, store
    """
    sqlDF = spark.sql("SELECT DISTINCT SKU, Store FROM dfView")
    store_item_list = sqlDF.rdd.map(tuple).collect()
    return store_item_list


@dataclass
class Dataset:
    df: pyspark.sql.dataframe.DataFrame
    store_item_list: List[tuple] = field(default_factory=list) 
        
    def get_period_length(self) -> int:
        """
        get the length of this dataset 
        """
        return (dataset.max_date - dataset.min_date).days + 1


###########################
### Data Preprocessing
##########################

def clean_data(df: pyspark.sql.dataframe.DataFrame, 
               spark: pyspark.sql.session.SparkSession):
    """
    Apply data processing. 
        1)  Rename columns name
        2)  Columns type cast
        3)  Remove the closed store
        4)  Short SKU name by removing itemID
        5)  Remove items if no sales in the whole month, since they are not OOS
        6)  Remove items if no stock in the whole month, since they are not OOS
        7)  Add more rows to ensure each item in each store has the full-month records
        8)  Replace none to 0
        9)  Convert float number between -1 and 1 to 0
        10)  Save the cleaned dataset
    """
    
    ### 1)  Rename column
    df = df.withColumnRenamed("POS Margin on Net Sales", "Margin")
    df = df.withColumnRenamed("POS Net Sales", "NetSales")
    df = df.withColumnRenamed("Stock Balance Qty", "StockQty")
    df = df.withColumnRenamed("POS Qty Sold", "QtySold")
    
    # 2)  Conver the `df` columns to `FloatType()`
    columns = ['NetSales', 'QtySold', 'Margin', 'StockQty']
    df = convertColumn(df, columns, FloatType())
    # Convert Date column to timestamp 
    df = df.withColumn("Date", to_timestamp(df.Date, "yyyyMMdd"))

    # 3)  Remove the closed store
    df = remove_closed_store(df)
    
    # 4)  Short SKU name by removing itemID
    """
    short_column_udf = udf(lambda name: short_column(name), StringType())
    count = df.count()
    df = df.withColumn("SKU", short_column_udf(col("SKU")))
    assert df.count() == count, "Some error here" # test on overall dataset
    print(df.count())
    """
    
    # 5)  Remove items if no sales in the whole month, since they are not OOS
    df = remove_no_sale_item(df)
    
    # 6)  Remove items if no stock in the whole month, since they are not OOS
    df = remove_no_stock_item(df)
    
    # 7)  Add more rows to ensure each item in each store has the full-month records
    date_generated = create_list_dates(df)
    df = clean_and_add_date(df, date_generated, spark)
    
    # 8)  Replace none to 0
    df = df.fillna(0)
    
    
    # 9)  convert float number between -1 and 1 to 0
    #clean_numeric_column_udf = udf(lambda name: clean_numeric_column(name), FloatType())
    #df = df.withColumn("StockQty", clean_numeric_column(col("StockQty")))
    
    # 10)  save the cleaned dataset, overwrite the old one.
    #df.coalesce(1).write.option("header", "true").mode('overwrite').csv("../data/cleanedData") # only specify folder name
    print("Data processing finished.")   
    
    return df, date_generated
    
    
    
def remove_no_sale_item(df: pyspark.sql.dataframe.DataFrame)-> pyspark.sql.dataframe.DataFrame:
    hassale_item = df.groupBy("SKU", "Store").agg({"NetSales":
                                                           "sum"}).filter(col('sum(NetSales)')!=0).drop('sum(NetSales)')

    new_df = hassale_item.join(df, on = ["SKU", "Store"], how = 'inner')
    return new_df

def remove_no_stock_item(df: pyspark.sql.dataframe.DataFrame)-> pyspark.sql.dataframe.DataFrame:
    hassale_item = df.groupBy("SKU", "Store").agg({"StockQty":
                                                           "sum"}).filter(col('sum(StockQty)')!=0).drop('sum(StockQty)')

    new_df = hassale_item.join(df, on = ["SKU", "Store"], how = 'inner')
    return new_df
    
    
    
def convertColumn(df, names, newType) -> pyspark.sql.dataframe.DataFrame:
    """
    Convert the data type of DataFrame columns
    """
    for name in names: 
        df = df.withColumn(name, df[name].cast(newType))
    return df 



def remove_closed_store(data: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    Removed colosed store if the store in the Close stores list.csv
    """
    closed_store = pd.read_csv('../data/closedStore/Closed stores list.csv')
    closed_store_list = closed_store['Store'].unique()
    
    for store in closed_store_list:
        data = data[data.Store !=store] # remove the colsed store
    return data



def short_column(name : str) -> str:
    """
    Short SKU name by removing itemID
    """
    return name.split("-")[1]
assert short_column("AULS498076-Shepherds Hut") == "Shepherds Hut" # test on specific case



def test_list_dates(date_generated: list, end: datetime, start: datetime):
    """
    Test the accuracy of the generated date list
    """
    assert min(date_generated) == start, 'test fail, the first day should be ' \
    + str(start) +'but now is '+ str(min(date_generated))
    assert max(date_generated) == end - timedelta(days=1), 'test fail, the last day should be ' \
    + str(end) +'but now is '+ str(max(date_generated)) 


    
def create_list_dates(df: pyspark.sql.dataframe.DataFrame) -> list:
    """
    Create a list of dates, 
        start from the first day of dataset
        end with the last day of dataset
    
    :param df: dataframe
    :return: a list of dates
    """
    end = df.agg({"Date": "max"}).collect()[0][0] + timedelta(days=1)
    start = df.agg({"Date": "min"}).collect()[0][0]
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]
    
    # Test the output 
    #test_list_dates(date_generated, end, start)
    return date_generated    



def clean_and_add_date(df: pyspark.sql.dataframe.DataFrame, date_generated: list
                       , spark: pyspark.sql.session.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    """
    Add more rows to ensure each item in each store has the full-month records 
       (since if both stock and sales are 0, the raw date can miss the relevant column)
    """
    # Create a list of dates, start from the first day of dataset, end with the last day of dataset
    date_df = spark.createDataFrame(date_generated, DateType()) # create a Date df 
    date_df = date_df.withColumnRenamed("value", "Date")
    
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("dfView")
    
    # get temporary table with distinct combinnation of SKU and Store
    ##sqlDF = spark.sql("SELECT SKU, Store FROM dfView GROUP BY SKU, Store") # same 
    sqlDF = spark.sql("SELECT DISTINCT SKU, Store FROM dfView") 
    
    # Cross join two dataset to create full schema
    schema = sqlDF.crossJoin(date_df) # using crossjoin to quickly add 
    #assert schema.count() == sqlDF.count() * len(date_generated) # check cross join result
    #assert schema.count() >= df.count(), 'We want ' + str(df.count()) + \
    #'row. But we get '+str(schema.count()) # we need add rows
    
    # left join origial dataset with new schema
    df = df.join(schema, on= ['Date', 'Store', 'SKU'], how = 'right')
    #assert df.count() == count # test on overall dataset 
    return df


@udf(returnType=FloatType())
def clean_numeric_column(name : float) -> float:
    """
    Convert float number between -1 and 1 to 0
    """
    if name > -1 and name < 1:
        name = 0
    return name

##################################
######## Analysis
##################################
def create_sub_time_series_one_item(sub_data: pandas.core.frame.DataFrame, item: str, store: str):
    """
    Abstract dataset given specific one item in one store
    """
    ## create sub dataset
    sub_data = sub_data[sub_data['SKU'] == item]
    sub_data = sub_data[sub_data['Store'] == store]
    sub_data = sub_data.sort_values(by="Date")
    return sub_data

def check_OOS_last_day(df: pandas.core.frame.DataFrame, date: datetime):
    """
    If OOS in the last day of dataset, will return 1, otherwise return 0
    
    Input:
        df: dataframe, containing column 'date'
        date: a given datetime object, for example, '2019-11-01'
    """
    last_day = df['Date'].max()
    if date == last_day:
        return 1
    else:
        return 0


def calculate_possible_loss(sub_data: pandas.core.frame.DataFrame) -> str:
    """
    Calculate possible average loss due to OOS
    
    Return the average * number of days in which has 0 stock and 0 sales
    """
    sub_data1 = sub_data.copy()
    sub_data1 = sub_data1[sub_data1['StockQty']!=0] # remove non-stock day
    
    loss_INV = sub_data1['Margin'].mean() 
    loss_NS = sub_data1['NetSales'].mean() 
    loss_QTY = sub_data1['QtySold'].mean()
    
    return loss_INV, loss_NS, loss_QTY



def check_OOS_by_rules(df: pyspark.sql.dataframe.DataFrame, date_generated: list, 
                       spark: pyspark.sql.session.SparkSession) -> pandas.core.frame.DataFrame:
    """
    Set rules for OOS items:
        Rule 1. if 0 sales all the time, no OOS. (did in clean data)
        Rule 2. if 0 stocks all the time, no OOS. (did in clean data)
        Rule 3. OOS occurs when both stock and QTY sold are 0, and have sales before and after
        Rule 4. OOS happens when we have sales before. i.e. not new product case
        Rule 5. OOS days consider only the last 7 days
    
    param df: 
        dataframe with columns:
            'date', 'item_name', 'store_name', 'POS Margin on Net Sales (INV)', 'POS Net Sales', 
            'POS Qty Sold', 'Stock Balance Qty'
    return output_data:
        dataframe with header: 
             'item_name', 'store_name', 'category', 'OOS_days', 'date_list', 'OOS_lastDay','avg_loss_sale_quantity',
             'avg_loss_net_sale','avg_loss_INV', 'total_loss_sale_quantity','total_loss_net_sale','total_loss_INV'
    
    """ 
    # create dataclass 
    dataset = Dataset(df = df, store_item_list = get_store_item_list(spark))

    # subtract the last 7 days from current date
    one_weeks_ago = (date_generated[-1] + timedelta(days=1) - timedelta(weeks = 1)).date()
   
    # convert to panda dataframe for checking line by line
    df = df.toPandas()
    
    # define output directory
    OOS_result = {}
    # begin to check one by one
    for SKU_store in dataset.store_item_list:
        
        # Establish new status to check given a item in a given store.
        check = 0 # how many days OOS 
        last_day_OOS = 0 # will be 1 if OOS in the last day of given dataset
        check_not_new = False # check if it is the new product in this month
        check_not_removed = False # check if the producted has been removed
        OOS_date_list = [] # the list to store the date if OOS 
        
        item = SKU_store[0]
        store = SKU_store[1]
        # get subdataset, which contain only info inside
        #sub_data = get_sub_dataset(SKU = item, Store = store, spark = spark)
        sub_data = create_sub_time_series_one_item(df, item, store)
        # Convert to panda for further claculation
        category = sub_data.Category.unique()[-1]
       
        for index, row in sub_data.iterrows():
                # Rule 4: OS happens when we have sales before
                ## i.e. remove new product case
                ## or the item has only stock, but not sold
                if row['StockQty'] != 0:
                    check_not_new = True 
                    
                # Rule 3: OOS occurs when both stock and QTY sold are 0 and check_not_new is True
                # OOS occurs when both stock and QTY sold are 0
                if row['StockQty'] == 0 and row['QtySold'] == 0 \
                    and check_not_new == True and row['Date'] >= one_weeks_ago:
                    check +=1
                    OOS_date_list.append(row['Date'])
                    #print('Item {} has 0 stock at store {} , Date: {}'.format(item, row['Store'],row['Date']))
                    ## check OOS in last day
                    last_day_OOS = check_OOS_last_day(df, row['Date']) # return 1 if OOS in the last days
                        
                # as long as we have stock in this month, we believe this item is not removed from store
                if row['StockQty'] > 0:
                    check_not_removed = True
            
            # When this (item, store) contains the OOS days, 
            # and confirmed that this product is not been removed  
        if check >0 and check_not_removed == True:
            key = (item , store)
            loss_INV, loss_NS, loss_QTY = calculate_possible_loss(sub_data)
            OOS_result[key] = (check,loss_INV, loss_NS, loss_QTY, last_day_OOS,OOS_date_list) # returned value 
                
    ## Output to dataframe    
    output_data = out_put_data(OOS_result, category)
    return output_data


def out_put_data(OOS_result: dir, category: str) -> pandas.core.frame.DataFrame: 
    """
    Create csv file to save the output OOS result

    input:
        directory:
            key: tuple (item , store) 
            value: tuple (OOS_days, loss_INV, loss_NS, loss_QTY, last_day_OOS, OOS_date_list)
    
    output:
        dataframe with header: 
             'SKU', 'Store', 'category', 'OOS_days', 'date_list', 'OOS_lastDay','avg_loss_sale_quantity',
             'avg_loss_net_sale','avg_loss_INV', 'total_loss_sale_quantity','total_loss_net_sale','total_loss_INV'
    """
    
    header = ['SKU', 'Store', 'category', 'OOS_days', 'date_list', 'OOS_lastDay','avg_loss_sale_quantity',
         'avg_loss_net_sale','avg_loss_mergin', 'total_loss_sale_quantity','total_loss_net_sale','total_loss_mergin']
    output_data = pd.DataFrame(columns = header)
    new_row = {}
    
    for key, value in OOS_result.items():
        new_row['Store'] = key[1]
        new_row['SKU'] = key[0]
        new_row['Category'] = category
        new_row['OOS_days'] = value[0]
        new_row['date_list'] = value[5]
        new_row['OOS_lastDay'] = value[4]
        new_row['avg_loss_sale_quantity'] = value[3]
        new_row['avg_loss_net_sale'] = value[2]
        new_row['avg_loss_mergin'] = value[1]
        new_row['total_loss_sale_quantity'] = value[3] *value[0]
        new_row['total_loss_net_sale'] = value[2] *value[0]
        new_row['total_loss_mergin'] = value[1] *value[0]
    
        ## insert the new row            
        output_data = output_data.append(new_row, ignore_index=True) 
    return output_data