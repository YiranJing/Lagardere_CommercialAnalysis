#### Script for Inventory optimization
#### Author: Yiran Jing
#### Date: Dec 2019 - Jan 2020

import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import pyplot
import plotly.express as px
from scipy import stats
import plotly.graph_objects as go
import warnings
warnings.filterwarnings('ignore')
import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
# Import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
import pandas
import pyspark
from pandas import ExcelWriter
#from pyspark.sql.functions import when
from pyspark.sql.functions import UserDefinedFunction
import re
import errno, sys

## selected columns in output files
# for full month SKU
selected_column_fullMonth = ['MatID','SKU', 'month', 'Vendor','SubCategory', 'Classification','POGS','Facings', 
                              'Depth','Capacity', 'ProposedDepth', 'VarianceDepth','Capacity_to_qty',
                              'totalMonthlyNetSale', 'totalMonthlyGrossSale',
                              'totalMonthlyQtySold', 'Price', 'SellMargin',
                              'Qty_GeoMean_by_month_Subcat', 'NS_GeoMean_by_month_Subcat',
                              'Qty_mean_by_month_Subcat', 'NS_mean_by_month_Subcat',
                              'Qty_std_by_month_Subcat', 'NS_std_by_month_Subcat']
# for at least one month SKU
selected_column_atLeastOneMonth = ['MatID','SKU', 'Vendor','SubCategory', 'Classification','POGS','Facings', 
                                   'Depth', 'ProposedDepth', 'VarianceDepth','Capacity','Capacity_to_avg_qty', 
                                   'Facing_to_avg_qty','AvgQtySold', 'Qty_std_by_SKU','StdQty_to_AvgQty']


###############################
### EDA
###############################    

def pie_chart_margin(column: str, df: pandas.core.frame.DataFrame, title1: str, title2: str, explode: tuple):
    
    """
    GroupBy Classification, visualization by margin
    """

    df1 = pd.DataFrame(df.groupby('Classification')[column].sum())
    df3 = pd.DataFrame(df.groupby('Classification')['totalMonthlyNetSale'].sum())
    df4 = df1['SellMargin']/df3['totalMonthlyNetSale']
    print(df3.reset_index())
    print('\nSell Margin % means: TotalSellMargin / totalMonthlyNetSale within classifiction ')
    
    labels = df1.index
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15,5))
    # plot 1
    sizes1 = df1[column] 
    axes[0].pie(sizes1, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    axes[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[0].set_title(title1, fontsize=15)
    
    # plot 2
    
    sns.barplot(x = "Classification", y = df4.reset_index().columns[1], 
                             data=df4.reset_index(), ax = axes[1])
    axes[1].set_title(title2, fontsize=15)
    axes[1].set_ylabel('% SellMargin / totalMonthlyNetSale of this Classification', fontsize=12)
    
    #axes[1].pie(df4, explode=explode, labels=labels, autopct='%1.1f%%',
            #shadow=True, startangle=90)
    #axes[1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    #axes[1].set_title(title2, fontsize=15)
    
    # plot 3
    axes[2] = sns.boxplot(x="Classification", y=column, data=df)
    axes[2] = sns.swarmplot(x="Classification", y=column, data=df, color=".25")
    axes[2].set_title('Sell Margin Details', fontsize=15)
    
    fig.tight_layout()
    plt.show()
    
def pie_chart_margin_selectedItem(column: str, pdf: pandas.core.frame.DataFrame, 
                                  df: pandas.core.frame.DataFrame, title1: str, title2: str, explode: tuple):
    
    """
    GroupBy Classification, visualization by margin for selected Item
    pdf: the full version of dataset
    df: selected items
    """

    df1 = pd.DataFrame(df.groupby('Classification')[column].sum())
    df2 = pd.DataFrame(pdf.groupby('Classification')['SellMargin'].sum())
    df3 = pd.DataFrame(df.groupby('Classification')['totalMonthlyNetSale'].sum())
    df4 = df1['SellMargin']/df2['SellMargin']
    df5 = pd.DataFrame(df.groupby(['Classification','month'])[column].sum()).reset_index()
    print('There are {} DISTINCT items'.format(df['SKU'].nunique()))
    
    print(df3.reset_index())
    print()
    print('⭐️Sell Margin % means: SumSellMarginselected items / SumSellMargin of all items within classifiction ')
    
    labels = df1.index
    fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(18,15))
    # plot 1
    sizes1 = df1[column] 
    axes[0,0].pie(sizes1, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    axes[0,0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[0,0].set_title(title1, fontsize=15)
    
    # plot 2
    sns.barplot(x = "Classification", y = df4.reset_index().columns[1], 
                             data=df4.reset_index(), ax = axes[0,1])
    axes[0,1].set_title(title2, fontsize=15)
    axes[0,1].set_ylabel('% SellMargin of selected items / SellMargin of all items within classifiction', fontsize=12)
    
    # plot 3
    sns.barplot(x = "Classification", y = column, 
                             data=df5, hue = 'month', ax = axes[1,0])
    axes[1,0].set_title(title1, fontsize=15)
    
    # plot 4
    axes[2,1] = sns.boxplot(x="Classification", y=column, data=df)
    axes[2,1] = sns.swarmplot(x="Classification", y=column, data=df, color=".25")
    axes[2,1].set_title('Sell Margin Details of items whose capacity > monthly sold Qty in three month', fontsize=15)
    
    # plot 5
    sns.barplot(x ="Classification", y = "Capacity_to_qty", hue = 'month',
                             data=df, ax = axes[2,0])
    axes[2,0].set_title("Capacity to qty of items whose capacity > monthly sold Qty in three month", fontsize=15)
    
    # plot 6
    sns.barplot(x ="Classification", y = "Capacity_to_sales", hue = 'month',
                             data=df, ax = axes[1,1])
    axes[1,1].set_title("Capacity to Net Sales of items whose capacity > monthly sold Qty in three month", fontsize=15)
    
    fig.tight_layout()
    plt.show()
    

def explain_average_and_total_difference():
    """
    Help function for pie_chart_classification
    """
    print("Explain the difference between Average and Total")
    print()
    print("SKU1 | Cat1 | Month1 |5")
    print("SKU1 | Cat1 | Month2 |6")
    print("SKU2 | Cat1 | Month1 |3")
    print("SKU3 | Cat1 | Month1 |7")
    print()
    print("Total of 'Cat1' is 5+6+7+3 = 21")
    print("Average of 'Cat1' is (5+6+7+3)/4 = 5.25\n")    
    
def pie_chart_classification(column: str, df: pandas.core.frame.DataFrame, title1: str, title2: str, explode: tuple, calss_choice: str):
    
    
    print("Plot for "+str(column))

    df1 = pd.DataFrame(df.groupby(calss_choice)[column].sum()
                       /(df[column].sum()) * 100)
    df2 = pd.DataFrame(df.groupby(calss_choice)[column].mean()
                       /(df[column].mean()) * 100)
    
    labels = df2.index
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15, 5))
    # plot 1
    sizes1 = df1[column] 
    axes[0].pie(sizes1, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    axes[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[0].set_title(title1, fontsize=15)
    
    # plot 2
    sizes2 = df2[column] 
    axes[1].pie(sizes2, explode=explode, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    axes[1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[1].set_title(title2, fontsize=15)
    
    fig.tight_layout()
    plt.show()
    
    # plot 3
    fig, axes = plt.subplots(figsize=(15, 5))
    df1 = pd.DataFrame(df.groupby([calss_choice,'month'])[column].sum()).reset_index()
    axes = sns.barplot(x=calss_choice, y=column, hue = 'month', data=df1)
    axes.set_title(title1, fontsize=15)
    plt.show()
    
    # plot 4
    fig, axes = plt.subplots(figsize=(15, 5))
    df2 = pd.DataFrame(df.groupby([calss_choice,'month'])[column].mean()).reset_index()
    axes = sns.barplot(x=calss_choice, y=column, hue = 'month', data=df2)
    axes.set_title(title2, fontsize=15)
    plt.show()
    
    fig.tight_layout()
    plt.show()
    print()
    print("------------------------------------------------------------------")

###############################
##### Identify issued items
###############################   

def find_check_item(month_merge:pyspark.sql.dataframe.DataFrame, 
                    dist_df:pyspark.sql.dataframe.DataFrame,
                    output_columns: list) -> pyspark.sql.dataframe.DataFrame:
    """
    checked item:
       The items are in distribution report, but have no sales from Apr-Sep (or Sep-Nov)
    """
    check_df = dist_df.join(month_merge, on=["MatID"], how="left").fillna(0, subset=['totalMonthlyGrossSale']) 
    check_item = check_df.filter(check_df.totalMonthlyGrossSale == 0)
    check_item = check_item.select(output_columns)
    return check_item


def find_removed_item(month_merge_late:pyspark.sql.dataframe.DataFrame,
                      month_merge_early:pyspark.sql.dataframe.DataFrame,
                      dist_df:pyspark.sql.dataframe.DataFrame,
                      output_columns: list) -> pyspark.sql.dataframe.DataFrame:
    """
    removed item:
       The items are in distribution report, but have no sales from July-Sep (or Sep-Nov)
    """
    Removed_df = dist_df.join(month_merge_late, on=["MatID"], how="left").fillna(0, subset=['totalMonthlyGrossSale'])
    Removed_df = dist_df.join(month_merge_early, on=["MatID"], how="inner").fillna(0, subset=['totalMonthlyGrossSale'])
    Removed_item = Removed_df.filter(Removed_df.totalMonthlyGrossSale == 0)
    Removed_item = Removed_item.select(output_columns)
    return Removed_item


def find_new_item(month_merge_late:pyspark.sql.dataframe.DataFrame,
                 dist_df:pyspark.sql.dataframe.DataFrame,
                 output_columns: list) -> pyspark.sql.dataframe.DataFrame:
    """
    new item: 
        The items are not in distribution report, but have sale history from July-Sep (or Sep-Nov)
    """
    New_df = dist_df.join(month_merge_late, on=["MatID"], how="right").fillna(0, subset=['totalMonthlyGrossSale'])
    New_item = New_df.filter(New_df.totalMonthlyGrossSale != 0) # new item is sold during July- Sep
    New_item = New_item.filter(col("Classification").isNull()) # new item has no classification records
    New_item = New_item.select(output_columns)
    return New_item 


def find_Incorrect_record_items(month_merge: pyspark.sql.dataframe.DataFrame, 
                                output_columns: list) -> pyspark.sql.dataframe.DataFrame:
    """
    The items has extremely high ratio of capacity/facing. (Ratio >6)
    """
    Incorrect_record_items = month_merge.filter(col('Capacity')/col('Facings') >6)
    Incorrect_record_items = Incorrect_record_items.withColumn("Depth", col('Capacity') / col('Facings')).select(output_columns)
    return Incorrect_record_items


def find_Depth2_items(month_merge: pyspark.sql.dataframe.DataFrame, 
                                output_columns: list) -> pyspark.sql.dataframe.DataFrame:
    """
    same as Capacity < Facings*2. They are issued items: for example:
       1. Capacity = Facings = 1, incorrect
       2. Facing = 3, Capacity = 5, incorrect
       3. Capacity should > Facing. 
    """
    Depth2_items = month_merge.filter(col('Capacity')/col('Facings') <2)
    Depth2_items = Depth2_items.withColumn("Depth", col('Capacity') / col('Facings')).select(output_columns)
    return Depth2_items

def Identify_and_output_issused_items(month_merge: pyspark.sql.dataframe.DataFrame, 
                                      month_merge_early: pyspark.sql.dataframe.DataFrame, 
                                      month_merge_late: pyspark.sql.dataframe.DataFrame,
                                      dist_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    Identify 5 kinds of issued items:
       1. check_item
       2. Removed_item
       3. New_item
       4. Incorrect_record_items
       5. Depth2_items
       
    Save them as 5 csv files into Output/IssuedItem folder
    """
    ## Define the output columns we want to keep in the output dataset
    # for check_item, Removed_item, and New_item
    output_columns = ['SKU', 'MatID', 'month', 'Price', 'POGS','Facings', 'Capacity','Classification', 
                  'SubCategory', 'totalMonthlyNetSale', 'totalMonthlyQtySold', 'SellMargin']
    
    # for Incorrect_record_items, and Depth2_items
    output_columns2 = ['MatID','SKU','Facings','Capacity','POGS', 'Depth','DaysSupply','Classification',
                      'month','SubCategory', 'Vendor', 'totalMonthlyNetSale', 'totalMonthlyGrossSale',
                      'avgCOGS','totalMonthlyQtySold','Price','SellMargin','avgFrontMargin']
    
    ## check_item
    check_item = find_check_item(month_merge, dist_df, output_columns)
    Removed_item = find_removed_item(month_merge_late, month_merge_early, dist_df, output_columns)
    New_item  = find_new_item(month_merge_late, dist_df, output_columns)

    ## Removed_item
    Removed_item = Removed_item.toPandas()
    print("Find {} Removed items, save them in Output/IssuedItem/removed_SKU.csv".format(len(Removed_item['MatID'].unique())))
    Removed_item.to_csv('../data/Output/IssuedItem/removed_SKU.csv', 
                                 index=False, encoding='utf-8')
    ## New_item
    New_item = New_item.toPandas()
    print("Find {} New items, save them in Output/IssuedItem/new_SKU.csv".format(New_item['MatID'].nunique()))
    New_item.to_csv('../data/Output/IssuedItem/new_SKU.csv', 
                                 index=False, encoding='utf-8')
    
    check_item = check_item.toPandas()
    print("Find {} checked items, save them in Output/IssuedItem/checked_SKU.csv".format(len(check_item['MatID'].unique())))
    check_item.to_csv('../data/Output/IssuedItem/checked_SKU.csv', 
                                 index=False, encoding='utf-8')
  
    # Join the DataFrames
    month_merge = dist_df.join(month_merge_late, on="MatID", how="inner")
    
    ## Incorrect_record_items
    Incorrect_record_items = find_Incorrect_record_items(month_merge, output_columns2).toPandas()
    print("Find {} Incorrect_record_items, save them in Output/IssuedItem/Incorrect_record_items.csv".format(len(Incorrect_record_items['MatID'].unique())))
    Incorrect_record_items.to_csv('../data/Output/IssuedItem/Incorrect_record_items.csv', 
                                 index=False, encoding='utf-8')
    ## Depth less than 2 items
    Depth2_items = find_Depth2_items(month_merge, output_columns2).toPandas()
    print("Find {} Depth < 2 items, save them in Output/IssuedItem/Depth2_items.csv".format(len(Depth2_items['MatID'].unique())))
    Depth2_items.to_csv('../data/Output/IssuedItem/Depth2_items.csv', 
                                 index=False, encoding='utf-8')
    return month_merge # full dataset

######################
## atLeastOneMonth_SKU
######################
def find_and_analysis_atLeastOneMonth_SKU(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    For SKU, which "soldQty < capacity" in at least one month,
       1. Calcuaate the average of REAL NS, Standard dev of avg NS;
          REAL NS means that if one item has 0 sale on one month, avg calculation will only consider another 2 months
       2. Calculate Capacity_to_avg_qty, and Facing_to_avg_qty
    
    Output: 2 dataset: 
       1. df_atLeastOneMonth: fulldataset
       2. unchange Depth SKU
       3. Changed Depth SKU
       4. df_full is the combination of unchanged_SKU and changed_SKU
    """  

    
    ## Find at least one month SKU
    df = df.withColumn('qty_less_than_capacity', when((col("totalMonthlyQtySold") < col('Capacity')) , 1).otherwise(0))
    df_atLeastOneMonth = df.filter(df.qty_less_than_capacity == 1) # find SKU which qtySold> capacity at least on month
    
    ## Calculate the average of REAL NS;
    df_groupbySKU = df.filter(df.totalMonthlyNetSale !=0).groupBy('MatID',"SubCategory", 'Vendor') # Group by each SKU
    ## get the average net-sales of each product 
    SKU_avg_Qty = df_groupbySKU.avg("totalMonthlyQtySold").withColumnRenamed("avg(totalMonthlyQtySold)", "AvgQtySold")
    SKU_avg_std = df_groupbySKU.agg(stddev('totalMonthlyQtySold'))\
    .withColumnRenamed('stddev_samp(totalMonthlyQtySold)', "Qty_std_by_SKU")
    
    
    ## Join datasets 
    df_1 = SKU_avg_Qty.join(df_atLeastOneMonth, on=["MatID", 'SubCategory', 'Vendor'], how="right")
    df_1 = df_1.join(SKU_avg_std, on=["MatID", 'SubCategory', 'Vendor'], how="left")
    df_1 = df_1.withColumn('Capacity_to_avg_qty',(col('Capacity') / col("AvgQtySold")))
    df_1 = df_1.withColumn('Facing_to_avg_qty',(col('Facings') / col("AvgQtySold")))
    # Calculate the ratio of average qty sold to the std of SKU
    df_1 = df_1.withColumn('StdQty_to_AvgQty',(col('Qty_std_by_SKU') /col("AvgQtySold")))
    
    
    # if no standard derivation, means that this SKU is sold only one month
    df_full = df_1.select(selected_column_atLeastOneMonth).dropDuplicates()
    # separate SKU to 2 groups
    unchanged_SKU = df_full.filter(col('Depth') < 3)
    changed_SKU = df_full.filter(col('ProposedDepth') == 3)
    
    return df_atLeastOneMonth, unchanged_SKU, changed_SKU, df_full 

def Group_and_save_atLeastOneMonth_SKU(unchanged_SKU: pyspark.sql.dataframe.DataFrame, 
                                       changed_SKU: pyspark.sql.dataframe.DataFrame):
    """
    Separate unadjusted SKU to three sheets within same excel file: Capacity_to_avg_qty<3, 
                                             Capacity_to_avg_qty<9 and Capacity_to_avg_qty>=3,
                                             Capacity_to_avg_qty>=9
                                            
    """
    # Separate SKU and save to excel files.
    changed_SKU.toPandas().to_csv('../data/Output/atLeastOneMonth/adjusted_SKU.csv', 
                                 index=False, encoding='utf-8')
    print("Save adjusted SKU(atLeastOneMonth) to Output/atLeastOneMonth/adjusted_SKU.csv")
    
    unchanged_SKU = unchanged_SKU.toPandas()
    unchanged_SKU1 = unchanged_SKU.query('Capacity_to_avg_qty<3')
    unchanged_SKU2 = unchanged_SKU.query('Capacity_to_avg_qty<9 and Capacity_to_avg_qty>=3')
    unchanged_SKU3 = unchanged_SKU.query('Capacity_to_avg_qty>=9')
    writer = ExcelWriter('../data/Output/atLeastOneMonth/unadjusted_SKU.xlsx')
    unchanged_SKU1.to_excel(writer, 'lessThan3', index=False)
    unchanged_SKU2.to_excel(writer, 'between3And9', index=False)
    unchanged_SKU3.to_excel(writer, 'moreThan9', index=False)
    writer.save()
    print("Save unadjusted SKU(atLeastOneMonth) to Output/atLeastOneMonth/unadjusted_SKU.xlsx")

######################
### full month SKU
#####################
def find_and_analysis_fullMonth_SKU(df_atLeastOneMonth: pyspark.sql.dataframe.DataFrame, 
                                    split_month:int, spark) -> pyspark.sql.dataframe.DataFrame:
    """
    Find SKU, which "soldQty < capacity" in every month
    """
    full_month_items = select_full_month_item(df_atLeastOneMonth.toPandas(), month_list = [split_month, 
                                                                split_month+1, split_month+2]) # three month data since split_month
    full_month_SKU_info = get_full_month_SKU_info(full_month_items, 
                                                  df_atLeastOneMonth.select(selected_column_fullMonth), spark).dropDuplicates()
    
    return full_month_SKU_info


def save_fullMonth_SKU(full_month_SKU_info):
    full_month_SKU_info.toPandas().to_csv('../data/Output/fullMonth/qty_less_than_capacity_all_month_SKU.csv', 
                             index=False, encoding='utf-8')
    print("Save fullMonth SKU to Output/fullMonth/qty_less_than_capacity_all_month_SKU.csv")

################
## EDA Analysis
################

def explain_distinct_mean():
    """
    Help function of find_monthly_qty_less_items
    """
    print("DISTINCT means DISTINCT SKU")
    print('The Example below has "2" distinct items')
    print('SKU1 | month1| xxx')
    print('SKU1 | month2| xxx')
    print('SKU2 | month1| xxx')
    print()



def find_monthly_qty_less_items(df, column, time = 1, class_choice = 'Classification'):
    """
    Identify items whose facing or capacity is more than monthly sold qty
    """
    # print total number of items within dataset
    count = df.select("SKU").distinct().count()
    print("There are {} DISTINCT items totally within this dataset".format(count))
    
    
    explain_distinct_mean()
    df1 = df.withColumn('qty_less_than_facing', when((col("totalMonthlyQtySold")* time < col(column)) , 1).otherwise(0))
    df1  = df1.filter(df1.qty_less_than_facing == 1)
    
    if df1.count() == 0:
        print("Cannot find")
        return
    # print total number of items found
    count = df1.select("SKU").distinct().count()
    print("There are {} DISTINCT items satisfy this condition".format(count))
    
    # plot items if we find 
    pdf1 = df1.toPandas() 
    
    # draw monthly plot
    g = sns.catplot(x=column, y="totalMonthlyQtySold", hue=class_choice, col="month",data=pdf1)
    plt.show()
    
    # draw polt, which 
    pdf2 = select_full_month_item(pdf1, month_list = [7, 8, 9])
    # print total number of items found
    count = pdf2["SKU"].nunique()
    print("There are {} DISTINCT items satisfy this condition in three month".format(count))
    
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15,5))
    # plot 1
    sns.scatterplot(x=column, y="totalMonthlyQtySold", hue =class_choice,
                    data=pdf2, ax = axes[0])
    title = "Items whose "+str(column) + " is more than monthly sold qty in three months"
    axes[0].set_title(title, fontsize=15)
    
    # plot 2
    df2 = pd.DataFrame(pdf2.groupby(class_choice)[column].count())
    labels = df2.index
    explode = tuple(0 for i in range(pdf2[class_choice].nunique()))
    # plot 1
    sizes1 = df2[column] 
    axes[1].pie(sizes1, explode = explode, labels=labels, autopct='%1.2f%%',
            shadow=True, startangle=90)
    axes[1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[1].set_title(title, fontsize=15)
    fig.tight_layout()
    plt.show()
    
    return df1, pdf2


def select_full_month_item(data: pandas.core.frame.DataFrame, month_list) -> pandas.core.frame.DataFrame:
    """
    select items which shows in all three months
    """
    df = data.copy()
    df1 = df[df.month == int(month_list[0])]
    df2 = df[df.month == int(month_list[1])]
    df3 = df[df.month == int(month_list[2])]
    df_full = pd.merge(df1, df2, on = 'SKU', how='inner')
    df_full = pd.merge(df_full, df3, on = 'SKU', how='inner')
    total_SKU = df_full.SKU.unique()

    # dataset keep selected SKU  
    df = df[df['SKU'].isin(total_SKU)]

    return df


############
## Group SKU
############

def print_summary_plot(df):
    
    table = df.groupby('SubCategory','month').avg('Qty_GeoMean_by_month_Subcat',
                                                 'NS_GeoMean_by_month_Subcat')\
    .withColumnRenamed('avg(NS_GeoMean_by_month_Subcat)', 'NS_geo_mean')\
    .withColumnRenamed('avg(Qty_GeoMean_by_month_Subcat)', 'qty_geo_mean')
     
    table_df = table.toPandas()
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(15, 12))
    sns.barplot(x="SubCategory", hue="month", y="qty_geo_mean", data=table_df, ax = axes[0])
    axes[0].set_title('Geometric Mean of QtySold', fontsize=20)
    sns.barplot(x="SubCategory", hue="month", y="NS_geo_mean", data=table_df, ax = axes[1])
    axes[1].set_title('Geometric Mean of Net Sale', fontsize=20)
    plt.show()


def find_monthly_qty_less_items_4_group(df, column, class_choice = 'Classification'):
    """
    Identify items whose facing or capacity is more than monthly sold qty
    
    class_choice: Classification or Subcateogory
    """
    #################
    # Separate group
    #################
    # group 0: monthly total qty sold is no less than class_choice
    df0 = df.withColumn('qty_less_than_column', when((col("totalMonthlyQtySold") > col(column)) , 1).otherwise(0))
    df0  = df0.filter(df0.qty_less_than_column == 1)
    
    # group 4: column > (monthly total sold qty *4)
    df4 = df.withColumn('qty_less_than_column4', when((col("totalMonthlyQtySold")* 4 < col(column)) , 1).otherwise(0))
    df4  = df4.filter(df4.qty_less_than_column4 == 1).drop('qty_less_than_column4')
    
    # group 3: (monthly total sold qty *4) > column > (monthly total sold qty *3)
    df3 = group_data_by_threshold(column, df, 3, 4)
    
    # group 2: (monthly total sold qty *3) > column > (monthly total sold qty *2)
    df2 = group_data_by_threshold(column, df, 2, 3)
    
    # group 1: (monthly total sold qty *2) > column > (monthly total sold qty *1)
    df1 = group_data_by_threshold(column, df, 1, 2)
    
    print()
    print('🚩 Group 1: ')
    print_content = "(monthly total sold qty *2) > " + str(column) + ">= (monthly total sold qty *1)"
    print(print_content)
    plot_monthly_data(df1, "SubCategory", column)
    print_summary_plot(df1)
    print()
    print("Details for other columns")
    plot_monthly_bar_plot(df1, "SubCategory", column)
    print()
    print('🚩 Group 2: ')
    print_content = "(monthly total sold qty *3) > " + str(column) + ">= (monthly total sold qty *2)"
    print(print_content)
    plot_monthly_data(df2, "SubCategory", column)
    print_summary_plot(df2)
    print()
    print("Details for other columns")
    plot_monthly_bar_plot(df2, "SubCategory", column)
    print()
    print('🚩 Group 3: ')
    print_content = "(monthly total sold qty *4) > " + str(column) + ">= (monthly total sold qty *3)"
    print(print_content)
    plot_monthly_data(df3, "SubCategory", column)
    print_summary_plot(df3)
    print()
    print("Details for other columns")
    plot_monthly_bar_plot(df3, "SubCategory", column)
    print()
    print('🚩 Group 4: ')
    print_content = str(column) + "> (monthly total sold qty *4)"
    print(print_content)
    plot_monthly_data(df4, "SubCategory", column)
    print_summary_plot(df4)
    print()
    print("Details for other columns")
    plot_monthly_bar_plot(df4, "SubCategory", column)
    
    return df1, df2, df3, df4



def group_data_by_threshold(column, df, lowerbd, upperbd):
    df1 = df.withColumn('qty_less_than_column1', when((col("totalMonthlyQtySold")* lowerbd < col(column)) , 1).otherwise(0))
    df1  = df1.filter(df1.qty_less_than_column1 == 1)
    df1 = df1.withColumn('qty_less_than_column2', when((col("totalMonthlyQtySold")* upperbd >= col(column)) , 1).otherwise(0))
    df1  = df1.filter(df1.qty_less_than_column2 == 1)
    
    print("There are {} DISTINCT items under lowerbound {}, upperbound {}".format(df1.select("SKU").distinct().count(),
                                                                        lowerbd, upperbd))
    return df1.drop('qty_less_than_column2', 'qty_less_than_column1')


def plot_monthly_data(df, class_choice, column):
    
    pdf = df.toPandas() 
    # draw monthly plot
    g = sns.catplot(x=column, y="totalMonthlyQtySold", hue=class_choice, col="month",data=pdf)
    plt.show()
    
def plot_monthly_bar_plot(df, class_choice, column):
    
    df = df.toPandas() 
    
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(18,10))
    df1 = pd.DataFrame(df.groupby([class_choice,'month'])['SellMargin'].sum()).reset_index()
    sns.barplot(x=class_choice, y='SellMargin', hue='month', data=df1, ax = axes[0,0])
    
    df2 = pd.DataFrame(df.groupby([class_choice,'month'])['totalMonthlyNetSale'].sum()).reset_index()
    sns.barplot(x=class_choice, y='totalMonthlyNetSale', hue='month', data=df2, ax = axes[0,1])
    
    df3 = pd.DataFrame(df.groupby([class_choice,'month'])['Facings'].sum()).reset_index()
    sns.barplot(x=class_choice, y='Facings', hue='month', data=df3, ax = axes[1,0])
    
    df4 = pd.DataFrame(df.groupby([class_choice,'month'])['totalMonthlyQtySold'].sum()).reset_index()
    sns.barplot(x=class_choice, y='totalMonthlyQtySold', hue='month', data=df4, ax = axes[1,1])
    
    fig.tight_layout()
    plt.show()
    
def get_full_month_SKU_info(full_month_items: pandas.core.frame.DataFrame, 
                            Groups_output: pyspark.sql.dataframe.DataFrame,
                            spark) -> pyspark.sql.dataframe.DataFrame:
    """
    Get items which Capacity > monthly Qty Sold in all three months
    """
    full_month_SKU = spark.createDataFrame(list(full_month_items['MatID']), StringType()).toDF("MatID")
    full_month_SKU_info = full_month_SKU.join(Groups_output, on = ['MatID'], how = 'left')
    # test joined result 
    #assert full_month_SKU.count()*3 == full_month_SKU_info.count()
    return full_month_SKU_info    
    
#####################################
### Date Cleaning 
####################################

def read_merge_and_clean_data(sales_data_path: str, dist_data_path: str, 
                              split_month: int, begin_date: str, 
                              end_date: str, store_name: str, spark) -> pyspark.sql.dataframe.DataFrame:
    """
    Read, merge and clean all input datasets
    
    Output: three dataframe
        month_merge: whole dataset
        month_merge_early: data before split_month
        month_merge_late: data inlcluded and after split_month
        dist_df: dataset of distributed report
    """
    
    ## load dataset
    df = spark.read.csv(sales_data_path, header=True)
    dist_df = spark.read.csv(dist_data_path, header=True)
    Subcat_info = spark.read.csv("../data/RawData/SubCateogoryInfo.csv", header=True) 
    Vendor_info = spark.read.csv("../data/RawData/Vendor_info.csv", header=True) 
    
    ## Merge dataset
    dist_df = clean_dist_df(dist_df)
    df = Data_clean_and_merge(df, Subcat_info, Vendor_info, store_name, begin_date, end_date)
    month_merge = merge_dataset(df)

    ## Split dataset based on split month
    month_merge_early = month_merge.filter(month_merge.month < split_month)  # e.g April- June
    month_merge_late = month_merge.filter(month_merge.month >= split_month)  # e.g July - Sep
    
    return month_merge, month_merge_early, month_merge_late, dist_df


def convertColumn(df: pyspark.sql.dataframe.DataFrame, names: list, newType) -> pyspark.sql.dataframe.DataFrame:
    """
    A custom function to convert the data type of DataFrame columns
    """
    for name in names: 
        df = df.withColumn(name, df[name].cast(newType))
    return df 

def calculate_geometric_mean(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    
    """
    Calculate the geometirc mean of qtySold and netSale, by adding the new column called `geo_mean`
    """
    df_geometric_mean = df.groupBy('month', 'SubCategory').agg(exp(avg(log(col('totalMonthlyQtySold')))))
    df_geometric_mean = df_geometric_mean.withColumnRenamed('EXP(avg(LOG(totalMonthlyQtySold)))', 
                                                            'Qty_GeoMean_by_month_Subcat')
    
    df_geometric_mean2 = df.groupBy('month', 'SubCategory').agg(exp(avg(log(col('totalMonthlyNetSale')))))
    df_geometric_mean2 = df_geometric_mean2.withColumnRenamed('EXP(avg(LOG(totalMonthlyNetSale)))', 
                                                            'NS_GeoMean_by_month_Subcat')
    
    # join the column to the original dataset
    df_new = df.join(df_geometric_mean, on = ['month', 'SubCategory'], how = 'inner')
    df_new = df_new.join(df_geometric_mean2, on = ['month', 'SubCategory'], how = 'inner')
    #assert df.count() == df_new.count()
    return df_new
    


def calculate_mean_std_and_geometric_mean(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    Calculate the mean, std and geometric mean of qtySold and netSale for each subcategory and each month
    """
    df_group = df.groupby('month', 'SubCategory')
    df = calculate_geometric_mean(df)
    df_group_sum = df_group.avg('totalMonthlyQtySold', 'totalMonthlyNetSale')\
    .withColumnRenamed('avg(totalMonthlyQtySold)', "Qty_mean_by_month_Subcat")\
    .withColumnRenamed('avg(totalMonthlyNetSale)', "NS_mean_by_month_Subcat")
    
    df_group_std = df_group.agg(stddev('totalMonthlyQtySold'))\
    .withColumnRenamed('stddev_samp(totalMonthlyQtySold)', "Qty_std_by_month_Subcat")
    
    df_group_std2 = df_group.agg(stddev('totalMonthlyNetSale'))\
    .withColumnRenamed('stddev_samp(totalMonthlyNetSale)', "NS_std_by_month_Subcat")
    
    # join to get final dataset
    df = df.join(df_group_sum, on = ['month', 'SubCategory'], how = 'inner')
    df = df.join(df_group_std, on = ['month', 'SubCategory'], how = 'inner')
    df = df.join(df_group_std2, on = ['month', 'SubCategory'], how = 'inner')
    return df
                                                            
def calculate_Capacity_to_sales(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    1. Capacity / Qty sold
    2. Capacity / NetSales
    """
    df = df.withColumn("Capacity_to_qty", (df.Capacity / df.totalMonthlyQtySold))
    df = df.withColumn("Capacity_to_sales", (df.Capacity / df.totalMonthlyNetSale))
    return df  

def calculate_Depths(df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    Depth =  Capacity / Facings
    ProposedDepth = 3, if Depth >= 4. Otherwise empty
    VarianceDepth = ProposedDepth - Depth (should be negative)
    """
    df = df.withColumn("Depth", (df.Capacity / df.Facings))
    df = df.withColumn("ProposedDepth",  when(col('Depth') >=4, 3).otherwise(''))
    df = df.withColumn("VarianceDepth",  when(col('Depth') >=4, (df.ProposedDepth - df.Depth)).otherwise(''))
    return df  



def clean_dist_df(dist_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    # filter data
    dist_df = dist_df.select("Name", "Facings", "Capacity", 'Days Supply','Classification', 'Mat ID', '# POGs')
    
    ### Rename column
    dist_df = dist_df.withColumnRenamed("Name", "SKU")
    dist_df = dist_df.withColumnRenamed("Days Supply", "DaysSupply")
    dist_df = dist_df.withColumnRenamed("Mat ID", "MatID")
    dist_df = dist_df.withColumnRenamed("# POGs", "POGS")
    
    # Conver columns to `FloatType()`
    dist_df = dist_df.withColumn("Facings", dist_df.Facings.cast('float'))
    dist_df = dist_df.withColumn("Capacity", dist_df.Capacity.cast('float'))
    dist_df = dist_df.withColumn("DaysSupply", dist_df.DaysSupply.cast('float'))
    dist_df = dist_df.withColumn("MatID", dist_df.MatID.cast('integer'))
    dist_df = dist_df.withColumn("POGS", dist_df.POGS.cast('integer'))
    return dist_df
    

def Data_clean_and_merge(df: pyspark.sql.dataframe.DataFrame,
                        Subcat_info: pyspark.sql.dataframe.DataFrame, 
                        Vendor_info: pyspark.sql.dataframe.DataFrame,
                        store_name: str,
                        begin_date = "2019-04-01", end_date = "2019-10-01") -> pyspark.sql.dataframe.DataFrame:
    # select useful columns
    Subcat_info = Subcat_info.select('SKU', 'SubCategory')
    Vendor_info = Vendor_info.select('SKU', 'Vendor')
    # clean data entry: remove ID
    split_col = split(Subcat_info['SKU'], '-')
    Subcat_info = Subcat_info.withColumn('MatID', split_col.getItem(0))
    Subcat_info = Subcat_info.withColumn('MatID', regexp_replace(col("MatID"), "[ZNDF]", "")) # remove letters from matID
    
    split_col2 = split(Vendor_info['SKU'], '-')
    Vendor_info = Vendor_info.withColumn('MatID', split_col2.getItem(0))
    Vendor_info = Vendor_info.withColumn('MatID', regexp_replace(col("MatID"), "[ZNDF]", "")) # remove letters from matID
    
    split_col = split(Subcat_info['SubCategory'], '-')
    split_col2 = split(Vendor_info['Vendor'], '-')
    Subcat_info = Subcat_info.withColumn('SubCategory', split_col.getItem(1))
    Vendor_info = Vendor_info.withColumn('Vendor', split_col2.getItem(1))
    # filter data
    df = df.select("Date", "Store", 'item','POS Gross Sales', 'POS Net Sales', 'POS Total Discount', 'POS Qty Sold',
                  'POS COGS (INV)')
    
    # Check only one store
    df = df.filter(df.Store == store_name)
    
    # Remove comma from integer (e.g. 1,333 to 1333)
    udf = UserDefinedFunction(lambda x: re.sub(',','',x), StringType())
    #num_columns = ['TotalDiscount', 'QtySold', 'GrossSales', 'NetSales', 'COGS']
    df = df.select(*[udf(column).alias(column) for column in df.columns])
    
    # filter data, and keep only half years
    # Convert Date column to timestamp 
    df = df.withColumn("Date", to_timestamp(df.Date, "yyyyMM"))
    df= df.filter(df.Date >= begin_date) 
    df = df.filter(df.Date < end_date)  # April - Sep
    
    # separate Item name to SKU and ID
    split_col = split(df['item'], '-')
    df = df.withColumn('MatID', split_col.getItem(0))
    df = df.withColumn('MatID', regexp_replace(col("MatID"), "[ZNDF]", "")) # remove letters from matID
    df = df.withColumn('SKU', split_col.getItem(1))

    ### Rename column
    df = df.withColumnRenamed("Sales Type", "SalesType")
    df = df.withColumnRenamed("POS Gross Sales", "GrossSales")
    df = df.withColumnRenamed("POS Net Sales", "NetSales")
    df = df.withColumnRenamed("POS Total Discount", "TotalDiscount")
    df = df.withColumnRenamed("POS Qty Sold", "QtySold")
    df = df.withColumnRenamed("POS COGS (INV)", "COGS")
    
    # Assign all column names to `columns`
    columns = ['TotalDiscount', 'QtySold', 'GrossSales', 'NetSales', 'COGS']
    # Conver the `df` columns to `FloatType()`
    df = convertColumn(df, columns, FloatType())
    
    # drop unnecessary items
    columns_to_drop = ['item']
    df = df.drop(*columns_to_drop)
    # Convert Date column to timestamp 
    df = df.withColumn("Date", to_timestamp(df.Date, "yyyyMM"))
    
    # Create the new columns 
    df = df.withColumn("Price", df.GrossSales / df.QtySold)
    df = df.withColumn("FrontMargin", (df.GrossSales + df.COGS))
    df = df.withColumn("SellMargin", (df.NetSales + df.COGS))

    # add subcategory column
    df = df.join(Subcat_info.select("MatID", 'SubCategory'), on=["MatID"], how="left")
    df = df.join(Vendor_info.select("MatID", 'Vendor'), on=["MatID"], how="left")
    return df

def merge_dataset(df: pyspark.sql.dataframe.DataFrame)-> pyspark.sql.dataframe.DataFrame:
    # Generate sale information for each product in each month
    month_df = df.select('MatID', "SKU", year("Date").alias('year'), month("Date").alias('month'), 
                         'GrossSales', 'NetSales', 'COGS', 'QtySold','Price', 
                         'SellMargin', 'FrontMargin','SubCategory', 'Vendor').groupBy("month",'MatID',"SubCategory", 'Vendor')
    ## get the average net-sales of each product 
    month_avg_NetSale = month_df.avg("NetSales").withColumnRenamed("avg(NetSales)", "totalMonthlyNetSale")
    ## get the average gross-sales of each product 
    month_avg_GrossSale = month_df.avg("GrossSales").withColumnRenamed("avg(GrossSales)", "totalMonthlyGrossSale")
    month_avg_COGS = month_df.avg("COGS").withColumnRenamed("avg(COGS)", "avgCOGS")
    month_avg_QtySold = month_df.avg("QtySold").withColumnRenamed("avg(QtySold)", "totalMonthlyQtySold")
    month_avg_Price = month_df.avg("Price").withColumnRenamed("avg(Price)", "Price")
    month_avg_SM = month_df.avg("SellMargin").withColumnRenamed("avg(SellMargin)", "SellMargin")
    month_avg_FM = month_df.avg("FrontMargin").withColumnRenamed("avg(FrontMargin)", "avgFrontMargin")
    
    month_merge = month_avg_NetSale.join(month_avg_GrossSale, on=["MatID", 'month','SubCategory', 'Vendor'], how="inner")
    month_merge = month_merge.join(month_avg_COGS, on=["MatID", 'month','SubCategory', 'Vendor'], how="inner")
    month_merge = month_merge.join(month_avg_QtySold, on=["MatID", 'month','SubCategory', 'Vendor'], how="inner")
    month_merge = month_merge.join(month_avg_Price, on=["MatID", 'month','SubCategory', 'Vendor'], how="inner")
    month_merge = month_merge.join(month_avg_SM, on=["MatID", 'month','SubCategory', 'Vendor'], how="inner")
    month_merge = month_merge.join(month_avg_FM, on=["MatID", 'month','SubCategory', 'Vendor'], how="inner")
    
    return month_merge


def user_put():
    """
    User Input from interface
    """
    print("""
📌Notes:
1. The sales_newzealand.csv must have the sales data from the store you want to test: (NZDF.AKL108 or NZDF.AKL109 or both)

2. The decision of start date is based on the "generating date of Distribution Report".

3. If start date is 2019-04-01, then end_date will be 2019-10-01, the month begin to test is July. 
   If start date is 2019-06-01, then end_date will be 2019-12-01, the month begin to test is Sep. 
   
4. The file name of Distribution Report must contain 'Departures' or 'Arrivals':
   Departures: NZDF.AKL108
   Arrivals: NZDF.AKL109
   
5. The output folder name is ‘Output’ only. Remeber rename it to distinguish different input data: 
   for example: Rename to Output_Sep_108, Output_July_108, Output_Sep_109
   
    """)
    sales_data_path = input("""Enter the path of 6 month sales data, 
    e.g. ../data/RawData/sales_newzealand.csv:""")
    
    dist_data_path = input("""Enter the path of disrtibuted report data, 
    e.g. ../data/RawData/Distribution Report - Auckland Departures - July19.csv or ../data/RawData/Distribution Report- Auckland Departures - Jan2020.csv or ../data/RawData/Distribution Report-AKL Arrivals-Jan 2020.csv:""")

    # test given store and distribution report is correct
    if ('Arrivals' not in dist_data_path and 'Departures' not in dist_data_path):
        print("\n ❌ERROR: file name of Distribution Report must contain 'Departures' or 'Arrivals'")
        sys.exit(errno.EACCES)
        
    elif 'Arrivals' in dist_data_path:
        store_name = 'NZDF.AKL109'
    else: # Departures
        store_name = 'NZDF.AKL108'
    print("Store name is {}".format(store_name))
          
    start_date = input("""Input start date, 2019-04-01 or 2019-06-01: """)
    
    # genterate end_date
    if start_date == '2019-04-01':
        end_date = '2019-10-01'
        split_month = 7
    elif start_date == '2019-06-01':
        end_date = '2019-12-01'
        split_month = 9
    else:
        print("\n ❌ERROR: Start date must be 2019-06-01 or 2019-06-01")
        sys.exit(errno.EACCES)
    print('end date is {},  the month begin to test is {}\n\n'.format(end_date, split_month))
   
    return sales_data_path, dist_data_path, start_date, end_date, split_month, store_name