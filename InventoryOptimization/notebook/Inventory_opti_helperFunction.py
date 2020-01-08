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
#from pyspark.sql.functions import when
from pyspark.sql.functions import UserDefinedFunction
import re





###############################
### EDA
###############################    

def pie_chart_margin(column: str, df: pandas.core.frame.DataFrame, title1: str, title2: str, explode: tuple):
    
    """
    GroupBy Classification, visualization by margin
    """

    df1 = pd.DataFrame(df.groupby('Classification')[column].sum())
    df2 = pd.DataFrame(df.groupby('Classification')[column].sum())
    df3 = pd.DataFrame(df.groupby('Classification')['totalMonthlyNetSale'].sum())
    df4 = df2['SellMargin']/df3['totalMonthlyNetSale']
    
    labels = df1.index
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15,5))
    # plot 1
    sizes1 = df1[column] 
    axes[0].pie(sizes1, explode=explode, labels=labels, autopct='%1.2f%%',
            shadow=True, startangle=90)
    axes[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[0].set_title(title1, fontsize=15)
    
    # plot 2
    axes[1].pie(df4, explode=explode, labels=labels, autopct='%1.2f%%',
            shadow=True, startangle=90)
    axes[1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[1].set_title(title2, fontsize=15)
    
    # plot 4
    df5 = pd.DataFrame(df.groupby(['Classification','month'])[column].sum()).reset_index()
    axes[2] = sns.boxplot(x="Classification", y=column, data=df)
    axes[2] = sns.swarmplot(x="Classification", y=column, data=df, color=".25")
    axes[2].set_title(title2, fontsize=15)
    
    fig.tight_layout()
    plt.show()
    
    # plot 3
    g = sns.factorplot(x = "Classification", y = column, 
                             data=df5, kind='bar', hue = 'month')
    g.fig.suptitle(title1, fontsize=15)
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
    axes[0].pie(sizes1, explode=explode, labels=labels, autopct='%1.2f%%',
            shadow=True, startangle=90)
    axes[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[0].set_title(title1, fontsize=15)
    
    # plot 2
    sizes2 = df2[column] 
    axes[1].pie(sizes2, explode=explode, labels=labels, autopct='%1.2f%%',
            shadow=True, startangle=90)
    axes[1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    axes[1].set_title(title2, fontsize=15)
    
    fig.tight_layout()
    plt.show()
    
    # plot 3
    fig, axes = plt.subplots(figsize=(10, 5))
    df1 = pd.DataFrame(df.groupby([calss_choice,'month'])[column].sum()).reset_index()
    axes = sns.barplot(x=calss_choice, y=column, hue = 'month', data=df1)
    axes.set_title(title1, fontsize=15)
    plt.show()
    
    # plot 4
    fig, axes = plt.subplots(figsize=(10, 5))
    df2 = pd.DataFrame(df.groupby([calss_choice,'month'])[column].mean()).reset_index()
    axes = sns.barplot(x=calss_choice, y=column, hue = 'month', data=df2)
    axes.set_title(title2, fontsize=15)
    plt.show()
    
    fig.tight_layout()
    plt.show()
    print()
    print("------------------------------------------------------------------")

###############################
##### check individual items
###############################   

def find_check_item(month_merge:pyspark.sql.dataframe.DataFrame, 
                    dist_df:pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    checked item
    """
    check_df = dist_df.join(month_merge, on=["MatID"], how="left").fillna(0, subset=['totalMonthlyGrossSale']) 
    check_item = check_df.filter(check_df.totalMonthlyGrossSale == 0)
    check_item = check_item.select('SKU', 'MatID', 'month', 'Price', 'Facings', 'Capacity','Classification', 
                               'SubCategory', 'totalMonthlyNetSale', 'totalMonthlyQtySold', 'SellMargin')
    return check_item


def find_removed_item(month_merge_late:pyspark.sql.dataframe.DataFrame,
                      month_merge_early:pyspark.sql.dataframe.DataFrame,
                      dist_df:pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    removed item
    """
    Removed_df = dist_df.join(month_merge_late, on=["MatID"], how="left").fillna(0, subset=['totalMonthlyGrossSale'])
    Removed_df = dist_df.join(month_merge_early, on=["MatID"], how="inner").fillna(0, subset=['totalMonthlyGrossSale'])
    Removed_item = Removed_df.filter(Removed_df.totalMonthlyGrossSale == 0)
    Removed_item = Removed_item.select('SKU', 'MatID', 'month', 'Price', 'Facings', 'Capacity','Classification', 
                               'SubCategory', 'totalMonthlyNetSale', 'totalMonthlyQtySold', 'SellMargin')
    return Removed_item


def find_new_item(month_merge_late:pyspark.sql.dataframe.DataFrame,
                 dist_df:pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    """
    new item
    """
    New_df = dist_df.join(month_merge_late, on=["MatID"], how="right").fillna(0, subset=['totalMonthlyGrossSale'])
    New_item = New_df.filter(New_df.totalMonthlyGrossSale != 0) # new item is sold during July- Sep
    New_item = New_item.filter(col("Classification").isNull()) # new item has no classification records
    New_item = New_item.select('SKU', 'MatID', 'month', 'Price', 'Facings', 'Capacity','Classification', 
                                   'SubCategory', 'totalMonthlyNetSale', 'totalMonthlyQtySold', 'SellMargin')
    return New_item 

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
    
    return df1


def select_full_month_item(data, month_list):
    """
    select items which shows in all three months
    """
    df = data.copy()
    df1 = df[df.month == str(month_list[0])]
    df2 = df[df.month == str(month_list[1])]
    df3 = df[df.month == str(month_list[2])]
    df_full = pd.merge(df1, df2, on = 'SKU', how='inner')
    df_full = pd.merge(df_full, df3, on = 'SKU', how='inner')
    total_SKU = df_full.SKU.unique()

    # dataset keep selected SKU  
    df = df[df['SKU'].isin(total_SKU)]

    return df

def find_too_high_facing_items(df, class_choice = 'Classification'):
    """
    Identify items facing is above 95% items, but qty sold is less than 85% items.
    """
    
    facing_threshold = df.approxQuantile('Facings',[0.95],0.01)[0] # 0.01 is relativeError
    qty_threshold = df.approxQuantile('totalMonthlyQtySold',[0.85],0.01)[0]
    print("The 95% facing is {}".format(facing_threshold))
    print("The 85% qty sold is {}".format(qty_threshold))
    high_facing = df[df.Facings > facing_threshold]
    high_facing = high_facing[high_facing.totalQtySold < qty_threshold]
    
    pdf1 = high_facing.toPandas() 
    plt.figure(figsize=(18,8))
    ax = sns.scatterplot(x="Facings", y="totalMonthlyQtySold", hue = class_choice,
                    data=pdf1)
    ax.set_title("Facings VS QtySold for items with too high facing", fontsize=20)
    plt.show()
    
    return high_facing


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
    df0 = df.withColumn('qty_less_than_facing', when((col("totalMonthlyQtySold") > col(column)) , 1).otherwise(0))
    df0  = df0.filter(df0.qty_less_than_facing == 1)
    
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
    print_content = "(monthly total sold qty *2) > " + str(column) + ">= (monthly total sold qty *1)"
    print(print_content)
    plot_monthly_data(df1, "SubCategory", column)
    print_summary_plot(df1)
    print()
    print("Details for other columns")
    plot_monthly_bar_plot(df1, "SubCategory", column)
    print()
    print_content = "(monthly total sold qty *3) > " + str(column) + ">= (monthly total sold qty *2)"
    print(print_content)
    plot_monthly_data(df2, "SubCategory", column)
    print_summary_plot(df2)
    print()
    print("Details for other columns")
    plot_monthly_bar_plot(df2, "SubCategory", column)
    print()
    print_content = "(monthly total sold qty *4) > " + str(column) + ">= (monthly total sold qty *3)"
    print(print_content)
    plot_monthly_data(df3, "SubCategory", column)
    print_summary_plot(df3)
    print()
    print("Details for other columns")
    plot_monthly_bar_plot(df3, "SubCategory", column)
    print()
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
    
    fig, axes = plt.subplots(figsize=(15,5))
    df1 = pd.DataFrame(df.groupby([class_choice,'month'])['SellMargin'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='SellMargin', hue='month', data=df1)
    plt.show()
    
    fig, axes = plt.subplots(figsize=(15,5))
    df2 = pd.DataFrame(df.groupby([class_choice,'month'])['totalMonthlyNetSale'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='totalMonthlyNetSale', hue='month', data=df2)
    plt.show()
    
    fig, axes = plt.subplots(figsize=(15,5))
    df3 = pd.DataFrame(df.groupby([class_choice,'month'])['Facings'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='Facings', hue='month', data=df3)
    plt.show()
    
    fig, axes = plt.subplots(figsize=(15,5))
    df4 = pd.DataFrame(df.groupby([class_choice,'month'])['totalMonthlyQtySold'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='totalMonthlyQtySold', hue='month', data=df4)
    plt.show()
    
    fig.tight_layout()
    plt.show()
    
    
    
#####################################
### Date Cleaning 
####################################

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



def clean_dist_df(dist_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    # filter data
    dist_df = dist_df.select("Name", "Facings", "Capacity", 'Days Supply','Classification', 'Mat ID')
    
    ### Rename column
    dist_df = dist_df.withColumnRenamed("Name", "SKU")
    dist_df = dist_df.withColumnRenamed("Days Supply", "DaysSupply")
    dist_df = dist_df.withColumnRenamed("Mat ID", "MatID")
    
    # Conver columns to `FloatType()`
    dist_df = dist_df.withColumn("Facings", dist_df.Facings.cast('float'))
    dist_df = dist_df.withColumn("Capacity", dist_df.Capacity.cast('float'))
    dist_df = dist_df.withColumn("DaysSupply", dist_df.DaysSupply.cast('float'))
    dist_df = dist_df.withColumn("MatID", dist_df.MatID.cast('integer'))
    return dist_df
    

def Data_clean_and_merge(df: pyspark.sql.dataframe.DataFrame,
                        dist_df: pyspark.sql.dataframe.DataFrame,
                        Subcat_info: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    # select useful columns
    Subcat_info = Subcat_info.select('SKU', 'SubCategory')
    # clean data entry: remove ID
    split_col = split(Subcat_info['SKU'], '-')
    Subcat_info = Subcat_info.withColumn('MatID', split_col.getItem(0))
    Subcat_info = Subcat_info.withColumn('MatID', regexp_replace(col("MatID"), "[ZNDF]", "")) # remove letters from matID
    
    split_col = split(Subcat_info['SubCategory'], '-')
    Subcat_info = Subcat_info.withColumn('SubCategory', split_col.getItem(1))
    
    # filter data
    df = df.select("Date", "Store", 'item','POS Gross Sales', 'POS Net Sales', 'POS Total Discount', 'POS Qty Sold',
                  'POS COGS (INV)')
    
    # Check only one store
    df = df.filter(df.Store == "NZDF.AKL108")
    
    # Remove comma from integer (e.g. 1,333 to 1333)
    udf = UserDefinedFunction(lambda x: re.sub(',','',x), StringType())
    #num_columns = ['TotalDiscount', 'QtySold', 'GrossSales', 'NetSales', 'COGS']
    df = df.select(*[udf(column).alias(column) for column in df.columns])
    
    # filter data, and keep only half years
    # Convert Date column to timestamp 
    df = df.withColumn("Date", to_timestamp(df.Date, "yyyyMM"))
    df= df.filter(df.Date >= "2019-04-01") 
    df = df.filter(df.Date < "2019-10-01")  # April - Sep
    
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
    
    return df

def merge_dataset(df: pyspark.sql.dataframe.DataFrame)-> pyspark.sql.dataframe.DataFrame:
    # Generate sale information for each product in each month
    month_df = df.select('MatID', "SKU", year("Date").alias('year'), month("Date").alias('month'), 
                         'GrossSales', 'NetSales', 'COGS', 'QtySold','Price', 
                         'SellMargin', 'FrontMargin','SubCategory').groupBy("month",'MatID',"SubCategory")
    ## get the average net-sales of each product 
    month_avg_NetSale = month_df.avg("NetSales").withColumnRenamed("avg(NetSales)", "totalMonthlyNetSale")
    ## get the average gross-sales of each product 
    month_avg_GrossSale = month_df.avg("GrossSales").withColumnRenamed("avg(GrossSales)", "totalMonthlyGrossSale")
    month_avg_COGS = month_df.avg("COGS").withColumnRenamed("avg(COGS)", "avgCOGS")
    month_avg_QtySold = month_df.avg("QtySold").withColumnRenamed("avg(QtySold)", "totalMonthlyQtySold")
    month_avg_Price = month_df.avg("Price").withColumnRenamed("avg(Price)", "Price")
    month_avg_SM = month_df.avg("SellMargin").withColumnRenamed("avg(SellMargin)", "SellMargin")
    month_avg_FM = month_df.avg("FrontMargin").withColumnRenamed("avg(FrontMargin)", "avgFrontMargin")
    
    month_merge = month_avg_NetSale.join(month_avg_GrossSale, on=["MatID", 'month','SubCategory'], how="inner")
    month_merge = month_merge.join(month_avg_COGS, on=["MatID", 'month','SubCategory'], how="inner")
    month_merge = month_merge.join(month_avg_QtySold, on=["MatID", 'month','SubCategory'], how="inner")
    month_merge = month_merge.join(month_avg_Price, on=["MatID", 'month','SubCategory'], how="inner")
    month_merge = month_merge.join(month_avg_SM, on=["MatID", 'month','SubCategory'], how="inner")
    month_merge = month_merge.join(month_avg_FM, on=["MatID", 'month','SubCategory'], how="inner")
    
    return month_merge