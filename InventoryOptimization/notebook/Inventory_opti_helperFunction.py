import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import pyplot
import plotly.express as px
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
#from pyspark.sql.functions import when
from pyspark.sql.functions import UserDefinedFunction
import re

###############################
##### EDA
###############################    


def pie_chart_margin(column, df, title1, title2, explode):

    df1 = pd.DataFrame(df.groupby('Classification')[column].sum())
    df2 = pd.DataFrame(df.groupby('Classification')[column].sum())
    df3 = pd.DataFrame(df.groupby('Classification')['avgNetSale'].sum())
    df4 = df2['avgSellMargin']/df3['avgNetSale']
    
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
    
def pie_chart_classification(column, df, title1, title2, explode, calss_choice):

    df1 = pd.DataFrame(df.groupby(calss_choice)[column].sum()
                       /(df[column].sum()) * 100)
    df2 = pd.DataFrame(df.groupby(calss_choice)[column].mean()
                       /(df[column].mean()) * 100)
    
    labels = df1.index
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15, 5))
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
    
    # plot 2
    sizes2 = df2[column] 
    axes[2] = sns.boxplot(x=calss_choice, y=column, data=df)
    axes[2] = sns.swarmplot(x=calss_choice, y=column, data=df, color=".25")
    axes[2].set_title(title1, fontsize=15)
    
    fig.tight_layout()
    plt.show()

###############################
##### check individual items
###############################    

def find_monthly_qty_less_items(df, column, time = 1, class_choice = 'Classification'):
    """
    Identify items whose facing or capacity is more than monthly sold qty
    """
    df1 = df.withColumn('qty_less_than_facing', when((col("totalQtySold")* time < col(column)) , 1).otherwise(0))
    df1  = df1.filter(df1.qty_less_than_facing == 1)
    
    if df1.count() == 0:
        print("Cannot find")
        return
    # print total number of items found
    count = df1.select("SKU").distinct().count()
    print("There are {} items satisfy this condition".format(count))
    
    # plot items if we find 
    pdf1 = df1.toPandas() 
    
    # draw monthly plot
    g = sns.catplot(x=column, y="totalQtySold", hue=class_choice, col="month",data=pdf1)
    plt.show()
    
    # draw polt, which 
    pdf2 = select_full_month_item(pdf1, month_list = [7, 8, 9])
    # print total number of items found
    count = pdf2["SKU"].nunique()
    print("There are {} items satisfy this condition in three month".format(count))
    
    plt.figure(figsize=(10,5))
    ax = sns.scatterplot(x=column, y="totalQtySold", hue =class_choice,
                    data=pdf2)
    title = "Items whose "+str(column) + " is more than monthly sold qty in three months"
    ax.set_title(title, fontsize=20)
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
    qty_threshold = df.approxQuantile('totalQtySold',[0.85],0.01)[0]
    print("The 95% facing is {}".format(facing_threshold))
    print("The 85% qty sold is {}".format(qty_threshold))
    high_facing = df[df.Facings > facing_threshold]
    high_facing = high_facing[high_facing.totalQtySold < qty_threshold]
    
    pdf1 = high_facing.toPandas() 
    plt.figure(figsize=(18,8))
    ax = sns.scatterplot(x="Facings", y="totalQtySold", hue = class_choice,
                    data=pdf1)
    ax.set_title("Facings VS QtySold for items with too high facing", fontsize=20)
    plt.show()
    
    return high_facing


############
## Group SKU
############
def find_monthly_qty_less_items_4_group(df, column, class_choice = 'Classification'):
    """
    Identify items whose facing or capacity is more than monthly sold qty
    
    class_choice: Classification or Subcateogory
    """
    #################
    # Separate group
    #################
    # group 0: monthly total qty sold is no less than class_choice
    df0 = df.withColumn('qty_less_than_facing', when((col("totalQtySold") > col(column)) , 1).otherwise(0))
    df0  = df0.filter(df0.qty_less_than_facing == 1)
    
    # group 4: facing > (monthly total sold qty *4)
    df4 = df.withColumn('qty_less_than_facing4', when((col("totalQtySold")* 4 < col(column)) , 1).otherwise(0))
    df4  = df4.filter(df4.qty_less_than_facing4 == 1)
    
    # group 3: (monthly total sold qty *4) > facing > (monthly total sold qty *3)
    df3 = group_data_by_threshold(column, df, 3, 4)
    
    # group 2: (monthly total sold qty *3) > facing > (monthly total sold qty *2)
    df2 = group_data_by_threshold(column, df, 2, 3)
    
    # group 1: (monthly total sold qty *2) > facing > (monthly total sold qty *1)
    df1 = group_data_by_threshold(column, df, 1, 2)
    
    print()
    print("(monthly total sold qty *2) > facing >= (monthly total sold qty *1)")
    plot_monthly_data(df1, "SubCategory", column)
    plot_monthly_bar_plot(df1, "SubCategory", column)
    print()
    
    print("(monthly total sold qty *3) > facing >= (monthly total sold qty *2)")
    plot_monthly_data(df2, "SubCategory", column)
    plot_monthly_bar_plot(df2, "SubCategory", column)
    print()
    
    print("(monthly total sold qty *4) > facing >= (monthly total sold qty *3)")
    plot_monthly_data(df3, "SubCategory", column)
    plot_monthly_bar_plot(df3, "SubCategory", column)
    print()
    
    print("facing > (monthly total sold qty *4)")
    plot_monthly_data(df4, "SubCategory", column)
    plot_monthly_bar_plot(df4, "SubCategory", column)
    
    return df1

def group_data_by_threshold(column, df, lowerbd, upperbd):
    df1 = df.withColumn('qty_less_than_facing1', when((col("totalQtySold")* lowerbd < col(column)) , 1).otherwise(0))
    df1  = df1.filter(df1.qty_less_than_facing1 == 1)
    df1 = df1.withColumn('qty_less_than_facing2', when((col("totalQtySold")* upperbd >= col(column)) , 1).otherwise(0))
    df1  = df1.filter(df1.qty_less_than_facing2 == 1)
    
    print("There are {} items under lowerbound {}, upperbound {}".format(df1.select("SKU").distinct().count(),
                                                                        lowerbd, upperbd))
    return df1

def plot_monthly_data(df, class_choice, column):
    
    pdf = df.toPandas() 
    # draw monthly plot
    g = sns.catplot(x=column, y="totalQtySold", hue=class_choice, col="month",data=pdf)
    plt.show()
    
def plot_monthly_bar_plot(df, class_choice, column):
    
    df = df.toPandas() 
    
    fig, axes = plt.subplots(figsize=(15,5))
    df1 = pd.DataFrame(df.groupby([class_choice,'month'])['SellMargin'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='SellMargin', hue='month', data=df1)
    plt.show()
    
    fig, axes = plt.subplots(figsize=(15,5))
    df2 = pd.DataFrame(df.groupby([class_choice,'month'])['totalNetSale'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='totalNetSale', hue='month', data=df2)
    plt.show()
    
    fig, axes = plt.subplots(figsize=(15,5))
    df3 = pd.DataFrame(df.groupby([class_choice,'month'])['Facings'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='Facings', hue='month', data=df3)
    plt.show()
    
    fig, axes = plt.subplots(figsize=(15,5))
    df4 = pd.DataFrame(df.groupby([class_choice,'month'])['totalQtySold'].sum()).reset_index()
    axes = sns.barplot(x=class_choice, y='totalQtySold', hue='month', data=df4)
    plt.show()
    
    fig.tight_layout()
    plt.show()