## Out of Store Check

#### Content:
# 1. Identify OOS items
# 2. Check sale Percentage

import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import pyplot
import plotly.express as px
import plotly.graph_objects as go
from OOS_check_helper import *
import warnings
warnings.filterwarnings('ignore')
import os
import sys
import subprocess
sns.set_style("whitegrid")



def main():
    ## read from standard input 
    category = input("Enter the category: ")    # input the category of the dataset belongs to
    clean_data = input("Have you stored cleaned data yet? (y/n): ") 
    
    ## data cleaning and read in 
    data_list = data_processing(category, clean_data)
    
    ## merge muptiple datasets
    df = merge_data(data_list)
    
    ## OOS check 
    output_data = check_OOS_by_rules(df)
    output_data = convert_to_string_times(output_data)
    
    ## Output dataset
    output_data.to_csv('../../data/output/OOS_beverage.csv', index=False, encoding='utf-8')
    
    ## Analysis OOS 7 days product and output the result
    OOS_7_days_item_analysis = output_OOS_7_days_analysis(df, output_data, category)
    file_name = '../../data/output/OOS_7_days_analysis/OOS_7_days_'+category+'.csv'
    OOS_7_days_item_analysis.to_csv(file_name, index=False, encoding='utf-8')    
        

def data_processing(category, clean_data):
    """
    Clean and read data
    """
    # clean data
    if clean_data == 'y': # when we have new raw data
        print("Beginning data processing...")
        if category == 'beverage':
            data_list = os.system("python clean_data_script/clean_data_beverage.py y")
           
        elif category =='confectionery':
            data_list = os.system("python clean_data_script/clean_data_confectionery.py y")
        
        print("Data processing finished.") 
    
    data_list = read_data(category)
    return data_list
    

def read_data(category):
    """
    Return a data_list containing a list of dataframe
    """
    if  category =='beverage':   
        data1 = pd.read_csv("../../data/cleanedData/BNC/Beverage/cleaned_SYD BNC 11.19 Data dail Beverages.csv")
        data_list = [data1]
    elif category == 'Confectionery':
        data1 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_TSV BNC 11.19 Data daily Confec.csv')
        data2 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_AKL BNC 11.19 Data daily Confec.csv')
        data3 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_AVV BNC 11.19 Data daily Confec.csv')
        data4 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_BNK BNC 11.19 Data daily Confec.csv')
        data5 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_CNS BNC 11.19 Data daily Confec.csv')
        data6 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_MEL BNC 11.19 Data daily Confec.csv')
        data7 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_OOL BNC 11.19 Data daily Confec.csv')
        data8 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_SYD BNC 11.19 Data daily Confec.csv')
        data9 = pd.read_csv("../../data/cleanedData/BNC/Confectionery/cleaned_PER BNC 11.19 data daily Confec.csv")
        data10 = pd.read_csv('../../data/cleanedData/BNC/Confectionery/cleaned_ADL BNC 11.19 data daily Confec.csv')
        data_list = [data1, data2, data3, data4, data5, data6, data7, data8, data9, data10]
    return data_list    
    
    
if __name__ == "__main__":
    main()


