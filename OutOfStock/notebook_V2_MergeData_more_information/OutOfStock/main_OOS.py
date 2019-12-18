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
    clean_data_choice = input("Do you need clean data? (y/n): ") 
    
    ## data cleaning and read in 
    data_list = data_processing(clean_data_choice)
    
    ## merge muptiple datasets
    df = merge_data(data_list)
    
    ## OOS check 
    output_data = check_OOS_by_rules(df)
    output_data = convert_to_string_times(output_data)
    
    ## Output dataset
    output_data.to_csv('../../data/output/OOS_SKU.csv', index=False, encoding='utf-8')
    
    ## Analysis OOS 7 days product and output the result
    #OOS_7_days_item_analysis = output_OOS_7_days_analysis(df, output_data)
    #file_name = '../../data/output/OOS_7_days_analysis/OOS_7_days_'+category+'.csv'
    #OOS_7_days_item_analysis.to_csv(file_name, index=False, encoding='utf-8')    
        

def data_processing(clean_data_choice):
    """
    Clean and read data
    """
    # clean data
    if clean_data_choice == 'y': # when we have new raw data
        print("Beginning data processing...")
        # read and clean data
        new_data1 = clean_data("../../data/rowData/Row_data.xlsx")
        # save the cleaned dataset
        new_data1.to_csv('../../data/cleanedData/cleaned_data.csv', 
                             index=False, encoding='utf-8')   
        print("Data processing finished.")   
    data_list = read_data()
    return data_list
    

def read_data():
    """
    Return a data_list containing a list of dataframe
    """  
    data1 = pd.read_csv("../../data/cleanedData/cleaned_data.csv")
    # currently, we only has one dataset
    data_list = [data1]
    return data_list    
    
    
if __name__ == "__main__":
    main()


