## Functions for out of stock check for Lagardere Travel Retail
### Author: Yiran Jing
### Date: Dec 2019

import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import pyplot
import plotly.express as px
import plotly.graph_objects as go
import warnings
warnings.filterwarnings('ignore')


########################
### Clean data
#######################

def clean_data(path):
    """
    Clean row data before OOS checking.
    
    Input:
       the relative path of raw data
       
    Output:
        dataframe with columns:
        'Date', 'item_name', 'store_name', 'POS Margin on Net Sales', 'POS Net Sales', 
        'POS Qty Sold', 'Stock Balance Qty'
    """
    ## read data
    data = pd.read_excel(path)
    ## add header to columns
    data.rename(columns={'Unnamed: 0':'Date', 
                    'Unnamed: 1':'State',
                    'Unnamed: 2':'City',
                    'Unnamed: 3':'BU',
                    'Unnamed: 4':'Concept',
                    'Unnamed: 5':'Store',
                    'Unnamed: 6':'Category',
                    'Unnamed: 7':'Brand',
                    'Unnamed: 8':'SKU',
                    'POS Margin on Net Sales': 'Margin',
                    'POS Net Sales':"Net Sale",
                    'POS Qty Sold': 'Qty Sold'}, inplace=True)
    
    ## Remove the closed store
    data = remove_closed_store(data)
    ## after create new data structure, more steps for data cleaning
    data = clean_and_add_date(data)
    ## replace none to 0
    data = data.fillna(0)
    ## convert float number between -1 and 1 to 0
    for index, row in data.iterrows():
        if row['Stock Balance Qty'] <1 and row['Stock Balance Qty'] >-1:
            data.at[index, 'Stock Balance Qty'] = 0  
    print("finish clean one dataset.")
    return data


def clean_and_add_date(df):
    """
    1. Convert column 'Date' to datatime object
    2. Add more rows to ensure each item in each store has the full-month records 
       (since if both stock and sales are 0, the raw date can miss the relevant column)
    """
    # Convert to datetime object
    df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
    # Ensure each item in each store has the full-month records
    end = df['Date'].max()
    start = df['Date'].min()
    item_list = df['SKU'].unique()
    store_list = df['Store'].unique()
    # Create a list of dates, start from the first day of dataset, end with the last day of dataset
    date_generated = create_list_dates(df)
    
    for item in item_list:
        for store in store_list:
            sub_data = create_sub_time_series_one_item(df, item, store)
            if len(sub_data) > 0 and len(sub_data) < len(date_generated): # there are missed columns we need to add in 
                # the list of missed date
                missed_date = list(set(date_generated) - set(sub_data['Date']))
                df = generate_new_row(item, store, missed_date, sub_data, df)                      
            else: # no missing date for the given item and store
                continue
    return df
  

    
def generate_new_row(item, store, missed_date, sub_data, df):
    """
    Generate a new dataset containing row with 0 entries    
    """
    ## test correct entry match
    test_correct_entry(sub_data, item)
  
    new_row = {}
    state = sub_data["State"].unique()[0]
    city = sub_data["City"].unique()[0]
    bu = sub_data["BU"].unique()[0]
    concept = sub_data["Concept"].unique()[0]
    category = sub_data["Category"].unique()[0]
    brand = sub_data["Brand"].unique()[0]
    
    for date in missed_date:
        new_row['Store'] = store
        new_row['Date'] = date
        new_row['SKU'] = item
        new_row['Margin'] = 0
        new_row['Net Sale'] = 0
        new_row['Qty Sold'] = 0
        new_row['Stock Balance Qty'] = 0
        new_row["State"] = state
        new_row["City"] = city
        new_row["BU"] = bu
        new_row["Concept"] = concept
        new_row["Category"] = category
        new_row["Brand"] = brand
        
        ## insert the last row            
        df = df.append(new_row, ignore_index=True) 
        #df.drop(df.index[2])
    return df

def test_correct_entry(sub_data, item):
    """
    One specific SKU should have only one state, city, etc.
    """
    assert len(sub_data["State"].unique()) == 1, 'test fail, the State are ' \
                                                + str(sub_data["State"].unique())+ "should be only one. The item is " + item
    assert len(sub_data["City"].unique()) == 1
    assert len(sub_data["BU"].unique()) == 1
    assert len(sub_data["Concept"].unique()) == 1
    assert len(sub_data["Category"].unique()) == 1
    assert len(sub_data["Brand"].unique()) == 1
    
    

            
def create_list_dates(df):
    """
    Create a list of dates, 
        start from the first day of dataset
        end with the last day of dataset
    
    :param df: dataframe
    :return: a list of dates
    """
    end = df['Date'].max() + timedelta(days=1)
    start = df['Date'].min()
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]
    
    # Test the output 
    test_list_dates(date_generated,end, start)
    return date_generated

def test_list_dates(date_generated,end, start):
    """
    Test the accuracy of the generated date list
    """
    assert min(date_generated) == start, 'test fail, the first day should be ' \
    + str(start) +'but now is '+ str(min(date_generated))
    assert max(date_generated) == end - timedelta(days=1), 'test fail, the last day should be ' \
    + str(end) +'but now is '+ str(max(date_generated)) 

def remove_closed_store(data):
    """
    Removed colosed store if the store in the Close stores list.xlsx
    """
    closed_store = pd.read_excel('../../data/closedStore/Closed stores list.xlsx')
    closed_store_list = closed_store['Store '].unique()
    store_list = data['Store'].unique()
    
    for store in store_list:
        if store in closed_store_list:
            data = data[data.Store !=store] # remove the colsed store
    return data



########################
### OOS check
#######################

def merge_data(data_list):
    """
    Merge multiple dataframe (same structure) together
    
    :param data_list: a list contain at least one dataset 
    :return data: the merged dataset
    """
    # case 1: if contain only one dataset
    if len(data_list) == 1:
        return data_list[0]
    # case 2: combine muptiple dataset
    else:
        df = data_list[0]
        for data in data_list[1:]:
            df = pd.concat([df, data], ignore_index=True)
    return df
    

def check_OOS_by_rules(df):
    """
    Set rules for OOS items:
        1. if 0 sales all the time, no OOS. (this is the case product removed from store)
        2. if 0 stock all the time, no OOS. (this is the case product removed from store)
        3. OOS occurs when both stock and QTY sold are 0, and have sales before and after
        4. OOS happens when we have sales before. i.e. not new product case
        5. OOS days consider only the last 7 days
    
    param df: 
        dataframe with columns:
            'date', 'item_name', 'store_name', 'POS Margin on Net Sales (INV)', 'POS Net Sales', 
            'POS Qty Sold', 'Stock Balance Qty'
    return output_data:
        dataframe with header: 
             'item_name', 'store_name', 'category', 'OOS_days', 'date_list', 'OOS_lastDay','avg_loss_sale_quantity',
             'avg_loss_net_sale','avg_loss_INV', 'total_loss_sale_quantity','total_loss_net_sale','total_loss_INV'
    
    """ 
    # Convert to datetime object
    try:
        df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(str(x)))
    except:
        print(df['Date'])
    #subtract 1 week from current date
    one_weeks_ago = df['Date'].max()+ timedelta(days=1) - timedelta(weeks = 1) # check only the last 7 days
    OOS_result = {}
    item_list = df['SKU'].unique()
    store_list = df['Store'].unique()
        
    for store in store_list:
        for item in item_list:
            sub_data = create_sub_time_series_one_item(df, item, store)
            if len(sub_data) == 0: # means this product is not sold in this store
                continue
            category = sub_data['Category'].unique()[0]
            ## Rule 1: not OOS product if no sales in the whole dataset
            sub_data = create_sub_time_series_one_item(df, item, store)
            if sub_data['Qty Sold'].sum()==0:
                continue
            # Rule 2: not OOS product if 0 stock all the time, no OOS
            if sub_data['Stock Balance Qty'].sum()==0:
                continue
            
            # Establish new status to check given a item in a given store.
            check = 0 # how many days OOS 
            last_day_OOS = 0 # will be 1 if OOS in the last day of given dataset
            check_not_new = False # check if it is the new product in this month
            check_not_removed = False # check if the producted has been removed
            OOS_date_list = [] # the list to store the date if OOS 
            
            
            for index, row in sub_data.iterrows():
                # Rule 4: OS happens when we have sales before
                ## i.e. remove new product case
                ## or the item has only stock, but not sold
                if row['Stock Balance Qty'] != 0:
                    check_not_new = True 
                
                # Rule 3: OOS occurs when both stock and QTY sold are 0 and check_not_new is True
                # OOS occurs when both stock and QTY sold are 0
                if row['Stock Balance Qty'] == 0 and row['Qty Sold'] == 0 \
                    and check_not_new == True and row['Date'] >= one_weeks_ago:
                    check +=1
                    OOS_date_list.append(row['Date'])
                    print('{} has 0 stock at {} , Date: {}'.format(item, row['Store'], row['Date']))
                    ## check OOS in last day
                    last_day_OOS = check_OOS_last_day(df, row['Date']) # return 1 if OOS in the last days
                        
                # as long as we have stock in this month, we believe this item is not removed from store
                if row['Stock Balance Qty'] > 0:
                    check_not_removed = True
            
            # When this (item, store) contains the OOS days, 
            # and confirmed that this product is not been removed  
            if check >0 and check_not_removed == True:
                
                state = sub_data["State"].unique()[0]
                city = sub_data["City"].unique()[0]
                bu = sub_data["BU"].unique()[0]
                concept = sub_data["Concept"].unique()[0]
                category = sub_data["Category"].unique()[0]
                brand = sub_data["Brand"].unique()[0]
                
                key = (item , store, state, city, bu, concept, brand)
                loss_INV, loss_NS, loss_QTY = calculate_possible_loss(sub_data, check)
                OOS_result[key] = (check, loss_INV, loss_NS, loss_QTY, last_day_OOS, OOS_date_list, category) # returned value 
                
    ## Output to dataframe    
    output_data = out_put_data(OOS_result)
    return output_data


def check_OOS_last_day(df, date):
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

def create_sub_time_series_one_item(sub_data, item, store):
    """
    Abstract dataset given specific one item in one store
    """
    ## create sub dataset
    sub_data = sub_data[sub_data['SKU'] == item]
    sub_data = sub_data[sub_data['Store'] == store]
    sub_data = sub_data.sort_values(by="Date")
    
    return sub_data

def calculate_possible_loss(sub_data, check):
    """
    Calculate possible loss due to OOS
    
    Return the average * number of days in which has 0 stock and 0 sales
    """
    sub_data1 = sub_data.copy()
    sub_data1 = sub_data1[sub_data1['Stock Balance Qty']!=0] # remove non-stock day
    
    loss_INV = sub_data1['Margin'].mean() 
    loss_NS = sub_data1['Net Sale'].mean() 
    loss_QTY = sub_data1['Qty Sold'].mean()
    
    return loss_INV, loss_NS, loss_QTY

    
####################################
#### Output data to csv
####################################
    
def convert_to_string_times(df):
    """
    Convert timestamp to string before store in csv file for readability
    
    param df: dataframe, contains one column caleed 'date_list', whose entry is a list of timestamp
    return df: dataframe, contains one column caleed 'date_list', whose entry is a list of time string
    """
    dateStr = lambda x: x.strftime("%Y-%m-%d")
    for i in range(len(df)):
        df['Date_list'][i] = list(map(dateStr, df['Date_list'][i]))
    return df
    
def out_put_data(OOS_result): 
    """
    Create csv file to save the output OOS result

    input:
        OOS_result: (directory)
            key: tuple (item , store) 
            value: tuple (OOS_days, loss_INV, loss_NS, loss_QTY, last_day_OOS, OOS_date_list)
    
    output:
        dataframe with header: 
             'item_name', 'store_name', 'category', 'OOS_days', 'date_list', 'OOS_lastDay','avg_loss_sale_quantity',
             'avg_loss_net_sale','avg_loss_INV', 'total_loss_sale_quantity','total_loss_net_sale','total_loss_INV'
    """
    header = ['SKU', 'Store', 'Category', "State", "City", "BU", "Concept", "Brand", 'OOS_days', 
              'Date_list', 'OOS_lastDay','Avg Loss Sale qty',
              'Avg Loss Net Sales','Avg Loss Margin', 'Total Loss Sale qty',
              'Total Loss Net Sales','Total Loss Margin']
    output_data = pd.DataFrame(columns = header)
    new_row = {}
    
    for key, value in OOS_result.items():
        new_row['Store'] = key[1]
        new_row['SKU'] = key[0]
        new_row['Category'] = value[6]
        new_row['OOS_days'] = value[0]
        new_row['Date_list'] = value[5]
        new_row['OOS_lastDay'] = value[4]
        new_row['Avg Loss Sale qty'] = value[3]
        new_row['Avg Loss Net Sales'] = value[2]
        new_row['Avg Loss Margin'] = value[1]
        new_row['Total Loss Sale qty'] = value[3] *value[0]
        new_row['Total Loss Net Sales'] = value[2] *value[0]
        new_row['Total Loss Margin'] = value[1] *value[0]
        new_row["State"] = key[2]
        new_row["City"] = key[3]
        new_row["BU"] = key[4]
        new_row["Concept"] = key[5]
        new_row["Brand"] = key[6]
    
        ## insert the new row            
        output_data = output_data.append(new_row, ignore_index=True) 
    return output_data





############################################
## OOS 7 days analysis
############################################

def output_OOS_7_days_analysis(df, output_data, category):
    """
    Analysis OOS 7 days product
    
    param df: dataframe, same data structre as the cleand dataset
    param output_data: dataframe with header: 
             'item_name', 'store_name', 'category', 'OOS_days', 'date_list', 'OOS_lastDay','avg_loss_sale_quantity',
             'avg_loss_net_sale','avg_loss_INV', 'total_loss_sale_quantity','total_loss_net_sale','total_loss_INV'
    param category: a string
             such as 'beverage', or 'confectionery'
    
    return result: dataframe with headers:
             'item_name', 'store_name', 'category', 'OOS_days', 'Avg_NS_Percentage', 'Avg_Margin_Percentage',
             'avg_loss_sale_quantity','avg_loss_net_sale','avg_loss_mergin', 'total_loss_sale_quantity',
              'total_loss_net_sale','total_loss_mergin'   
             
             Avg_Margin_Percentage: The average margin % of this item in this store (Remove the 7 OOS days) 
                                    devided by 
                                    Average margin of this store for the given category 
             Avg_NS_Percentage: The average net sale % of this item in this store (Remove the 7 OOS days) 
                                devided by 
                                Average net sale of this store for the given category 
    """
    
    
    OOS_collection = OOS_7_days_collection(output_data)
    OOS_7_days = output_data[output_data.OOS_days == 7]
    OOS_7_item = OOS_7_days['SKU'].unique()
    OOS_7_store = OOS_7_days['Store'].unique()
    
    ## create output dataset
    header = ['item_name', 'store_name', 'category', 'OOS_days', 'Avg_NS_Percentage', 'Avg_Margin_Percentage',
             'avg_loss_sale_quantity','avg_loss_net_sale','avg_loss_mergin', 'total_loss_sale_quantity',
              'total_loss_net_sale','total_loss_mergin']
    result = pd.DataFrame(columns = header)
    new_row = {}
    
    for store in OOS_7_store:
        store_data = df[df.store_name == store]
        
        # average sale one day for one store
        total_sale = store_data['POS Net Sales'].sum()/len(store_data['date'].unique())
        total_margin = store_data['POS Margin on Net Sales (INV)'].sum()/len(store_data['date'].unique())
        for item in OOS_7_item:
            if (item, store) in OOS_collection:
                new_row['store_name'] = store
                new_row['item_name'] = item
                new_row['category'] = category
                # average net sale one day of this item (remove the last 7 days)
                store_item_data = store_data[store_data.item_name == item]
                sub_sale = store_item_data['POS Net Sales'].sum()/(len(store_data['date'].unique() -7))
                new_row['Avg_NS_Percentage'] = sub_sale/total_sale * 100
                # average margin one day of this item (remove the last 7 days)
                sub_margin = store_item_data['POS Margin on Net Sales (INV)'].sum()/(len(store_data['date'].unique() -7))
                new_row['Avg_Margin_Percentage'] = sub_margin/total_margin * 100
                
                ## collect other information
                store_item_OOS = output_data[(output_data.item_name == item) & 
                                             (output_data.store_name == store)].reset_index(drop=True)
                assert len(store_item_OOS) == 1
                new_row['avg_loss_sale_quantity'] = store_item_OOS['avg_loss_sale_quantity'][0]
                new_row['avg_loss_net_sale'] = store_item_OOS['avg_loss_net_sale'][0]
                new_row['avg_loss_mergin'] = store_item_OOS['avg_loss_mergin'][0]
                new_row['total_loss_sale_quantity'] = store_item_OOS['total_loss_sale_quantity'][0]
                new_row['total_loss_net_sale'] = store_item_OOS['total_loss_net_sale'][0]
                new_row['total_loss_mergin'] = store_item_OOS['total_loss_mergin'][0]
                new_row['OOS_days'] = 7
                ## insert the new row            
                result = result.append(new_row, ignore_index=True) 
                
    return result
                
def OOS_7_days_collection(output_data):
    """
    colloect tuple for (item, store) , which means this item in this store is OOS in the last 7 days
    """
    OOS_7_days = output_data[output_data.OOS_days == 7]
    len(OOS_7_days)
    OOS_collection= []
    for index, row in OOS_7_days.iterrows():
        key = (row['SKU'], row['Store'])
        if key not in OOS_collection:
            OOS_collection.append(key)
        else:
            continue
    return OOS_collection

