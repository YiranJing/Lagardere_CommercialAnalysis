## Test functions for rank analysis


from dataclasses import dataclass, field
from typing import Dict, List
from pyspark.sql.functions import lit
from Rank_analysis_helperfunction import *
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
import pytest


"""
Build the SparkSession
"""
spark = SparkSession.builder \
   .master("local") \
   .appName("Rank Model") \
   .config("spark.executor.cores",1) \
   .getOrCreate()    
sc = spark.sparkContext


def test_range_expansion():
    """
    Should not have blank for all items sold in W2,
    
    test case:
    1.
    AUDF.CNS113
    AUDF100430711
    should be false 
    
    2.
    AULS.AVV102 
    AULS126353
    should be true
    
    3. 
    AULS.AVV102
    AULS488089
    should be false
    """
    ## Create test dataset
    test_rows = [('3', '2019W01', 'AUDF', 'BNC', 'Duty Free', '', 'AUDF.CNS113', 
                 'AUDF100430711', 241.8, 1),
                 ('4', '2019W02', 'AUDF', 'BNC', 'Duty Free', '', 'AUDF.CNS113', 
                 'AUDF100430711', 322.8, 1),
                ('1', '2019W02', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV102', 
                 'AULS126353', 76.89, 17),
                ('3', '2019W01', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV102', 
                 'AULS488089', 76.89, 17)]
    dataset = create_test_dataset(test_rows)
    ## calculation 
    test_result = calculate_range_expansion(dataset)
    ## test function result
    assert test_result.filter((col('Store') == 'AULS.AVV102') \
                             & (col('SKU') == 'AULS126353')).collect()[0]['range_expansion']=='True'
    assert test_result.filter((col('Store') == 'AULS.AVV102') \
                             & (col('SKU') == 'AULS488089')).collect()[0]['range_expansion']!='True'
    for row in range(2):
        assert test_result.filter((col('Store') == 'AUDF.CNS113') \
                             & (col('SKU') == 'AUDF100430711')).collect()[row]['range_expansion']!='True'


def test_in_W1_not_W2():
    ## Create test dataset
    test_rows = [('1', '2019W01', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV102', 
                 'AULS488089', 76.89, 17),
                ('2', '2019W01', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV104', 
                 'AULS488082', 76.89, 17),
                ('3', '2019W02', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV104', 
                 'AULS488082', 76.89, 17)]
    dataset = create_test_dataset(test_rows)
    ## calculation 
    test_result = calculate_in_W1_not_W2(dataset)
    ## test function result
    assert test_result.filter((col('SKU') == 'AULS488089') \
                             & (col('Store') == 'AULS.AVV102')).collect()[0]['in_W1_not_W2']=='True'
    for row in range(2):
        assert test_result.filter((col('SKU') == 'AULS488082') \
                             & (col('Store') == 'AULS.AVV104')).collect()[row]['in_W1_not_W2']!='True'
        

column_names= ['Index', 'Date', 'Company', 'BusinessUnit', 'Concept_NEW', 'Category', 
                   'Store', 'SKU', 'NetSales', 'rank']

def test_material_change():
    """
    test case:
    1.
    AULS488089
    AULS.AVV102
    should be true
    
    2.
    AULS488089
    AULS.AVV104
    should be false
    """
    ## Create test dataset
    
    test_rows = [('1', '2019W01', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV102', 
                 'AULS488089', 76.89, 17),
                ('2', '2019W02', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV104', 
                 'AULS488089', 30.1, 1),
                ('3', '2019W01', 'AUDF', 'BNC', 'Air N&R', '', 'AULS.AVV104', 
                 'AULS488089', 76.89, 7)]
    
    dataset = create_test_dataset(test_rows)
    
    ## calculation 
    dataset.df = calculate_range_expansion(dataset)
    dataset.df = calculate_in_W1_not_W2(dataset)
    dataset.df = calculate_material_change(dataset)
    
    ## test function result
    assert dataset.df.filter((col('SKU') == 'AULS488089') \
                             & (col('Store') == 'AULS.AVV102')).collect()[0]['material_change']=='True'
    for row in range(2):
        assert dataset.df.filter((col('SKU') == 'AULS488089') \
                             & (col('Store') == 'AULS.AVV104')).collect()[row]['material_change']!='True'


def create_test_dataset(test_rows: list) -> Dataset:
    """
    Create temperate dataframe for test cases
    """
    test_df = spark.createDataFrame(test_rows, column_names)
    dataset = Dataset(df = test_df, store_item_concept = get_store_item_concept_list(test_df, spark),
                  week = get_week_list(test_df), concept = get_concept_list(test_df))
    return dataset
    
    
def test_all():
    
    global column_names
    column_names= ['Index', 'Date', 'Company', 'BusinessUnit', 'Concept_NEW', 'Category', 
                   'Store', 'SKU', 'NetSales', 'rank']
    
    print("Running tests")
    for func in [test_in_W1_not_W2, test_range_expansion, test_material_change]:
        try:
            func()
        except AssertionError:
            status = "😫"
        else:
            status = "🎉"
        print(f"{status}\t{func.__name__}")    
    
    