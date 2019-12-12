import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import pyplot
import plotly.express as px
import plotly.graph_objects as go
from OOS_check import *
import sys


%matplotlib inline
sns.set_style("whitegrid")

import warnings
warnings.filterwarnings('ignore')

# read and clean data
new_data1 = clean_data("../../data/rowData/BNC/Beverage/SYD BNC 11.19 Data dail Beverages.xlsx")



# save the cleaned dataset
new_data1.to_csv('../../data/cleanedData/BNC/Beverage/cleaned_SYD BNC 11.19 Data dail Beverages.csv', 
                             index=False, encoding='utf-8')