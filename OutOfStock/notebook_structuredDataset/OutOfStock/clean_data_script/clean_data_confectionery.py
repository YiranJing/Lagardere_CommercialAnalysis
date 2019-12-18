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


## read and clean the raw data 
new_data1 = clean_data("../../data/rowData/BNC/Confectionery/TSV BNC 11.19 Data daily Confec.xlsx")
new_data2 = clean_data("../../data/rowData/BNC/Confectionery/AKL BNC 11.19 Data daily Confec.xlsx")
new_data3 = clean_data("../../data/rowData/BNC/Confectionery/AVV BNC 11.19 Data daily Confec.xlsx")
new_data4 = clean_data("../../data/rowData/BNC/Confectionery/BNK BNC 11.19 Data daily Confec.xlsx")
new_data5 = clean_data("../../data/rowData/BNC/Confectionery/CNS BNC 11.19 Data daily Confec.xlsx")
new_data6 = clean_data("../../data/rowData/BNC/Confectionery/MEL BNC 11.19 Data daily Confec.xlsx")
new_data7 = clean_data("../../data/rowData/BNC/Confectionery/OOL BNC 11.19 Data daily Confec.xlsx")
new_data8 = clean_data("../../data/rowData/BNC/Confectionery/SYD BNC 11.19 Data daily Confec.xlsx")
new_data9 = clean_data("../../data/rowData/BNC/Confectionery/ADLBNC 11.19 Data daily Confec.xlsx")
new_data10 = clean_data("../../data/rowData/BNC/Confectionery/Perth BNC 11.19 data daily Confec")

## Save cleaned data out 
new_data1.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_TSV BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data2.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_AKL BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data3.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_AVV BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data4.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_BNK BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data5.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_CNS BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data6.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_MEL BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data7.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_OOL BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data8.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_SYD BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data9.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_ADL BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')
new_data10.to_csv('../../data/cleanedData/BNC/Confectionery/cleaned_PER BNC 11.19 Data daily Confec.csv', index=False, encoding='utf-8')



