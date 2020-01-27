## Data and Notebook information

#### Store: 
   - Departures: NZDF.AKL108
   - Arrivals: NZDF.AKL109

#### Sub-category: 15_ Liquor


### Input Dataset Description
1. `Distribution Report - Auckland Departures - July19.csv` <br/>or `../data/RawData/Distribution Report- Auckland Departures - Jan2020.csv` <br/>or `../data/RawData/Distribution Report-AKL Arrivals-Jan 2020.csv`
- `# POGs`: number of position within store 
- `Facings`: totoal number among all POGS
- `Capacity`: totoal number among all POGS
- `Linear (cm)`: the width of each Item

2. `sales_newzealand.csv`: monthly sale data
- Dataset download from TM1
3. `SubCateogoryInfo.csv` 
- from TM1
4. `Vendor_info.csv`
- from TM1


### Notebook description

1. EDA_InventoryOptimization.ipynb
   - EDA, plots overall
   
2. CheckItems_bySubCategory.ipynb
   - EDA, plots, zoom in each SKU
   
3. Capacity_Depth_Adjustment.ipynb ⭐️
   - This notebook is based on EDA_InventoryOptimization.ipynb and CheckItems_bySubCategory.ipynb, included all calculations within these two notebook, but NO PLOT. Just generate new CSV files.
   - Remeber **rename output folder** based on different input data: e.g Output_Sep_108, Output_July_108, Output_Sep_109. See details within this notebook.
   
4. Compare_July-Sep_and_Sep-Nov.ipynb
   - Find diffence between two distribution reports of store 108.
  
5. Inventory_opti_helperFunction.py
   - helper functions
