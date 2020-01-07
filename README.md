# Lagardere_CommercialAnalysis

### Date: Dec 2019 - March 2020

Main tasks I did during this Data Scientist Internship in lagardere commercial analytics team during summer holiday! 

***
## Inventory Optimization
[See the process](https://github.com/YiranJing/Lagardere_CommercialAnalysis/projects/2)
#### Motivation:
From one side, we want to avoid costly shortages, on the other hand, Inventory consumes space, gets damaged, and sometimes becomes obsolete — and carrying surplus inventory costs the organization. Simply, optimizing inventory means finding the perfect balance between demand and supply including `facing`, and `capacity`.

### Key points for this project:
1. We only check three months after distribution report, and we mainly focus on the items have same issue in all 3 months. 
2. Only analysis for `non-promotaion` items.  
3. **We focus on `Capacity` first, since capacity is easier to be adjusted than facing**
4. Currently focus on  `WH to store`, will consider `Supply chain to WH` later

- Stage 1: [EDA](https://github.com/YiranJing/Lagardere_CommercialAnalysis/blob/master/InventoryOptimization/notebook/EDA_InventoryOptimization.ipynb)
  - Check new and removed items. 
  - Identify the relationship between classifiction of items with price, sold quantity, and sell margins
  - Check the influence of time effect
  - Find patterns and possible method to check issues with `facing` and `capacity` for each items, grouped by `Classification` and `subCategory`
- Satge 2: [Identify issued items by multiple rules](https://github.com/YiranJing/Lagardere_CommercialAnalysis/blob/master/InventoryOptimization/notebook/CheckItems_bySubCategory.ipynb)
- Stage 3: Communicate with store managers to confirm issues
- Stage 4: Design algothms to adjust issues.

***
## [Out of Stock model](https://github.com/YiranJing/Lagardere_CommercialAnalysis/tree/master/OutOfStock)
Will update new version using more efficienct algo with pyspark 
***
## [Rank Analysis]
- Stage 1: Query data from Datawarehouse
- Stage 2: Calculate `rank` and `new concept`
- Stage 3: [More analysis and test output](https://github.com/YiranJing/Lagardere_CommercialAnalysis/tree/master/RankAnalysis/notebook)
- Stage 4: DashBoard building
