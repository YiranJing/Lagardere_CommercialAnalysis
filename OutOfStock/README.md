## Out Of Stock (OOS) Model

[Data folder structure and output files format](https://github.com/YiranJing/Lagardere_CommercialAnalysis/tree/master/OutOfStock/notebook/OutOfStock/support)
### Business Obejctive
- Automatically check and report 'out of stock' items in the last 7 days, running on last 30 days data
- Estimating the possible `margin loss` and `net sale loss`due to OOS
- Further analysis on the items which are out of stock in the past 7 days
- Identify `removed product`, `new product`
- Identify the `store`, which has the significant OOS issue
- Identify the `product`, which commonly OOS in multiple stores.
- Visualization on Power BI

### How to run the code
Open terminal (Mac)/command (Window)
```
$ python3 main_OOS.py
```
Then, input `category` (for example, `beverage`) and clean data option (if have saved the cleand data, enter `n`), for example:
```
$ python3 main_OOS.py
Enter the category: beverage
Have you stored cleaned data yet? (y/n): n
```
### Output
#### 1. Total OOS items
Location: `data/output` folder

#### 2. Analysis of OOS 7 days items
Location: `data/output/OOS_7_days_analysis`

#### 3. Figures
PowerBI
