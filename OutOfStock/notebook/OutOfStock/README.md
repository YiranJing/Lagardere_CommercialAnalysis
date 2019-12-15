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
Location: `data/output/figure` folder <br />
You might need to install `plotly` model in computer: 
```
$ pip install plotly==4.4.1
$ conda install -c plotly plotly-orca psutil requests
```

### How to update and run new dataset
1. Uodate raw data to `data/rowData/...` (for example, `data/rowData/BNC/Beverage`)
2. Add or modify relative data path in `OutOfStock/clean_data_script` (for example, `OutOfStock/clean_data_script/clean_data_beverage.py`)
3. Add or modify relative data path in function `read_data` in `main_OOS.py`
4. Add or modify `data_list` in function `read_data` in `main_OOS.py`

### Speed Up recommendation:
Run 19 categories in parallel:  <br/>
The running time for one categroy is arund 6 hours (3 hrs for clean data + 3 hrs for OOS check), thus, we need a way to run all categories in parallel to get the overall output quickly. To do it, we need to use `distributed computing`:

Choice 1: Ray (Free but maxmum parallel processes are 4)
- [Why Ray](https://towardsdatascience.com/10x-faster-parallel-python-without-python-multiprocessing-e5017c93cce1)
- [Modern Parallel and Distributed Python: A Quick Tutorial on Ray](https://towardsdatascience.com/modern-parallel-and-distributed-python-a-quick-tutorial-on-ray-99f8d70369b8)

Choice 2: AWS plantfrom (Max CPU can use at the same time is 128) (Not Free)
[Amazon EC2](https://aws.amazon.com/ec2/faqs/#EC2_On-Demand_Instance_limits)


