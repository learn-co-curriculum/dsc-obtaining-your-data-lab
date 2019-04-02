
# Obtaining Your Data - Lab

## Introduction
In this lab you'll practice your munging and transforming skills in order to load in your data to solve a regression problem.

## Objectives
You will be able to:
* Understand the ETL process and the steps it consists of
* Understand the challenges of working with data from multiple sources 

## Task Description

Your boss gives you a general description of some of the datasets at your disposal for analyzing weekly store sales. They're eventually looking for you to build a model to help determine what factors impact sales, and model future sales forecasting for business planning.  
  
Most of the properietary store data sits in the company sql database, accessible by all managers and above. The database is called **Walmart.db** Your boss provides you with the following basic schema:  

<img src='images/db_schema.jpg' width=500>  

She then tells you that she's put together a second dataset on general economy statistics for the various dates that she would also like you to incorporate in your analysis. That data, she says, is stored in a file **economy_data.csv**.

As a first step in creating your model for providing recommendations and projections, load and synthesize these disperate datasets into a singular unified DataFrame. Then save your results to a file **Merged_Store_Data.csv**.

Make sure you check the various data types and merge appropriately.


```python
import sqlite3
import pandas as pd
```


```python
con = sqlite3.connect('Walmart.db')
cur = con.cursor()
cur.execute("""select * from sales join store_details using(store);""")
df1 = pd.DataFrame(cur.fetchall())
df1.columns = [i[0] for i in cur.description]
print(df1.shape)
df1.head()
```

    (452192, 7)





<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Store</th>
      <th>Dept</th>
      <th>Date</th>
      <th>Weekly_Sales</th>
      <th>IsHoliday</th>
      <th>Type</th>
      <th>Size</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2010-02-05</td>
      <td>24924.50</td>
      <td>False</td>
      <td>A</td>
      <td>151315</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>2010-02-12</td>
      <td>46039.49</td>
      <td>True</td>
      <td>A</td>
      <td>151315</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>1</td>
      <td>2010-02-19</td>
      <td>41595.55</td>
      <td>False</td>
      <td>A</td>
      <td>151315</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>1</td>
      <td>2010-02-26</td>
      <td>19403.54</td>
      <td>False</td>
      <td>A</td>
      <td>151315</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>1</td>
      <td>2010-03-05</td>
      <td>21827.90</td>
      <td>False</td>
      <td>A</td>
      <td>151315</td>
    </tr>
  </tbody>
</table>
</div>




```python
df2 = pd.read_csv('economy_data.csv')
print(df2.shape)
df2.head()
```

    (8190, 12)





<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Store</th>
      <th>Date</th>
      <th>Temperature</th>
      <th>Fuel_Price</th>
      <th>MarkDown1</th>
      <th>MarkDown2</th>
      <th>MarkDown3</th>
      <th>MarkDown4</th>
      <th>MarkDown5</th>
      <th>CPI</th>
      <th>Unemployment</th>
      <th>IsHoliday</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2010-02-05</td>
      <td>42.31</td>
      <td>2.572</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.096358</td>
      <td>8.106</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2010-02-12</td>
      <td>38.51</td>
      <td>2.548</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.242170</td>
      <td>8.106</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>2010-02-19</td>
      <td>39.93</td>
      <td>2.514</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.289143</td>
      <td>8.106</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>2010-02-26</td>
      <td>46.63</td>
      <td>2.561</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.319643</td>
      <td>8.106</td>
      <td>False</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>2010-03-05</td>
      <td>46.50</td>
      <td>2.625</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.350143</td>
      <td>8.106</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Naive Merge is Faulty
merged = pd.merge(df1, df2)
print(merged.shape)
merged.head()
```

    (0, 16)





<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Store</th>
      <th>Dept</th>
      <th>Date</th>
      <th>Weekly_Sales</th>
      <th>IsHoliday</th>
      <th>Type</th>
      <th>Size</th>
      <th>Temperature</th>
      <th>Fuel_Price</th>
      <th>MarkDown1</th>
      <th>MarkDown2</th>
      <th>MarkDown3</th>
      <th>MarkDown4</th>
      <th>MarkDown5</th>
      <th>CPI</th>
      <th>Unemployment</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>




```python
#Investigating
df1.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 452192 entries, 0 to 452191
    Data columns (total 7 columns):
    Store           452192 non-null int64
    Dept            452192 non-null int64
    Date            452192 non-null object
    Weekly_Sales    452192 non-null float64
    IsHoliday       452192 non-null object
    Type            452192 non-null object
    Size            452192 non-null int64
    dtypes: float64(1), int64(3), object(3)
    memory usage: 24.1+ MB



```python
df2.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 8190 entries, 0 to 8189
    Data columns (total 12 columns):
    Store           8190 non-null int64
    Date            8190 non-null object
    Temperature     8190 non-null float64
    Fuel_Price      8190 non-null float64
    MarkDown1       4032 non-null float64
    MarkDown2       2921 non-null float64
    MarkDown3       3613 non-null float64
    MarkDown4       3464 non-null float64
    MarkDown5       4050 non-null float64
    CPI             7605 non-null float64
    Unemployment    7605 non-null float64
    IsHoliday       8190 non-null bool
    dtypes: bool(1), float64(9), int64(1), object(1)
    memory usage: 711.9+ KB



```python
common = [col for col in df1.columns if col in df2.columns]
common
```




    ['Store', 'Date', 'IsHoliday']




```python
for col in common:
    ex1 = df1[col].iloc[0]
    ex2 = df2[col].iloc[0]
    print(col)
    print('Types:')
    print('df1: {}, df2: {}'.format(type(ex1), type(ex2)))
    print('\n')
```

    Store
    Types:
    df1: <class 'numpy.int64'>, df2: <class 'numpy.int64'>
    
    
    Date
    Types:
    df1: <class 'str'>, df2: <class 'str'>
    
    
    IsHoliday
    Types:
    df1: <class 'str'>, df2: <class 'numpy.bool_'>
    
    


IsHoliday seems to be the culprit here; one is a string, the other a boolean.


```python
#Converting the datatype
df1.IsHoliday = df1.IsHoliday.astype(bool)
```


```python
#Remerging
merged = pd.merge(df1, df2)
print(merged.shape)
merged.head()
```

    (31817, 16)





<div>
<style>
    .dataframe thead tr:only-child th {
        text-align: right;
    }

    .dataframe thead th {
        text-align: left;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Store</th>
      <th>Dept</th>
      <th>Date</th>
      <th>Weekly_Sales</th>
      <th>IsHoliday</th>
      <th>Type</th>
      <th>Size</th>
      <th>Temperature</th>
      <th>Fuel_Price</th>
      <th>MarkDown1</th>
      <th>MarkDown2</th>
      <th>MarkDown3</th>
      <th>MarkDown4</th>
      <th>MarkDown5</th>
      <th>CPI</th>
      <th>Unemployment</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2010-02-12</td>
      <td>46039.49</td>
      <td>True</td>
      <td>A</td>
      <td>151315</td>
      <td>38.51</td>
      <td>2.548</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.24217</td>
      <td>8.106</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>2010-02-12</td>
      <td>44682.74</td>
      <td>True</td>
      <td>A</td>
      <td>151315</td>
      <td>38.51</td>
      <td>2.548</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.24217</td>
      <td>8.106</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>3</td>
      <td>2010-02-12</td>
      <td>10887.84</td>
      <td>True</td>
      <td>A</td>
      <td>151315</td>
      <td>38.51</td>
      <td>2.548</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.24217</td>
      <td>8.106</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>4</td>
      <td>2010-02-12</td>
      <td>35351.21</td>
      <td>True</td>
      <td>A</td>
      <td>151315</td>
      <td>38.51</td>
      <td>2.548</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.24217</td>
      <td>8.106</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>5</td>
      <td>2010-02-12</td>
      <td>29620.81</td>
      <td>True</td>
      <td>A</td>
      <td>151315</td>
      <td>38.51</td>
      <td>2.548</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>211.24217</td>
      <td>8.106</td>
    </tr>
  </tbody>
</table>
</div>




```python
merged.to_csv('Merged_Store_Data.csv', index=False)
```

## Summary
Nice work! You're working more and more independently through the workflow and ensuring data integrity!
