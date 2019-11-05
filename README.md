
# Obtaining Your Data - Lab

## Introduction
In this lab you'll practice your munging and transforming skills in order to load in your data to solve a regression problem.

## Objectives
You will be able to:
* Perform an ETL process with multiple tables and create a single dataset

## Task Description

You just got hired by Lego! Your first project is going to be to develop a pricing algorithm to help set a target price for new lego sets that are released to market. To do this, you're first going to need to start mining the company database in order to collect the information you need to develop a model.

Start by investigating the database stored in lego.db and joining the tables into a unified dataset!

> **Hint:** use this SQL query to preview the tables in an unknown database:
```sql
SELECT name FROM sqlite_master
            WHERE type='table'
            ORDER BY name;
```


```python
import sqlite3
import pandas as pd
```


```python
conn = sqlite3.connect('lego.db')
c = conn.cursor()
response = c.execute("""SELECT name FROM sqlite_master
             WHERE type='table'
             ORDER BY name;
          """).fetchall()
response
```




    [('product_details',),
     ('product_info',),
     ('product_pricing',),
     ('product_reviews',)]




```python
#Preview the tables:
for item in response:
    table = item[0]
    length = c.execute("""SELECT count(*) from {};""".format(table)).fetchall()
    results = c.execute("""SELECT * from {} limit 5;""".format(table)).fetchall()
    df = pd.DataFrame(results)
    df.columns = [x[0] for x in c.description]
    print(table, length, '\n', df, '\n\n')
```

    product_details [(744,)] 
        prod_id                          prod_desc  \
    0      630                Everyone needs one!   
    1     2304              Start creations here!   
    2     7280      Add roads to your LEGO® town!   
    3     7281       Roads for your LEGO® layout!   
    4     7499  Get killer curves in your tracks!   
    
                                          prod_long_desc theme_name  
    0  This tool makes it a snap to pull those small ...    Classic  
    1  Even big imaginations need a place to start--a...     DUPLO®  
    2  Expand your LEGO® town with this set of straig...       City  
    3  Add a T-junction and curved roads to your LEGO...       City  
    4  Add flexible train tracks to your locomotive s...       City   
    
    
    product_info [(744,)] 
        prod_id  ages  piece_count                         set_name
    0      630    4+            1                  Brick Separator
    1     2304  1½-5            1     LEGO® DUPLO® Green Baseplate
    2     7280  5-12            2      Straight & Crossroad Plates
    3     7281  5-12            2  T-Junction & Curved Road Plates
    4     7499  5-12           24     Flexible and Straight Tracks 
    
    
    product_pricing [(10870,)] 
        prod_id country list_price
    0    75823      US     $29.99
    1    75822      US     $19.99
    2    75821      US     $12.99
    3    21030      US     $99.99
    4    21035      US     $79.99 
    
    
    product_reviews [(744,)] 
        prod_id  num_reviews  play_star_rating review_difficulty  star_rating  \
    0      630          180               4.0         Very Easy          4.8   
    1     2304           15               4.4              Easy          4.0   
    2     7280           85               4.1         Very Easy          3.5   
    3     7281           40               4.1         Very Easy          3.9   
    4     7499           89               2.9              Easy          2.5   
    
       val_star_rating  
    0              4.6  
    1              3.3  
    2              2.3  
    3              2.8  
    4              2.2   
    
    



```python
cmd = """SELECT * FROM product_info
                  JOIN product_details
                  USING(prod_id)
                  JOIN product_pricing
                  USING(prod_id)
                  JOIN product_reviews
                  USING(prod_id);"""
result = c.execute(cmd).fetchall()
df = pd.DataFrame(result)
df.columns = [x[0] for x in c.description]
print(len(df))
df.head()
```

    10870





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>prod_id</th>
      <th>ages</th>
      <th>piece_count</th>
      <th>set_name</th>
      <th>prod_desc</th>
      <th>prod_long_desc</th>
      <th>theme_name</th>
      <th>country</th>
      <th>list_price</th>
      <th>num_reviews</th>
      <th>play_star_rating</th>
      <th>review_difficulty</th>
      <th>star_rating</th>
      <th>val_star_rating</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>75823</td>
      <td>6-12</td>
      <td>277</td>
      <td>Bird Island Egg Heist</td>
      <td>Catapult into action and take back the eggs fr...</td>
      <td>Use the staircase catapult to launch Red into ...</td>
      <td>Angry Birds™</td>
      <td>US</td>
      <td>$29.99</td>
      <td>2.0</td>
      <td>4.0</td>
      <td>Average</td>
      <td>4.5</td>
      <td>4.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>75822</td>
      <td>6-12</td>
      <td>168</td>
      <td>Piggy Plane Attack</td>
      <td>Launch a flying attack and rescue the eggs fro...</td>
      <td>Pilot Pig has taken off from Bird Island with ...</td>
      <td>Angry Birds™</td>
      <td>US</td>
      <td>$19.99</td>
      <td>2.0</td>
      <td>4.0</td>
      <td>Easy</td>
      <td>5.0</td>
      <td>4.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>75821</td>
      <td>6-12</td>
      <td>74</td>
      <td>Piggy Car Escape</td>
      <td>Chase the piggy with lightning-fast Chuck and ...</td>
      <td>Pitch speedy bird Chuck against the Piggy Car....</td>
      <td>Angry Birds™</td>
      <td>US</td>
      <td>$12.99</td>
      <td>11.0</td>
      <td>4.3</td>
      <td>Easy</td>
      <td>4.3</td>
      <td>4.1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>21030</td>
      <td>12+</td>
      <td>1032</td>
      <td>United States Capitol Building</td>
      <td>Explore the architecture of the United States ...</td>
      <td>Discover the architectural secrets of the icon...</td>
      <td>Architecture</td>
      <td>US</td>
      <td>$99.99</td>
      <td>23.0</td>
      <td>3.6</td>
      <td>Average</td>
      <td>4.6</td>
      <td>4.3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>21035</td>
      <td>12+</td>
      <td>744</td>
      <td>Solomon R. Guggenheim Museum®</td>
      <td>Recreate the Solomon R. Guggenheim Museum® wit...</td>
      <td>Discover the architectural secrets of Frank Ll...</td>
      <td>Architecture</td>
      <td>US</td>
      <td>$79.99</td>
      <td>14.0</td>
      <td>3.2</td>
      <td>Challenging</td>
      <td>4.6</td>
      <td>4.1</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 10870 entries, 0 to 10869
    Data columns (total 14 columns):
    prod_id              10870 non-null int64
    ages                 10870 non-null object
    piece_count          10870 non-null int64
    set_name             10870 non-null object
    prod_desc            10870 non-null object
    prod_long_desc       10870 non-null object
    theme_name           10870 non-null object
    country              10870 non-null object
    list_price           10870 non-null object
    num_reviews          9449 non-null float64
    play_star_rating     9321 non-null float64
    review_difficulty    10870 non-null object
    star_rating          9449 non-null float64
    val_star_rating      9301 non-null float64
    dtypes: float64(4), int64(2), object(8)
    memory usage: 1.2+ MB



```python
df.to_csv('Lego_data_merged.csv', index=False)
```

## Summary
Nice work! You're working more and more independently through the workflow and ensuring data integrity! In this lab, you successfully executed an ETL process to merge different tables!
