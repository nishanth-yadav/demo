import pandas as pd
import sqlite3

#order
orders=pd.read_csv("orders-1.csv")
print(orders.head())

#user
user=pd.read_json("users.json")
print(user.head())

#sql
conn = sqlite3.connect("restaurants.db")
restaurants = pd.read_sql("SELECT * FROM restaurants", conn)
print(restaurants.head())    

merged_data=pd.merge(
  orders,
  user,
  how="left",
  left_on="user_id",
  right_on="user_id"
  
)
final_data=pd.merge(
  merged_data,
  restaurants,
  how="left",
  left_on="restaurant_id",
  right_on="restaurant_id"  
)

#final_data
final_data.to_csv("final_food_delivery_dataset.csv",index=False)
#order
final_data['order_date'] = pd.to_datetime(final_data['order_date'])
final_data.groupby(final_data['order_date'].dt.month).size()

#user
final_data.groupby('user_id')['order_id'].count()

#member
final_data.groupby('membership')['total_amount'].sum()
#revenue
final_data['total_amount'].describe()
#1
gold = final_data[final_data['membership'] == 'Gold']
result=gold.groupby('city')['total_amount'].sum().sort_values(ascending=False)
#2
result=final_data.groupby('cuisine')['total_amount'].mean().sort_values(ascending=False)
#3
(final_data['total_amount']>1000).sum()
#4
result=final_data.groupby('rating')['total_amount'].sum().sort_values(ascending=False)
#5
x=gold.groupby('city')['total_amount'].mean().sort_values(ascending=False)
#6
y=gold.groupby('cuisine').agg({'restaurant_id':'nunique',
                                    'total_amount':'sum'}).sort_values('restaurant_id')
#7
per=(len(gold)/len(final_data))*100

#8
rest_orders = final_data.groupby('restaurant_name_x').agg({
    'order_id':'count',
    'total_amount':'mean'
})

v=rest_orders[rest_orders['order_id'] < 20].sort_values('total_amount', ascending=False)
#9
result=final_data.groupby(['membership','cuisine'])['total_amount'].sum().sort_values(ascending=False)
#10
final_data['order_date'] = pd.to_datetime(final_data['order_date'])
final_data['quarter'] = final_data['order_date'].dt.to_period('Q')
final_data.groupby('quarter')['total_amount'].sum().sort_values(ascending=False)

#2.1
gold=final_data[final_data['membership'] == 'Gold']
gold['order_id'].count() 
#2     
hyd=final_data[final_data['city'] == 'hyderabad']['total_amount'].sum()
round(hyd)
#3
final_data['user_id'].nunique()
#4
gold=final_data[final_data['membership'] == 'Gold']['total_amount'].mean()
round(gold,2)
#5
gold=final_data[final_data['rating'] >= 4.5]
['order_id'].count()
 
#6
top=gold.index[0]
final_data[(final_data['membership'] == 'Gold')&(final_data['city']==top)]['order_id'].count()

gold = final_data[final_data['membership'] == 'Gold']['total_amount'].mean()
print(round(gold,2))







 