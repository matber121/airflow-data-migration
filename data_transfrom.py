import pandas as pd


order = pd.read_csv('C:/Users/alima/Desktop/order.csv')
product = pd.read_csv('C:/Users/alima/Desktop/product.csv')
order['customer_id'] = order['customer_id'].astype(object)
order.drop_duplicates()
product.drop_duplicates()
order.fillna("unknown",inplace=True)
product.fillna("unknown",inplace=True)
print(order['customer_id'].dtype)

order = order[order['customer_id'].str.lower() != 'unknown']

order = order[order['customer_id'] != 'Unknown']

order['total_cost'] = order['quantity'] * order['unit_price']



# print(order)
# print(product)



 

#order1['total_cost'] = order1['quantity'].mul(order1['unit_price'])


# print(order1)
# print(product)