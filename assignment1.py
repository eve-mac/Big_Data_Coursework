#Import necessary packages
import pymongo
import os
import datetime

print('Connecting to B30DM...')

#Connection details:
uri = "mongodb+srv://DETAILS.mongodb.net/?" +\
"authSource=%24external&" +\
"authMechanism=MONGODB-X509&" +\
"retryWrites=true&" +\
"w=majority"

#Connect to MongoDB
client = pymongo.MongoClient(uri,
tls=True,
tlsCertificateKeyFile="PATH_TO_KEY.pem")

#Specify database
db = client["B30DM"]

print('connection successful')

#Specify collections
sales_collection = db["sales"]
products_collection = db["products"]

#Creating a specific collection so I can effectively test my own work
replacement_orders_collection = db["replacement_orders_emm"]

# Define function to aggregate sales data
def query_sales_data():
    pipeline = [
        {"$group": {"_id": {"provider": "$product.provider", "code": "$product.code"}, "total_sold": {"$sum": 1}}}
    ]
    return sales_collection.aggregate(pipeline)


# Define function to retrieve product information
def query_products_data():
    pipeline = [
        {"$group": {"_id": "$provider", "products": {"$push": {"code": "$code", "name": "$name", "wholesale_price": "$wholesale_price"}}}}
    ]
    return products_collection.aggregate(pipeline)


# Define the function to generate replacement orders
def generate_replacement_orders():
    #Call query functions to retrieve info from sales/product collection
    sales_data = query_sales_data()
    products_data = query_products_data()
    #Define current month to add detail to each replacement order
    current_month = datetime.datetime.now().strftime("%Y-%m")  # Get current month in YYYY-MM format

    # Create a dictionary to store product details for each provider
    provider_products = {doc['_id']: doc['products'] for doc in products_data}

    #Loop to geneneerate replacement orders grouped by providers, based on sales
    replacement_orders = []
    for sale in sales_data:
        provider = sale['_id']['provider']
        code = sale['_id']['code']
        total_sold = sale['total_sold']
        if provider in provider_products:
            order = next((order for order in replacement_orders if order['provider'] == provider), None)
            if order is None:
                order = {"provider": provider, "order_period": current_month, "products": []}
                replacement_orders.append(order)
            product_info = next((product for product in provider_products[provider] if product['code'] == code), None)
            if product_info:
                product_order = {"code": code, "name": product_info['name'], "amount": total_sold}
                order['products'].append(product_order)

    return replacement_orders


#Define function to bulk update the replacement orders
def bulk_update_replacement_orders(orders):
    if not orders:
        print("No replacement orders to update")
        return

    bulk_operations = [
        pymongo.UpdateOne({"provider": order["provider"]}, {"$set": order}, upsert=True)
        for order in orders
    ]

    if bulk_operations:
        replacement_orders_collection.bulk_write(bulk_operations)
        print("Replacement orders updated successfully")
    else:
        print("No bulk operations to execute")

# Define a function to batch process sales data
def batch_process_sales_data(sales_data, batch_size):
    batches = [sales_data[i:i + batch_size] for i in range(0, len(sales_data), batch_size)]
    for batch in batches:
        process_batch(batch)

# Define a function to optimize database queries for replacement orders
def optimize_database_queries():
    # Create an index on the "provider" field for efficient querying
    replacement_orders_collection.create_index("provider")

# Main function to generate replacement orders and update/insert them
def main():
    replacement_orders = generate_replacement_orders()
    bulk_update_replacement_orders(replacement_orders)


if __name__ == "__main__":
    main()
