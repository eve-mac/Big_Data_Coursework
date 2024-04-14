# Import necessary packages
import pymongo
import os
import datetime

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


# Connect to MongoDB
client = pymongo.MongoClient(uri,
                             tls=True,
                             tlsCertificateKeyFile="/Users/evemcaleer/Documents/X509-cert-4240541237570912733.pem")

# Specify database and collections
db = client["B30DM"]
sales_collection = db["sales"]
products_collection = db["products"]
replacement_orders_collection = db["replacement_orders_emm"]

print('Connection successful')


# Define function to query sales data for the last 28 days
def query_sales_data():
    # Calculate the start and end dates for the last 28 days
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=28)

    # Define the pipeline to aggregate sales data for the specified time range
    pipeline = [
        {"$match": {"timestamp": {"$gte": start_date, "$lt": end_date}}},
        {"$group": {"_id": {"provider": "$product.provider", "code": "$product.code"}, "total_sold": {"$sum": 1}}}
    ]

    # Execute the aggregation pipeline and return the result
    return sales_collection.aggregate(pipeline)



# Define function to retrieve product information
def query_products_data():
    pipeline = [
        {"$group": {"_id": "$provider", "products": {"$push": {"code": "$code", "name": "$name", "wholesale_price": "$wholesale_price"}}}}
    ]
    return products_collection.aggregate(pipeline)


# Define the function to generate replacement orders
def generate_replacement_orders():
    # Retrieve sales and products data
    sales_data = query_sales_data()
    products_data = query_products_data()
    current_month = datetime.datetime.now().strftime("%Y-%m")  # Get current month in YYYY-MM format

    # Create a dictionary to store product details for each provider
    provider_products = {doc['_id']: doc['products'] for doc in products_data}

    # Generate replacement orders based on sales data
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


# Define function to bulk update the replacement orders
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


def optimize_database_queries():
    # Create an index on the "provider" field for efficient querying
    replacement_orders_collection.create_index("provider")

    # Create an index on the "code" field for efficient querying in the products collection
    #products_collection.create_index("code")

    # Create an index on the "timestamp" field for efficient querying in the sales collection
    #sales_collection.create_index("timestamp")


# Main function to generate replacement orders and update/insert them
def main():
    # Optimize database queries for replacement orders
    optimize_database_queries()  # Create index on "provider" field

    # Generate replacement orders
    replacement_orders = generate_replacement_orders()

    # Update/insert replacement orders in bulk
    bulk_update_replacement_orders(replacement_orders)


if __name__ == "__main__":
    main()
