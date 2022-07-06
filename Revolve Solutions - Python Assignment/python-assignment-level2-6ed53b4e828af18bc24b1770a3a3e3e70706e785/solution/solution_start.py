import argparse
import os


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


def main():
    params = get_params()
    # PYSpark Code
    from pyspark.sql import SparkSession, functions as f
    spark = SparkSession.builder.master('local').appName('python').getOrCreate()
    df = spark.read.csv('customers.csv', header=True, inferSchema=True)
    df1 = spark.read.csv('products.csv', header=True, inferSchema=True)
    df2 = spark.read.json('transactions/d=2018-12-01/transactions.json', multiLine=False)
    df3 = df2.withColumn('b', f.explode('basket')).drop("basket").select('customer_id', 'b.product_id', 'b.price',
                                                                         'date_of_purchase')
    df4 = df3.groupBy('customer_id').count()
    df5 = df4.join(df3, df4.customer_id == df3.customer_id, "inner").select(df4['customer_id'], 'product_id', 'count'). \
        join(df, df.customer_id == df4.customer_id, 'inner').select(df4['customer_id'], 'loyalty_score', 'product_id','count'). \
        join(df1, df1.product_id == df3.product_id, 'inner').select('customer_id', 'loyalty_score', df1['product_id'],
                                                                    'product_category', 'count').show()


if __name__ == "__main__":
    main()
