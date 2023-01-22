# Overview

At Multiply, we manage the prices of different merchants (our clients) on online marketplaces such as Amazon. In order for us to do this, our clients provide us with some information about the products they sell on these marketplaces. The information may change very often e.g. a merchant may wish to inform us of a product that has gone out of stock or has been restocked. Multiply is required to ingest this information that comes from our clients. Your assignment here is to implement a small part of this data ingestion system.

# Task

Create a system where clients will be able to submit new data or updates to existing product data. Important requirements are:

* Client column names need to be mapped to internal database column names
* Where possible, each piece of data should be verified to ensure it is sensible (datatypes, ranges etc)
* For any product, if any field is inappropriate, no part of that product should be updated. Instead, this product should be included in the error report

The result of an import should be:

* A csv file containing the row data that would be updated/inserted in the products table
* A report  listing any products which were not successfully updated (because of errors such as incorrect input)

You may assume the following things:

* All input update files from our clients will be csv files with the name `{merchant_id}_product_update.csv` where `{merchant_id}` is the merchant’s actual id within Multiply (called `multiply_merchant_id` in the product table) and will always be a positive integer less than 9999
* This task does not need you to perform any database actions. You are simply required to ingest a csv file and transform it into another csv file

The product table contains the following relevant columns:

* multiply_product_id - Internal id for the product (you wont need this for this task)
* multiply_merchant_id - An internal id given by multiply to the merchant (our client)
* merchant_product_id - Merchant’s id for the product
* marketplace_product_id - Marketplace’s id for the product
* name- Name of the product
* max_price_inc_vat
* min_price_inc_vat
* stock_qty - Stock quantity

# Other instructions
You have been provided with some starter code to use to complete this task. While this code is meant to be useful, you are allowed to modify any (or every) part of it that you wish in order to complete the task as you wish. However, please do not use any libraries that are not part of the [python standard library](https://docs.python.org/3/library/index.html).

