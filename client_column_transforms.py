column_name_mappings ={ 
    # multiply_merchant_id: {transform_mapping},
    421: {
        "prod_id": "merchant_product_id",
        "mktplc_prod_id":"marketplace_product_id", 
        "max_selling_price":"max_price_inc_vat", 
        "min_selling_price":"min_price_inc_vat", 
        "inventory_level": "stock_qty",
        "name":"name"
        # You will need to create the rest of this
         },
    276 : {
        "item_id": "merchant_product_id",
        "mkt_plc_prod_id": "marketplace_product_id",
        "highest_price": "max_price_inc_vat",
        "multiply_merchant_id":"multiply_merchant_id",
        "name":"name" 
    },
    # other client column name mappings can go here if needed
}