import csv
from decimal import Decimal
import enum
from types import NoneType

# Data type validators
def is_decimal(data):
    return isinstance(data, Decimal)
    # res = isinstance(data, Decimal)
    # if not res: return res, "Not a decimal"
    # return res, None

def is_int_nullable(data):
    return isinstance(data, (int, NoneType))
    # res = isinstance(data, (int, NoneType))
    # if not res: return res, "Not a valid int"
    # return res, None

def is_string(data):
    return isinstance(data, str)
    # res = isinstance(data, str)
    # if not res: return res, "Not a valid string"
    # return res, None


# Read column names (header)
class MerchantDataFileHandler:

    def __init__(self, multiply_merchant_id, file_path, column_name_mapping):
        self.multiply_merchant_id = multiply_merchant_id
        self.file_path = file_path
        self.column_name_mapping = column_name_mapping

        # TODO: Add more column validators and 
        # data transformers as appropriate
        self.column_data_validators = {
            'merchant_product_id': [is_string,],
            'marketplace_product_id': [is_string,],
            'name': [is_string,],
            'max_price_inc_vat': [is_decimal],
            "min_price_inc_vat":[is_decimal],
            "multiply_merchant_id":[is_int_nullable],
            "stock_qty":[is_string],
            # ... other validators
        }

        self.column_data_transformers = {
            'merchant_product_id': [str,],
            'marketplace_product_id': [str,],
            'name': [str,],
            'max_price_inc_vat': [Decimal,],
            "min_price_inc_vat":[Decimal,],
            "multiply_merchant_id":[int,],
            "stock_qty":[int,],
            
            # ... other data transformers
        }

    def validate(self,value, column_name):  
        validators = self.column_data_validators[column_name]
        transformers = self.column_data_transformers[column_name]
        errors = []
        # The assumptions made here: 
        # There are multiple validators 
        # For every validator provided, there will be a matching transformation type at the same index
     
        # NB: If there are multiple validators, validators will receive already transformed values by previous validators in the list 

        for index, valid in enumerate(validators):
            _type = transformers[index]
            if not valid(value): 
                try: 
                    value = _type(value)
                except: 
                    text = f"Could not transform value({value}) to {_type}"
                    errors.append(text)
                    return value, errors # At this point there is no need in moving on to other validators
        return value, None
        
   

    
    
    def arrange(self,row): 
        item = {}
        err = []
        # for client_table_key, our_table_key in self.column_name_mapping.items(): 
        for client_table_key, value in row.items(): 
            try:
                our_table_key = self.column_name_mapping[client_table_key]
                transformed_value, errors = self.validate(value, our_table_key)
                if errors: 
                    pass
                item[our_table_key] = transformed_value 
                
            except KeyError: 
                # If client data contains any new columns, this error will be thrown. To ensure that corresponding column in multiply database is made
                err.append(f"Could not find table mapping corresponding to ({client_table_key}) ")
                
        return item, err
        
    def generate_output_file_contents(self):
        # 1. read input file
        # 2. validate data
        # 3. create output contents (inject multiply merchant id here)
        out_rows = []
        err_rows = []

        # TODO: Implement me 
        
        with open(self.file_path, mode="r") as client_file: 
            reader = csv.DictReader(client_file) 
            for row in reader: 
                print(row)
                print("Now transformed into: ")
                print(self.arrange(row))
                print("------------------------------")
               


        
        return  out_rows, err_rows
