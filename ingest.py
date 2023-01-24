import csv
from decimal import Decimal



NoneType = type(None)

# Data type validators
def is_decimal(data):
    return isinstance(data, Decimal)

def is_int_nullable(data):
    return isinstance(data, (int, NoneType))

def is_string(data):
    return isinstance(data, str)
   


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
        # errors = []
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
                    error = f"Could not transform value({value}) to {_type}"
                    # errors.append(text)
                    return value, error # At this point there is no need in moving on to other validators
        return value, None
        
   

    
    
    def restructure(self,row): 
        item = {}
        err = []
        for client_column_name, value in row.items(): 
            try:
                multiply_column_name = self.column_name_mapping[client_column_name]
                transformed_value, validation_error = self.validate(value, multiply_column_name)
                # Check if there were any errors during validation, if there were,
                # Create a dict with a few picked out values and add to the error list
                if validation_error: 
                    error_item = {
                        "client_column_name": client_column_name,
                        "multiply_column_name": multiply_column_name, 
                        "value":value, 
                        "error_description":validation_error, 
                    }
                    err.append(error_item)
                # No errors occured during validation, so we can transfer the various column fields 
                # and their transformed values
                else: item[multiply_column_name] = transformed_value 
                
            except KeyError: 
                # If client data contains any unknown new columns, this error will be thrown. To ensure that corresponding column in multiply database is made
                err.append({
                    "client_column_name":client_column_name,
                    "multiply_column_name": "-", 
                    "value":value,
                    "error_description":f"Could not find table mapping corresponding to ({client_column_name}) ",
                    })
        return item, err
        
    

    def generate_output_file_contents(self):
        # 1. read input file
        # 2. validate data
        # 3. create output contents (inject multiply merchant id here)
        out_rows = []
        err_rows = []
        
        with open(self.file_path, mode="r") as client_file: 
            reader = csv.DictReader(client_file) 
            for row in reader: 
                ready_item, errors = self.restructure(row)
                if len(errors): 
                    err_rows.append({"full_product":str(row), "errors":errors})
                else: out_rows.append(ready_item)
               
        return  out_rows, err_rows

    def generate_error_string(self,errors):
        temp = ""        
        for obj in errors: 
            product = obj["full_product"]
            _errors = obj["errors"]
            temp+="AFFECTED PRODUCT \n"
            temp+=f"{product}\n"
            temp+="--------------------------------------\n"
            temp +="ERRORS \n"
            temp+="--------------------------------------\n"
            for i,error in enumerate(_errors): 
                ccn = error["client_column_name"]
                mcn = error["multiply_column_name"]
                value = error["value"]
                description = error["error_description"]
                temp += f"{str(i+1)}. Client Column Name: {ccn}\n"
                temp += f"Column Name at Multiply : {mcn}\n" 
                temp += f"Value : {value}\n" 
                temp += f"Error : {description}\n" 
                temp +="\n"
            temp +="\n"
        return temp
