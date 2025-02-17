def create_table(table_name,column_info,sep):
    table = f'''create table if not exists {table_name} ( {column_info} )
    row format delimited fields terminated by '{sep}'
    tblproperties('skip.header.line.count'='1')'''
    return table

online_sales_columns = '''InvoiceNo bigint,StockCode string,Description string,
Quantity int,InvoiceDate string,UnitPrice float,CustomerID float,
Country string,Discount float,PaymentMethod string,ShippingCost float,
Category string,SalesChannel string,ReturnStatus string,
ShipmentProvider string,WarehouseLocation string,OrderPriority string'''


def insert_data(path,table_name):
    data = f'''load data local inpath '{path}' 
                overwrite into table {table_name}'''
    return data