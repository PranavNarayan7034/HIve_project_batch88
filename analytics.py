# 1 : which payment method have more transactions

def analytics_1():
    query = '''select PaymentMethod,count(*)as No_of_transaction 
    from online_sales group by PaymentMethod order by No_of_transaction desc'''
    return query

def analytics_2():
    query = ''' select WarehouseLocation,Count(*) as count 
    from online_sales group by WarehouseLocation'''
    return query

import matplotlib.pyplot as plt
def plot_bar_graph(x,y,title,xaxis,yaxis):
    plt.bar(x,y,color = 'r')
    plt.title(title)
    plt.xlabel(xaxis)
    plt.ylabel(yaxis)
    plt.show()

def analytics_4():
    query = '''create table if not exists c_transaction as 
        select Country,PaymentMethod,count(*) as count
        from online_sales group by Country,PaymentMethod'''
    return query

def graph_2(x_values,x_names,y1,y2,y3):
    bar_width = 0.25
    plt.bar(x_values-bar_width,y1,bar_width,color='red',label='BankTransaction')
    plt.bar(x_values,y2,bar_width,color='yellow',label='CreditCard')
    plt.bar(x_values +bar_width,y3,bar_width,color='blue',label='Paypall')
    plt.xticks(x_values,x_names)
    plt.legend()
    plt.show()