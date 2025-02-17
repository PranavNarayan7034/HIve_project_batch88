from pyhive import hive
import hive_fns
import analytics
import numpy as np

try:
    c = hive.connect(host="localhost",
                     database="batch88").cursor()
    print('Connection established...')
    data_path = '/home/admin1/Downloads/online_sales_dataset.csv'
    try:
        c.execute(hive_fns.create_table('online_sales',
                                        hive_fns.online_sales_columns,
                                        ','))
        c.execute(hive_fns.insert_data(data_path,'online_sales'))
        print('Table created Successfully....')
    except Exception as e:
        print(f'Error in table creation...: {e}')

    #Analytics
    # 1 : which payment method have more transactions
    try:
        c.execute(analytics.analytics_1())
        print(f'No.of transactions in each payment method: {c.fetchall()}')
    except Exception as e:
        print(f'Error in Analytics one: {e}')

    # 2 : No of orders in each warehouses with visualization
    c.execute(analytics.analytics_2())
    result = c.fetchall()
    print(f'No.of orders in Warehouses : {result}')
    x_values = []
    y_values = []
    for i in result:
        if i[0]!= '':
            x_values.append(i[0])
            y_values.append(i[1])
    # analytics.plot_bar_graph(x_values,y_values,'orders in warehouse',
    #                          'warehouse','count')
    # 3: find the average shipping cost in each country
    # 4 : find no.of different mode of transaction in each country
    print('Result of Analytics 4 ..........')
    c.execute(analytics.analytics_4())
    c.execute('select distinct country from c_transaction')
    x_names = c.fetchall()
    c.execute('select * from c_transaction')
    x_values = np.arange(1,len(x_names)+1)
    y_values = c.fetchall()
    bt = []
    cc = []
    pp = []
    for i in y_values:
        if i[1] == 'Bank Transfer':
            bt.append(i[2])
        elif i[1] == 'Credit Card':
            cc.append(i[2])
        elif i[1] == 'paypall':
            pp.append(i[2])

    analytics.graph_2(x_values,x_names,bt,cc,pp)


except Exception as e:
    print(f'Error in connection: {e}')


# group : country : australia, spain, germany
#         paymentmethod: credit,paypal,bt