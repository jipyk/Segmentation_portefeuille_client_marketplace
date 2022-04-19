#### libraries #####
import pandas as pd
import numpy as np
##### ORDERS ########
def orders_processing(yyyy, months, ref_for_recence):
    '''
    This function creates theses following target variables by customer: 
        -Number of orders for a periode
        -Numbers of distinct month where any order was passed
        -Last order date
        -Delivering delay(min, max, mean, std)
        -time between 2 orders(min, max, mean, std)
        
    *** INPUT
        yyyy : reference year
        ref_for_recence : date(yyyy-mm-dd) used to find most recent order
    '''
    # Loading orders database
    ORDERS = pd.read_csv('olist_orders_dataset.csv')
    #filtering on right period by year
    ORDERS_yyyy = ORDERS[pd.to_datetime(ORDERS['order_purchase_timestamp']).map(lambda x: x.year)==yyyy]
    #filtering on right period by month
    ORDERS_yyyy = ORDERS_yyyy[pd.to_datetime(ORDERS_yyyy['order_purchase_timestamp']).map(lambda x: x.month).isin(months)]

    #deleting useless columns
    ORDERS_yyyy = ORDERS_yyyy.iloc[:,[0,1,3,7]]
    #Joining customer_unique_id
    CLIENTS = pd.read_csv('olist_customers_dataset.csv')
    ORDERS_yyyy = ORDERS_yyyy.merge(CLIENTS, on='customer_id', how='left')
    #Delivering delay
    ORDERS_yyyy['delay'] = (pd.to_datetime(ORDERS_yyyy['order_estimated_delivery_date']) - pd.to_datetime(ORDERS_yyyy['order_purchase_timestamp'])).map(lambda x: x.days)
    #Order Month
    ORDERS_yyyy['order_month'] = pd.to_datetime(ORDERS_yyyy['order_purchase_timestamp']).map(lambda x: x.strftime('%B'))
    #Number of days after last order
    ORDERS_yyyy['recence'] = (pd.to_datetime(ref_for_recence) - pd.to_datetime(ORDERS_yyyy['order_purchase_timestamp'])).map(lambda x: x.days)
    
    #Sorting database
    ORDERS_yyyy.loc[:,'order_purchase_timestamp'] = pd.to_datetime(ORDERS_yyyy['order_purchase_timestamp'])
    ORDERS_yyyy = ORDERS_yyyy.sort_values(by=['customer_unique_id','order_purchase_timestamp'])
    
    #Days between 2 orders
    ORDERS_yyyy = ORDERS_yyyy.set_index('customer_unique_id')
    ORDERS_yyyy['duree'] = ORDERS_yyyy.pivot_table(index=ORDERS_yyyy.index, values='order_purchase_timestamp', dropna=False, aggfunc='diff')
    ORDERS_yyyy = ORDERS_yyyy.reset_index()
    ORDERS_yyyy['duree'] = ORDERS_yyyy['duree'].map(lambda x: x.days)
    
    #target Variales creation
    valeurs = ['order_id','recence', 'order_month', 'delay', 'duree']
    functions = {'order_id':'count', 'recence': 'min', 'order_month':lambda x: len(x.unique()), 'delay':['min','max', 'mean', 'std'], 'duree':['min','max', 'mean', 'std']}
    df_target = ORDERS_yyyy.pivot_table(index='customer_unique_id', values=valeurs, aggfunc=functions).fillna(0)
    
    return df_target



##### REVIEWS ##########

def reviews_processing(yyyy, months):
    '''
    This function creates theses following target variables by customer: 
        -Number of orders with a score reviews of 5/4 or 1/2
        
    *** INPUT
        yyyy : reference year
        ref_for_recence : date(yyyy-mm-dd) used to find most recent order
    '''
    # Loading orders database
    REVIEWS = pd.read_csv('olist_order_reviews_dataset.csv')
    #deleting useless columns
    REVIEWS = REVIEWS.iloc[:,[1,2,5]]
    #Set date dtype for review_creation_date variable
    REVIEWS.review_creation_date = pd.to_datetime(REVIEWS.review_creation_date)
    #Keeping last reviews for each orders
    REVIEWS = REVIEWS.loc[REVIEWS.groupby('order_id').review_creation_date.idxmax()]
    #Keeping orders for the right period (yyyy)
    ORDERS_KEYS = pd.read_csv('olist_orders_dataset.csv')
    ORDERS_KEYS = ORDERS_KEYS[pd.to_datetime(ORDERS_KEYS['order_purchase_timestamp']).map(lambda x: x.month).isin(months)]
    ORDERS_KEYS = ORDERS_KEYS.loc[pd.to_datetime(ORDERS_KEYS['order_purchase_timestamp']).map(lambda x: x.year)==yyyy, ['order_id','customer_id']]
    #Joining customer_unique_id
    CLIENTS = pd.read_csv('olist_customers_dataset.csv')
    ORDERS_KEYS = ORDERS_KEYS.merge(CLIENTS.iloc[:,[0,1]], on='customer_id', how='left')
    REVIEWS = ORDERS_KEYS.merge(REVIEWS,how='left')
    
    #target Variales creation
    #df_target = REVIEWS.pivot_table(index = 'customer_unique_id', values='order_id', columns='review_score', aggfunc='count')
    df_target = REVIEWS.groupby(by='customer_unique_id').mean()
    #df_target = df_target.fillna(0)
    #df_target['note_4_ou_5'] = df_target[4] + df_target[5]
    #df_target['note_1_ou_2'] = df_target[1] + df_target[2]
    #df_target.drop(range(1,6), axis=1, inplace=True)
    
    return df_target


##### PAYMENTS ##########
def payments_processing(yyyy, months):
    '''
    This function creates theses following target variables by customer: 
        -Number of orders using installments (1,2,3,4,5,10)
        -Numbers of orders sold using credit card
        
    *** INPUT
        yyyy : reference year
    '''
    # Loading orders database
    PAYMENTS = pd.read_csv('olist_order_payments_dataset.csv')
    #deleting useless columns
    PAYMENTS.drop(['payment_type','payment_sequential'], axis=1, inplace=True)
    #Reducing payment installments categories
    PAYMENTS['payment_installments'] = np.where(PAYMENTS['payment_installments'].isin([1,2,3,4,5,10]), PAYMENTS['payment_installments'], '999')
    PAYMENTS['payment_installments'] = PAYMENTS['payment_installments'].map(lambda x:'installment_'+str(x))
    values = PAYMENTS[['order_id', 'payment_value']].groupby(by='order_id').sum()
    PAYMENTS = PAYMENTS.iloc[:,:2].merge(values,left_on='order_id', right_index=True)
    PAYMENTS.drop_duplicates(inplace=True)
    #Reducing payment type categories
    #PAYMENTS['payment_type'] = np.where(PAYMENTS['payment_type']=='credit_card', PAYMENTS['payment_type'], 'paiement comptant')

    #Keeping orders for the right period (yyyy)
    ORDERS_KEYS = pd.read_csv('olist_orders_dataset.csv')
    ORDERS_KEYS = ORDERS_KEYS[pd.to_datetime(ORDERS_KEYS['order_purchase_timestamp']).map(lambda x: x.month).isin(months)]
    ORDERS_KEYS = ORDERS_KEYS.loc[pd.to_datetime(ORDERS_KEYS['order_purchase_timestamp']).map(lambda x: x.year)==yyyy, ['order_id','customer_id']]
    #Joining customer_unique_id
    CLIENTS = pd.read_csv('olist_customers_dataset.csv')
    ORDERS_KEYS = ORDERS_KEYS.merge(CLIENTS.iloc[:,[0,1]], on='customer_id', how='left')
    PAYMENTS = ORDERS_KEYS.merge(PAYMENTS, how='left')
    PAYMENTS.drop_duplicates(inplace=True)
    PAYMENTS = pd.get_dummies(PAYMENTS,columns=['payment_installments'], drop_first=False, prefix='', prefix_sep='')

    #target Variales creation
    df_target = PAYMENTS.pivot_table(index='customer_unique_id', values=['installment_1','installment_2','installment_3','installment_4','installment_5','installment_10','installment_999','payment_value'], aggfunc='sum')

    return df_target




##### ORDERS DETAILS ##########
def odrers_details_processing(yyyy, months):
    '''
    This function creates theses following target variables by customer: 
        - Last order value
        - Order value (min, max, mean, std)
        - Nb of product by order ( min, max, mean, std)
        - Nb categories by order ( min, max, mean, std)
        
    *** INPUT
        yyyy : reference year
    '''
  
    # Loading orders database
    ORDERS_DETAILS = pd.read_csv('olist_order_items_dataset.csv')
    #Global product price
    ORDERS_DETAILS['product_global_price'] = ORDERS_DETAILS.price + ORDERS_DETAILS.freight_value   
    ORDERS_DETAILS['prct_fret_on_global_price'] = ORDERS_DETAILS['freight_value'] / ORDERS_DETAILS['product_global_price']
    #Joining Category for each product
    CATEGORIES = pd.read_csv('product_category_name_translation.csv')
    PRODUCTS = pd.read_csv('olist_products_dataset.csv')
    ORDERS_DETAILS = ORDERS_DETAILS.iloc[:,[0,2,7,8]].merge(PRODUCTS.iloc[:,[0,1]], how='left').replace(CATEGORIES.product_category_name.to_list(), CATEGORIES.product_category_name_english.to_list())

    #Keeping orders for the right period (yyyy)
    ORDERS_KEYS = pd.read_csv('olist_orders_dataset.csv')
    ORDERS_KEYS = ORDERS_KEYS[pd.to_datetime(ORDERS_KEYS['order_purchase_timestamp']).map(lambda x: x.month).isin(months)]
    ORDERS_KEYS = ORDERS_KEYS.loc[pd.to_datetime(ORDERS_KEYS['order_purchase_timestamp']).map(lambda x: x.year)==yyyy, ['order_id','customer_id','order_purchase_timestamp']]
    ORDERS_KEYS['order_purchase_timestamp'] = pd.to_datetime(ORDERS_KEYS['order_purchase_timestamp'])
    
    #Joining customer_unique_id
    CLIENTS = pd.read_csv('olist_customers_dataset.csv')
    ORDERS_KEYS = ORDERS_KEYS.merge(CLIENTS.iloc[:,[0,1]], on='customer_id', how='left')
    
    #Joining orders_details
    ORDERS_DETAILS = ORDERS_KEYS.merge(ORDERS_DETAILS, how='left')

    #target Variales creation
    ## ORDER values
    LAST_ORDERS_AMOUNT = ORDERS_DETAILS.loc[ORDERS_DETAILS.groupby('customer_unique_id').order_purchase_timestamp.idxmax(), ['customer_unique_id','order_id','product_global_price']]
    LAST_ORDERS_AMOUNT = LAST_ORDERS_AMOUNT.pivot_table(index='customer_unique_id', values='product_global_price', aggfunc='sum').rename(columns = {'product_global_price':'last_order_value'})
    
    ## For each order: 
    ### global value, traio fret/global_price
    ORDERS_DETAILS = ORDERS_DETAILS.drop('product_global_price', axis=1).merge(ORDERS_DETAILS.pivot_table(index = 'order_id', values = 'product_global_price', aggfunc='sum'), left_on='order_id', right_index=True)
    #ORDERS_DETAILS = ORDERS_DETAILS.drop('prct_fret_on_global_price', axis=1).merge(ORDERS_DETAILS.pivot_table(index = 'order_id', values = 'prct_fret_on_global_price', aggfunc='mean'), left_on='order_id', right_index=True)

    ### numbers of products
    ORDERS_DETAILS = ORDERS_DETAILS.merge(ORDERS_DETAILS.order_id.value_counts().rename('Product count'), how = 'left', right_index=True, left_on='order_id')
    ### number of disctinct categories
    ORDERS_DETAILS = ORDERS_DETAILS.drop('product_category_name', axis=1).merge(ORDERS_DETAILS.pivot_table(index='order_id', values = 'product_category_name', aggfunc = lambda x: len(x.unique())),
                                                                                left_on='order_id', right_index=True)
    
    valeurs = ['product_global_price', 'product_category_name', 'Product count','prct_fret_on_global_price' ]
    
    fonctions = {'product_global_price':['min','max','mean','std'], 
                 'prct_fret_on_global_price':['mean'],
             'product_category_name':['min','max','mean','std'],
            'Product count':['min','max','mean','std']}

    df_target = ORDERS_DETAILS.pivot_table(index='customer_unique_id', values=valeurs, aggfunc=fonctions).fillna('0')
    df_target = df_target.merge(LAST_ORDERS_AMOUNT, right_index=True, left_index=True)
    
    return df_target


##### DATABASE GENERATOR ##########
def db_generator(yyyy,months, ref_for_recence) :
    '''
    This functions merge all small databases provided by thematic processing functions.
    
    *** INPUT
    yyyy : reference year
    ref_for_recence : date(yyyy-mm-dd) used to find most recent order
    '''
    
    #Adding orders processed databasde
    db = orders_processing(yyyy,months, ref_for_recence)
    
    #Adding orders details processed databse
    db = db.merge(odrers_details_processing(yyyy, months), right_index=True, left_index=True)
    
    #Adding orders details processed databse
    db = db.merge(payments_processing(yyyy, months), right_index=True, left_index=True)
    
    #Adding orders details processed databse
    db = db.merge(reviews_processing(yyyy, months), right_index=True, left_index=True)
    
    # Formatting
    new_col = ['_'.join(col) for col in db.columns[:23]]
    new_col = [x.replace(' ', '_') for x in new_col]
    new_col[9] = 'nb_distinct_order_month'
    old_col = db.columns[:24]
    exchange = dict(zip(old_col, new_col))
    db.rename(columns=exchange, inplace=True)
    
    db.loc[:,'installment_1'] = db['installment_1']/db['order_id_count']
    db.loc[:,'installment_2'] = db['installment_2']/db['order_id_count']
    db.loc[:,'installment_3'] = db['installment_3']/db['order_id_count']
    db.loc[:,'installment_4'] = db['installment_4']/db['order_id_count']
    db.loc[:,'installment_5'] = db['installment_5']/db['order_id_count']
    db.loc[:,'installment_10'] = db['installment_10']/db['order_id_count']
    db.loc[:,'installment_999'] = db['installment_999']/db['order_id_count']

    return db

