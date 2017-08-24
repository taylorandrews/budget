import pandas as pd
import numpy as np
import datetime

def read_in(file1, file2):
    """
    simply reads in the raw csv trasactions into dataframes

    INPUT
        - filepath for chase banking activity [string]
        - filepath for wells fargo banking activity [string]

    OUPUT
        - list of dataframes with raw transaction data read in [list]
    """

    chase = pd.read_csv(file1,
                        header=0,
                        names=
                            ['type',
                             'date',
                             'del',
                             'desc',
                             'amount']
                             )
    wf = pd.read_csv(file2,
                     header=None,
                     names=
                        ['date',
                         'amount',
                         'del',
                         'del1',
                         'desc'])
    chase.name = 'c'
    wf.name = 'w'

    return [chase, wf]

def organize(dfs):
    """
    takes out unnecessary data from the raw dfs
    changes date to correct datatype
    concatinates wells fargo and chase transactions
    sets unique transaction ids as primary key

    INPUT
        - list of dataframes with raw transaction data read in [list]

    OUTPUT
        - dataframe with all transaction data, all types, categories unknown [dataframe]
    """

    desired_cols = ['merchant', 'date', 'amount', 'category', 'subcategory', 'desc']
    for df in dfs:
        for col in df.columns:
            if col[0:3] == 'del':
                df.drop([col], axis=1, inplace=True)
        df['trans_id'] = 0
        df['date'] = pd.to_datetime(df['date'])
        df['trans_id'] = df['date'].map(lambda x: x.month*10000)
        df['trans_id'] = df['trans_id'] + df.index
        df['trans_id'] = df.trans_id.map(str) + df.name
        df.set_index('trans_id', inplace=True)
        df['merchant'] = 'unknown'
        df['category'] = 'unknown'
        df['subcategory'] = 'unknown'
        df = df[desired_cols]
    return pd.concat(dfs)

def categorize(trans, categories_filepath):
    """
    changes desc and type columns to appropriate values
    asks user about unfamiliar transactions and updates/saves categories.csv

    INPUT
        - dataframe with all transaction data, all types unknown [dataframe]
        - filepath for category data [string]

    OUTPUT
        - updated and categorized dataframe
    """

    columns = ['raw_desc', 'desc', 'cat', 'type']
    avail_subcat = ['debt_payment', 'savings', 'rent', 'car', 'clothes', 'exercise', 'gifts', 'groceries', 'life', 'recreation', 'restaurants', 'travel', 'other']

    cats_raw = pd.read_csv(categories_filepath)
    cats_raw['list'] = list(zip(cats_raw.merchant, cats_raw.subcategory, cats_raw.category))
    category_map = dict(zip(list(cats_raw['raw_desc']), list(cats_raw['list'])))
    print ":", category_map

    for idx, row in trans.iterrows():
        raw_desc = trans.loc[idx]['desc']
        if raw_desc[0:10] in category_map.keys():
            trans.loc[idx,'merchant'] = category_map[raw_desc[0:10]][0]
            trans.loc[idx,'subcategory'] = category_map[raw_desc[0:10]][1]
            trans.loc[idx,'category'] = category_map[raw_desc[0:10]][2]
        else:
            print 'new trans desc: {} (${})'.format(raw_desc, trans.loc[idx]['amount'])
            new_desc = raw_input("merchant? ('s'kip, 'l'ist) > ")
            if new_desc == 's':
                pass
            else:
                if new_desc == 'l':
                    for item in cats_raw.desc:
                        print item
                    new_merchant = raw_input("merchant? > ")
                new_subcategory = raw_input("subcategory? 'c'hoices > ")
                if new_subcategory == 'c':
                    print avail_subcat
                    new_subcategory = raw_input("subcategory? > ")
                if trans.loc[idx]['amount'] < 0:
                    new_category = 'expense'
                else:
                    new_type = 'revenue'
                category_map[raw_desc[0:10]] = [new_merchant, new_subcategory, new_category]
                print 'thanks. new merchant added.'

############################################# ended here
######### validate this function
######### then break up the df with small dataset

    pd.DataFrame(
        np.hstack(
            (np.array(category_map.keys())[:, None],
             np.array(category_map.values()))), columns=columns).to_csv(categories_filepath)

    return trans


if __name__ == '__main__':
    # month = datetime.datetime.now().strftime("%B")
    # year = datetime.datetime.now().strftime("%Y")
    month = 'june'
    year = '2017'
    chase_raw = '../data/{}/{}/chase_{}.csv'.format(year, month, month)
    wf_raw = '../data/{}/{}/wf_{}.csv'.format(year, month, month)
    cats = '../data/categories_new.csv'
    dfs = read_in(chase_raw, wf_raw)
    trans = organize(dfs)
    # print trans
    ayo = categorize(trans, cats)
