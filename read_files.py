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
                             'amt']
                             )
    wf = pd.read_csv(file2,
                     header=None,
                     names=
                        ['date',
                         'amt',
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

    desired_cols = ['date', 'amt', 'desc', 'category', 'type']
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
        df['type'] = 'unknown'
        df['category'] = 'unknown'
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

    cats_raw = pd.read_csv(categories_filepath)
    cats_raw['list'] = list(zip(cats_raw.desc, cats_raw.cat, cats_raw.type))
    category_map = dict(zip(list(cats_raw['raw_desc']), list(cats_raw['list'])))

    for idx, row in trans.iterrows():
        raw_desc = trans.loc[idx]['desc']
        if raw_desc[0:10] in category_map.keys():
            trans.loc[idx,'desc'] = category_map[raw_desc[0:10]][0]
            trans.loc[idx,'category'] = category_map[raw_desc[0:10]][1]
            trans.loc[idx,'type'] = category_map[raw_desc[0:10]][2]
        else:
            print 'new trans desc: {} (${})'.format(raw_desc, trans.loc[idx]['amt'])
            new_desc = raw_input("what should the descrpition be? ('s'kip, 'l'ist) > ")
            if new_desc == 's':
                pass
            else:
                if new_desc == 'l':
                    for item in cats_raw.desc:
                        print item
                    new_desc = raw_input("what should the descrpition be? > ")
                new_category = raw_input('what should the category be? > ')
                if trans.loc[idx]['amt'] < 0:
                    new_type = 'payment'
                else:
                    new_type = 'deposit'
                category_map[raw_desc[0:10]] = [new_desc, new_category, new_type]
                print 'thanks. new merchant added.'

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
    cats = '../data/categories.csv'
    dfs = read_in(chase_raw, wf_raw)
    trans = organize(dfs)
    # print trans
    ayo = categorize(trans, cats)
