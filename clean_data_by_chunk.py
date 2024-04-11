print(f'beginning to load data to memory')
import pickle

# open pickle files

# splitting into 11 pieces to speed up runtime via running in parallel terminals
# comment and uncomment based on which data chunk which is to be cleaned
dbfile0 = open('county_data_0.pkl', 'rb')
db0 = pickle.load(dbfile0)
dbfile0.close()

# dbfile1 = open('county_data_1.pkl', 'rb')
# db1 = pickle.load(dbfile1)
# dbfile1.close()

# dbfile2 = open('county_data_2.pkl', 'rb')
# db2 = pickle.load(dbfile2)
# dbfile2.close()

# dbfile3 = open('county_data_3.pkl', 'rb')
# db3 = pickle.load(dbfile3)
# dbfile3.close()

# dbfile4 = open('county_data_4.pkl', 'rb')
# db4 = pickle.load(dbfile4)
# dbfile4.close()

# dbfile5 = open('county_data_5.pkl', 'rb')
# db5 = pickle.load(dbfile5)
# dbfile5.close()

# dbfile6 = open('county_data_6.pkl', 'rb')
# db6 = pickle.load(dbfile6)
# dbfile6.close()

# dbfile7 = open('county_data_7.pkl', 'rb')
# db7 = pickle.load(dbfile7)
# dbfile7.close()

# dbfile8 = open('county_data_8.pkl', 'rb')
# db8 = pickle.load(dbfile8)
# dbfile8.close()

# dbfile9 = open('county_data_9.pkl', 'rb')
# db9 = pickle.load(dbfile9)
# dbfile9.close()

# dbfile10 = open('county_data_10.pkl', 'rb')
# db10 = pickle.load(dbfile10)
# dbfile10.close()

print(f'finished loading data to memory')

# OPTIONAL: combine dictionaries into one dictionary
df_dict = dict()
df_dict.update(db0)
# df_dict.update(db1)
# df_dict.update(db2)
# df_dict.update(db3)
# df_dict.update(db4)
# df_dict.update(db5)
# df_dict.update(db6)
# df_dict.update(db7)
# df_dict.update(db8)
# df_dict.update(db9)
# df_dict.update(db10)

print(f'number records to clean: {len(df_dict)}')

# remove obviously bad data
df_dict = {key: value for key, value in df_dict.items() if not isinstance(value, str)}

print(f'num records after initial clean: {len(df_dict)}')

# Create various functions for parsing specific types of data
def prepare_assessed_value_df(df):
    df_temp = pd.DataFrame()
    df_temp = df.T.rename(columns=df.T.loc[0]).drop(index=0)
    df_temp.reset_index(drop=True, inplace=True)
    
    return df_temp

def prepare_govt_entity_df(df):
    df_temp = df.T.rename(columns=df.T.iloc[0]).iloc[1:]
    for col in df_temp.columns:
        df_temp.insert(df_temp.columns.get_loc(col) + 1, 'Tax Amount-' + col, df_temp.loc[df_temp.index[df_temp.index.str.contains('Amount')][0], col])
    df_temp.columns = ['Rate-' + str(col) if i % 2 == 0 else str(col) for i, col in enumerate(df_temp.columns)]
    df_temp.drop(df_temp.index[1], inplace=True)
    df_temp.reset_index(drop=True, inplace=True)
    
    return df_temp

def prepare_land_improvements_df(df):
    df_temp = df.T.rename(columns=df.T.loc['Description']).drop(index='Description')
    for col in df_temp.columns:
        df_temp.insert(df_temp.columns.get_loc(col) + 1, 'Tax Rate-' + col, df_temp.loc[df_temp.index[df_temp.index.str.contains('Rate')][0], col])
        df_temp.insert(df_temp.columns.get_loc(col) + 2, 'Tax Amount-' + col, df_temp.loc[df_temp.index[df_temp.index.str.contains('Amount')][0], col])
    df_temp.columns = ['Values-' + str(col) if i % 3 == 0 else str(col) for i, col in enumerate(df_temp.columns)]
    df_temp.drop(df_temp.index[2], inplace=True)
    df_temp.drop(df_temp.index[1], inplace=True)
    df_temp.reset_index(drop=True, inplace=True)
    
    return df_temp

def prepare_code_df(df):
    df_temp = df.T.rename(columns=df.T.fillna('SUBTOTAL').loc['Assessment Name']).drop(index=['Code','Assessment Name','Contact Number'])
    df_temp.reset_index(drop=True, inplace=True)
    
    return df_temp

def prepare_state_water_project2_df(df):
    df_temp = df.add_suffix('-State Water Project')
    df_temp.drop(columns=df_temp.columns[0], axis=1, inplace=True)
    df_temp.drop(df_temp.index[1], inplace=True)

    return df_temp

def prepare_state_water_project3_df(df):
    df_temp = df.add_suffix('-State Water Project')
    df_temp.drop(df_temp.index[2], inplace=True)
    for col in df_temp.columns:
        df_temp.insert(df_temp.columns.get_loc(col) + 1, col.split('-')[0] + '-' + df_temp.iloc[1,0], df_temp[col].iloc[1])
    df_temp.drop(df_temp.index[1], inplace=True)
    df_temp.drop(columns=df_temp.columns[0:2], axis=1, inplace=True)
    df_temp.reset_index(drop=True, inplace=True)

    return df_temp

def prepare_countywide_df(df):
    df_temp = df.T.rename(columns=df.T.loc['Description']).drop(index='Description')
    df_temp.drop(df_temp.index[0], inplace=True)
    for col in df_temp.columns:
        df_temp.insert(df_temp.columns.get_loc(col) + 1, 'Tax Amount-' + col, df_temp.loc[df_temp.index[df_temp.index.str.contains('Amount')][0], col])
    df_temp.columns = ['Rate-' + str(col) if i % 2 == 0 else str(col) for i, col in enumerate(df_temp.columns)]
    df_temp.drop(df_temp.index[1], inplace=True)
    df_temp.reset_index(drop=True, inplace=True)

    return df_temp

print(f'beginning deep clean...may take very many hours')
import pandas as pd
pd.set_option('display.max_columns', None)
import datetime
import time

t1 = time.time()
df_final = pd.DataFrame()
for key, values in df_dict.items():
    # get metadata
    metadata = pd.DataFrame({'Fiscal Year':[str(datetime.datetime.now().year-1)+'-'+str(datetime.datetime.now().year)],'APN Suffix':[key],'Installment  Number':[1],'Tax Amount':[None],'Additional  Charges':[None], 'My  Payments':[None], 'Payment  Posted':[None]})
    
    year_counter0 = 1
    year_counter1 = 1
    year_counter2 = 1
    year_counter3 = 1
    year_counter4 = 1
    year_counter5 = 1
    df0 = pd.DataFrame()
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    df4 = pd.DataFrame()
    df5 = pd.DataFrame()

    for df in values:
        if df[df.columns[0]].str.contains('Assessed Value').any():
            if year_counter0 == 1:
                df0 = prepare_assessed_value_df(df)
                year_counter0 += 1
                continue
    
        if df.columns[0] == 'Government Entity':
            if year_counter1 == 1:
                df1 = prepare_govt_entity_df(df)
                year_counter1 += 1
                continue
    
        if df[df.columns[0]].str.contains('Total Land and Improvements').any():
            if year_counter2 == 1:
                df2 = prepare_land_improvements_df(df)
                year_counter2 += 1
                continue
    
        if df.columns[0] == 'Code':
            if year_counter3 == 1:
                df3_curr = prepare_code_df(df)
                year_counter3 += 1
                continue
        
        if df[df.columns[0]].str.contains('State Water Project').any() and len(df) == 2:
            if year_counter4 == 1:
                df4_curr = prepare_state_water_project2_df(df)
                year_counter4 += 1
                continue
    
        if df[df.columns[0]].str.contains('State Water Project').any() and len(df) == 3:
            if year_counter4 == 1:
                df4_curr = prepare_state_water_project3_df(df)
                year_counter4 += 1
                continue
    
        if df[df.columns[0]].str.contains('Countywide 1%').any():
            if year_counter5 == 1:
                df5_curr = prepare_countywide_df(df)
                year_counter5 += 1
                continue
    
    df_final = pd.concat([df_final, pd.concat([metadata, df0, df1, df2, df3, df4, df5], axis=1)], ignore_index=True)

t2 = time.time()
print(f'elapsed time: {t2-t1}')
print(f'finished deep cleaning')
print(f'final record count: {len(df_final)}')
df_final.to_csv('df_final0.csv', index=False) # increment to 1,2,3,4,5,6,7,8,9,10 as necessary for individual runs
print(f'saved final csv as "df_final#.csv where # is the number of the data chunk specified')
