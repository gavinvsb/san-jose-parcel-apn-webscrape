import pandas as pd
df0 = pd.read_csv('df_final0.csv')
df1 = pd.read_csv('df_final1.csv')
df2 = pd.read_csv('df_final2.csv')
df3 = pd.read_csv('df_final3.csv')
df4 = pd.read_csv('df_final4.csv')
df5 = pd.read_csv('df_final5.csv')
df6 = pd.read_csv('df_final6.csv')
df7 = pd.read_csv('df_final7.csv')
df8 = pd.read_csv('df_final8.csv')
df9 = pd.read_csv('df_final9.csv')
df10 = pd.read_csv('df_final10.csv')

df_final = pd.concat([df0, pd.concat([metadata,df1,df2,df3,df4,df5,df6,df7,df8,df9,df10], axis=1)], ignore_index=True)

df_final.to_csv('df_final.csv', index=False)
