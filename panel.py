import pandas as pd
import os
import glob
import numpy as np
from pathlib import Path
import fuzzymatcher
import recordlinkage
import math

df2016 = pd.read_csv("/Users/malavikakalani/Desktop/matches crf/finalmatches2016.csv") 
df2016['Year'] = 2016
df2017 = pd.read_csv("/Users/malavikakalani/Desktop/matches crf/finalmatches2017.csv") 
df2017['Year'] = 2017
df2018 = pd.read_csv("/Users/malavikakalani/Desktop/matches crf/finalmatches2018.csv") 
df2018['Year'] = 2018
df2019 = pd.read_csv("/Users/malavikakalani/Desktop/matches crf/finalmatches2019.csv") 
df2019['Year'] = 2019

df2020 = pd.read_csv("/Users/malavikakalani/Desktop/matches crf/finalmatches2020.csv") 
df2020['Year'] = 2020
df2020.loc[(df2020['Year'] == 2020) & (df2020['schID'].astype(str).str.startswith('370')), 'schID'] = df2020['schID'].astype(str).str[3:]
df2020['schID'] = df2020['schID'].astype(str).str.lstrip('0')
df2020['schID'] = df2020['schID'].astype(int)

df2021 = pd.read_csv("/Users/malavikakalani/Desktop/matches crf/finalmatches2021.csv") 
df2021['Year'] = 2021
df2021.loc[(df2021['Year'] == 2021) & (df2021['schID'].astype(str).str.startswith('370')), 'schID'] = df2021['schID'].astype(str).str[3:]
df2021['schID'] = df2021['schID'].astype(str).str.lstrip('0')
df2021['schID'] = df2021['schID'].astype(int)



def get_unmatched_schools(schools_filepath, matches_filepath):
    df_all = pd.read_csv(schools_filepath)
    df_matched = pd.read_csv(matches_filepath)
    df_all = df_all.rename(columns={'school_id': 'schID'})
    #df_all = df_all.rename(columns={'school_level': 'School_level'})
    merged_df = pd.merge(df_all, df_matched, on='schID', how='outer', indicator=True)
    merged_df.to_csv("/Users/malavikakalani/Desktop/schoolcheck.csv")
    merged_df = merged_df.rename(columns={'leaid': 'leaID'})
    merged_df = merged_df.rename(columns={'school_name': 'School_Name'})
    merged_df = merged_df.rename(columns={'school_level': 'School_level'})
    merged_df = merged_df.rename(columns={'year': 'Year'})
    merged_df['Year'] = merged_df['Year'].replace('mixed - most recent', 2021)
    merged_df.loc[(merged_df['Year'].isin([2020, 2021])) & (merged_df['schID'].astype(str).str.startswith('370')), 'schID'] = merged_df['schID'].astype(str).str[3:]
    merged_df['schID'] = merged_df['schID'].astype(str).str.lstrip('0')
    merged_df['schID'] = merged_df['schID'].astype(int)
    df_unmatched = merged_df[merged_df['_merge'] == 'left_only'].copy()
    df_unmatched = df_unmatched[['schID', 'Year', 'leaID', 'School_Name', 'School_level']]
    df_unmatched = df_unmatched.dropna(axis = 1, how = 'all')
    

    return df_unmatched

df_unmatched2016 = get_unmatched_schools("/Users/malavikakalani/Desktop/CRF 2023/schools_2016.csv", "/Users/malavikakalani/Desktop/matches crf/finalmatches2016.csv")
df_unmatched2017 = get_unmatched_schools("/Users/malavikakalani/Desktop/CRF 2023/schools_2017.csv", "/Users/malavikakalani/Desktop/matches crf/finalmatches2017.csv")
df_unmatched2018 = get_unmatched_schools("/Users/malavikakalani/Desktop/CRF 2023/schools_2018.csv", "/Users/malavikakalani/Desktop/matches crf/finalmatches2018.csv")
df_unmatched2019 = get_unmatched_schools("/Users/malavikakalani/Desktop/CRF 2023/schools_2019.csv", "/Users/malavikakalani/Desktop/matches crf/finalmatches2019.csv")
df_unmatched2020 = get_unmatched_schools("/Users/malavikakalani/Desktop/CRF 2023/schools_2020.csv", "/Users/malavikakalani/Desktop/matches crf/finalmatches2020.csv")
df_unmatched2021 = get_unmatched_schools("/Users/malavikakalani/Desktop/CRF 2023/schools_2021.csv", "/Users/malavikakalani/Desktop/matches crf/finalmatches2021.csv")

df_all_unmatched = pd.concat([df_unmatched2016, df_unmatched2017, df_unmatched2018, df_unmatched2019, df_unmatched2020, df_unmatched2021], axis=0, ignore_index=True)

def build_panel(df):
    

    # Select the relevant columns
    df = df[['schID', 'Year', 'leaID', 'School_Name', 'School_level', 'EIN', 'Organization_Name', 'Org_code', 'Revenue']]
    df = df.rename(columns={'Organization_Name': 'Org_name'})
    df = df.rename(columns={'Revenue': 'Rev'})
    # Sort the dataframe by School_Name
    sorted_df = df.sort_values('School_Name')

    # Add a column to serve as a unique identifier for each school
    sorted_df['School_org'] = sorted_df.groupby('School_Name').cumcount() + 1

    # Pivot the dataframe
    pivoted_df = sorted_df.pivot_table(index=['schID','Year', 'leaID', 'School_Name', 'School_level'], columns='School_org', values=['EIN','Org_name', 'Org_code','Rev'])

    # Flatten the multi-level column index and assign new column names
    pivoted_df.columns = [f'{col[0]}{col[1]}' for col in pivoted_df.columns.to_flat_index()]

    # Get the number of unique School_org values
    num_orgs = sorted_df['School_org'].nunique()


    # Create a new list of column names with recurring order
    new_columns = []
    for i in range(1, num_orgs + 1):
        new_columns.extend([f'EIN{i}', f'Org_name{i}', f'Org_code{i}', f'Rev{i}'])

    # Reindex the columns in the pivoted dataframe
    pivoted_df = pivoted_df.reindex(columns=new_columns)

    # Get the unique organization names and codes for each school and school organization
    unique_org_names = sorted_df.groupby(['schID', 'Year','leaID', 'School_Name', 'School_level','School_org'])['Org_name'].first().unstack()
    unique_org_codes = sorted_df.groupby(['schID','Year', 'leaID', 'School_Name','School_level', 'School_org'])['Org_code'].first().unstack()
    unique_revs = sorted_df.groupby(['schID','Year', 'leaID', 'School_Name', 'School_level','School_org'])['Rev'].first().unstack()
    unique_revs.fillna(0, inplace=True)
    #unique_revs.to_csv("/Users/malavikakalani/Desktop/revs.csv")

    # Assign unique organization names, codes and revenue to the pivoted dataframe
    for i in range(1, num_orgs + 1):
        pivoted_df[f'Org_name{i}'] = unique_org_names[i]
        pivoted_df[f'Org_code{i}'] = unique_org_codes[i]
        pivoted_df[f'Rev{i}'] = unique_revs[i]

    # Reset the index
    pivoted_df.reset_index(inplace=True)
    

    return pivoted_df


panel2016 = build_panel(df2016)
panel2017 = build_panel(df2017)
panel2018 = build_panel(df2018)
panel2019 = build_panel(df2019)
panel2020 = build_panel(df2020)
panel2021 = build_panel(df2021)

final_panel = pd.concat([panel2016, panel2017, panel2018, panel2019, panel2020, panel2021], axis=0, ignore_index=True)

# ADD NEW VARIABLES TO THE PANEL AFTER BUILDING AN INITIAL FORMAT WITH ALL THE YEARS
rev_cols = []
for col in final_panel.columns:
    if col.startswith('Rev'):
        rev_cols.append(col)


final_panel['TotRev'] = 0
row_counter1 = 0 
for school in final_panel['School_Name']:
    revs = []
    for rev_col in rev_cols:
        if type(final_panel.loc[row_counter1, rev_col]) == str:
            final_panel.loc[row_counter1, rev_col] = final_panel.loc[row_counter1, rev_col].replace('$','')
            final_panel.loc[row_counter1, rev_col] = final_panel.loc[row_counter1, rev_col].replace(',','')
        
        
        revs.append(float(final_panel.loc[row_counter1, rev_col]))

    new_revs = [x if not math.isnan(x) else 0 for x in revs]
    final_panel.loc[row_counter1, 'TotRev'] = sum(new_revs)
    #print(revs, row_counter1)
    row_counter1 += 1

final_panel['ANYbig'] = 0 # indicate orgs with revenue greater than $25000
final_panel['ANYhuge'] = 0 # indicate orgs with revenue greater than $50000
final_panel['ANYenormous'] = 0 # indicate orgs with revenue greater than $100000
final_panel['ANYenormous2'] = 0 # indicate orgs with revenue greater than $200000

row_counter0 = 0
for school in final_panel['School_Name']: 
    if (final_panel.loc[row_counter0, 'TotRev'] >= 200000):
        final_panel.loc[row_counter0, 'ANYenormous2'] = 1

    elif (final_panel.loc[row_counter0, 'TotRev'] >= 100000):
        final_panel.loc[row_counter0, 'ANYenormous'] = 1

    elif (final_panel.loc[row_counter0, 'TotRev'] >= 50000):
        final_panel.loc[row_counter0, 'ANYenormous'] = 1

    elif (final_panel.loc[row_counter0, 'TotRev'] >= 25000):
        final_panel.loc[row_counter0, 'ANYbig'] = 1

    row_counter0 += 1
    
org_code_cols = []
for col in final_panel.columns:
    if col.startswith('Org_code'): 
        org_code_cols.append(col)

#variables needed: ANYalive, PTOalive, PTAalive, BOOSTalive, OTHERalive
final_panel['ANYalive'] = 0
final_panel['PTAalive'] = 0
final_panel['PTOalive'] = 0
final_panel['BOOSTalive'] = 0
final_panel['OTHERalive'] = 0
row_counter2 = 0
for school in final_panel['School_Name']: # iterate through each row
    codes = []
    for code_col in org_code_cols:
        codes.append(final_panel.loc[row_counter2, code_col])
    
    if len(codes) != 0:
        final_panel.loc[row_counter2, 'ANYalive'] = 1

    if codes.__contains__(1):
        final_panel.loc[row_counter2, 'PTAalive'] = 1

    if codes.__contains__(2):
        final_panel.loc[row_counter2, 'PTOalive'] = 1

    if codes.__contains__(3):
        final_panel.loc[row_counter2, 'BOOSTalive'] = 1
    
    if codes.__contains__(4):
        final_panel.loc[row_counter2, 'OTHERalive'] = 1

    row_counter2 += 1

new_panel = pd.concat([final_panel,df_all_unmatched],axis=0, ignore_index=True)
new_panel = new_panel.sort_values(by=['schID', 'Year'])


new_panel.to_csv("/Users/malavikakalani/Desktop/FINAL_PANEL.csv")

