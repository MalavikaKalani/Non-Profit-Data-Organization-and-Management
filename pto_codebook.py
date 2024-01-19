"""

CRF 2023 - Team PTO

This module contains functions to filter BMF and Core files from the NCCS Data Archive
and categorize data by identifying  data for PTOs, PTAs and other school-linked non profits in 
North Carolina for a given year. It also contains functions to run a matching process to match 
filtered PTOs, PTAs and other school-linked non profits to potential schools they could be serving
based on different comparing factors.

Authors: Malavika Kalani & Huey Li
Date: June 12, 2023 

"""

# python libraries required for codebook
import pandas as pd
import os
import glob
import numpy as np
from pathlib import Path
import fuzzymatcher
import recordlinkage


def filter_data(path):
    """
    Reads in and filters CSV files in the given directory.
    Categorizes data of non-profit organizations down to traditional 
    parent-teacher associations, parent teacher organizations, 
    arts- and sports supporting boosters, and other single-school 
    supporting organizations. 

    Args:
        path (str): The path to the directory containing CSV files.

    Returns:
        pandas.DataFrame: The filtered and categorized data as a DataFrame.

    Raises:
        FileNotFoundError: If the specified directory does not exist.

    """
    
     # read all csv files from the given directory 
    files_orgs = os.listdir(path)

    # create an empty dataframe to store the organization data 
    df_orgs = pd.DataFrame() 

    # read all CSV files in the given year's directory to combine all provided months 
    for file in files_orgs:
        file_path = os.path.join(path, file)  # Get the full file path
        if file.endswith(".csv"):  # Check if the file is a CSV file
            df_temp = pd.read_csv(file_path)
            df_orgs = df_orgs.append(df_temp, ignore_index=True)

    # filtering by state to only look at NC 
    df_orgs = df_orgs[df_orgs['STATE'].str.contains('NC', na = False)]

    # filtering by NTEE code to only look at education indicated by B
    df_orgs = df_orgs[df_orgs['NTEE1'].str.contains('B', na = False)]

    # removing the EINs of orgs that have already been seen once in that year 
    df_orgs = df_orgs.drop_duplicates(subset = "EIN")
    # reset indices of rows after removing duplicates 
    df_orgs = df_orgs.reset_index()


    # In the original file, 4 and 6 are recoded to boosters, 5, 8, 9, 10, 11, 12 are recoded as other

    # array to store possible explicit terms for a PTA
    pta_names = np.array(["PTA ", " PTA", "P T A", "PTA-", "PTSA", "P T S A", "PARENT TEACHER ASSOCIATION", "PARENT-TEACHER ASSOCIATION", "PARENT-TEACHER ASS", 
    "PARENT TEACHER ASS", "PARENT TEACHER STUDENT ASSOCIATION", "PARENT AND TEACHER ASSOCIATION", "PARENTS AND TEACHERS ASSOCIATION", 
    "PARENTS TEACHERS ASSOCIATION", "PARENT & TEACHER ASSOCIATION", "PARENT TEACHERS ASSOCIATION", "PARENT- TEACHERS ASSOCIATION", 
    "PARENT- TEACHER ASSOCIATION", "PARENT-TEACHERS ASSOCIATION", "PARENTS-TEACHERS ASSOCIATION", "PARENT-TEACHER- STUDENT ASSOCIATION", 
    "PARENT- TEACHER-STUDENT ASSOCIATION", "PARENT TEACHER STUDENT ASSOCIATON", "PARENTS TEACHERS AND STUDENTS ASSOC", "PARENTS TEACHERS & STUDENTS ASSO", 
    "PARENT-TEACHER-STUDENT ASSOCIATION", "PARENT-TEACHER STUDENT ASSOCIATION", "PARENT TEACHER STUDENT ASS", "PARENT TEACHERS STUDENT ASSOCIATION", 
    "PARENTS & TEACHERS ASSN", "PARENT & TEACHERS ASSOC", "PARENT AND TEACHERS ASSOCATION", "PARENT TEACHERS ASSN", "PARENT TEACHERS ASSO", "PARENTS TEACHERS ASSOC", 
    "PARENT-TEACHERS ASSN", "PARENTS TEACHER ASSOCIATION", "PARENT TEACHERS ASS", "PARENTS & TEACHERS ASSOCIATION", "PARENT TEACHER AND STUDENT ASSOC", "PARENT & TEACHER ASSOC", "PARENT TEACHER FELLOWSHIP"])

    # array to store possible explicit terms for a PTO
    pto_names = np.array(["PTO "," PTO","P T O","PTO-","-PTO","PTSO","P T S O","PTO "," PTO",
                        "P T O","-PTO", "PTSO", "SUNDANCE PARENTS ASSOCIATION", "LULING PARENTEACHER BOOSTERS", 
                        "BEVERLEY MANOR ELEMENTARY PARENT TEACHER BOOSTER CLUB"])

    # array to store possible occurences of words in any order to indicate a PTO
    pto_terms = np.array([["PARENT","TEACHER", "ORG"],["PARENTS","TEACHER", "ORG"],
                        ["PARENT","TEACHER", " ORG"],["PARENT","TEACH", " ORG"],["PARENT","TEACH"," ASS"],
                        ["PARENT","STAFF"," ORG"],["PARENT","SCHOOL"," ORG"],["PARENT","SCHOOL"," ORG"],
                        ["PARENT","FACUL"," ORG"]])

    # array to store possible explicit terms for arts and sports boosters 
    booster_names = np.array(["BOOSTER CLUB", "BOOSTERS CLUB", "BAND BOOSTER", "MUSIC BOOSTER", "BAND", "MUSIC", "MARCHING", "CHOIR", "SPIRIT", "DANCE", "FIRST FLIGHT HIGH SCHOOL FINE ARTS BOOSTERS", 
    "MINT HILL MIDDLE SCHOOL PERFORMING ARTS BOOSTER CLUB", "PANTHER CREEK HIGH SCHOOL FINE ARTS BOOSTER CLUB", "HOLLY SPRINGS HIGH SCHOOL FINE ARTS BOOSTERS INC", 
    "CRESTDALE MIDDLE SCHOOL ARTS BOOSTERS INC", "GREEN HOPE HIGH SCHOOL FINE ARTS BOOSTERS", "CARY HIGH SCHOOL PERFORMING ARTS BOOSTER CLUB", 
    "LEESVILLE ROAD HIGH SCHOOL PERFORMING ARTS BOOSTERS", "CLAYTON HIGH SCHOOL PERFORMING ARTS BOOSTER CLUB", "SOUTH CHARLOTTE MIDDLE SCHOOL FINE ARTS NETWORK", 
    "NORTHEAST MIDDLE SCHOOL PERFORMING ARTS BOOSTER CLUB", "EAST MECK ART BOOSTERS", "ATHLETIC BOOSTER", "FOOTBALL BOOSTER", " SPORTS BOOSTER", "ATHLETIC", 
    "QUARTERBACK CLUB", "SOCCER", "SWIM", "GYMNAST", "FOOTBALL", "TOUCHDOWN", "CHEER", "CREW", " SPORT", "PARENT BOOSTER", "BOOSTER ASSOCIATION", "MILE-HIGH DIVING TEAM BOOSTER CLUB", 
    "CREW BOOSTERS OF WINTER PARK INC", "CREW BOOSTERS CLUB OF CENTRAL FLORIDA INC", "LAKE ZURICH POM BOOSTER CLUB", "ICE ATHLETICS BOOSTERS CLUB INC", "LAKES CROSS-COUNTRY BOOSTER CLU", 
    "J J PEARCE PACESETTER BOOSTER CLUB", "HARMONY HIGH BALLROOM BOOSTERS INC", "YORBA LINDA MIDDLE SCHOOL INSTRUCTIONAL MUCIC BOOSTER CLUB", "BOOSTER ASSOCIATION FOR STRING STUDENTS", 
    "FORT MADISON VOCAL BOOSTERS INC", "MERIDIAN MARCHING UNIT BOOSTERS", "FRIENDS OF BEETHOVEN BOOSTER CLUB", "CASTILLERO VOCAL BOOSTER CLUB 6384 LEYLAND PARK DR"])

    # array to store possible occurences of words in any order to indicate arts and sports boosters 
    booster_terms = np.array([["SCHOOL", "BAND"], ["SCHOOL", "MUSIC"], ["SCHOOL", "CHOIR"], ["SCHOOL", "CHORAL"],
    ["SCHOOL", "THEATER"], ["SCHOOL", "THEATRE"], ["SCHOOL", "DRAMA"], ["SCHOOL", "DANCE"], ["BAND", "HIGH"], ["CHORAL", "HIGH"],
    ["DRAMA", "HIGH"], ["BAND", "HS "], ["PARENT", "GYMNAST"], ["PARENT", " SPORT"], ["PARENT", "CHEER"], ["SCHOOL", "ATHLETIC BOOSTER"],
    ["SCHOOL", "QUARTERBACK CLUB"], ["SCHOOL", "LACROSSE"], ["SCHOOL", "CHEER"], ["SCHOOL", "VOLLEYBALL"], ["SCHOOL", "ATHLETIC"], 
    ["HIGH", "ATHLETIC BOOSTER"], ["HIGH", "ATHLETIC"], ["HS ", "ATHLETIC BOOSTER"], [ "HS ", "CHEER"], ])

    # array to store possible explicit terms for other school-linked organizations 
    other_names = np.array(["NEWPORT HARBOR HIGH SCHOOL AQUATIC BOOSTER CLUB", "FOUNTAIN VALLEY HIGH SCHOOL GIRLS VALLEYBALL BOOSTERS", "LINCOLN HIGH HOMEPLATE CLUB INC HIGH SCHOOL BOOSTER ORGANIZA", 
    "KENNESAW MOUNTAIN HIGH SCHOOL LACROSS BOOSTER CLUB", "CHAMPAIGN CENTRAL HIGH SCHOOL BASEB ALL BOOSTERS", "AG BOOSTER", "AGRICULTURE BOOSTER", "SPEECH AND DEBATE BOOSTER", "TECHNOLOGY BOOSTER", 
    "INTERNATIONAL BACCALAUREATE BOOSTER", "COMMUNITY COUNCIL", "PARTNERS IN EDUCATION", "EDUCATIONAL SUPPORT", "SCHOOL SUPPORT", "SCHOOL FOUNDATION", "PARENT"])

    # array to store possible occurences of words in any order to indicate other school-linked organizations
    other_terms = np.array([["SCHOOL", "FOUNDATION"], ["FRIENDS OF", "HIGH"], ["FRIENDS OF", "HS"], ["FRIENDS OF", "ELEM"], ["FRIENDS OF", "SCHOOL"]])

    # array to store terms that need to be dropped from the dataset
    drop_names = np.array(["HOME SCHOOL", "HOME AND SCHOOL", "LESBIANS AND GAYS", "LESBIANS & GAYS"])
    
    # function to check if a given string contains all the words in a given array 
    def contain_terms(text, words):
        return all(word in text for word in words)

    # initialize all organization codes to 0 before categorizing
    df_orgs['ORG CODE'] = 0
    
    # variable to keep track of each row
    row_counter = 0 
    # iterate through each organization name in the dataframe
    for org_name in df_orgs['NAME']:
        # identify orgs that need to be immediately dropped and mark it as 0 
        if any(word in org_name for word in drop_names):
            df_orgs.loc[row_counter,'ORG CODE'] = 0
        # identifying and categorizing PTAs 
        if any(word in org_name for word in pta_names):
            df_orgs.loc[row_counter,'ORG CODE'] = 1
        # identifying and categorizing PTOs 
        elif any(word in org_name for word in pto_names):
            df_orgs.loc[row_counter,'ORG CODE'] = 2
        elif any(contain_terms(org_name,words) for words in pto_terms):
            df_orgs.loc[row_counter,'ORG CODE'] = 2
        # identifying and categorizing boosters 
        elif any(word in org_name for word in booster_names):
            df_orgs.loc[row_counter,'ORG CODE'] = 3
        elif any(contain_terms(org_name,words) for words in booster_terms):
            df_orgs.loc[row_counter,'ORG CODE'] = 3
        # identifying and categorizing other school-linked non-profits
        elif any(word in org_name for word in other_names):
            df_orgs.loc[row_counter,'ORG CODE'] = 4
        elif any(contain_terms(org_name,words) for words in other_terms):
            df_orgs.loc[row_counter,'ORG CODE'] = 4
        # if the org doesn't fall into any category, we mark it as 0
        else:
            #dataFrame = dataFrame.drop(row_counter)
            df_orgs.loc[row_counter,'ORG CODE'] = 0

        row_counter += 1 # continue iterating through each row

    # remove the organization entries that are marked NA
    df_orgs = df_orgs[df_orgs['ORG CODE'] != 0]
    # all column strings in organization files are upper case for a cohesive format
    df_orgs = df_orgs.rename(columns=lambda x: x.upper())

    return df_orgs


def format_orgs(df_allorgs):
    """
    Formats and filters the organization data.

    This function formats the organization data by creating single variables for address, ZIP code, and name,
    based on non-empty values from different sources. It also identifies entries with a PO Box condition
    and removes them from the dataset. The resulting DataFrame is returned along with a copy of the original DataFrame.

    Args:
        df_allorgs (pandas.DataFrame): The DataFrame containing organization data.

    Returns:
        tuple: A tuple containing two DataFrames:
            - The filtered and formatted DataFrame without entries having a PO Box condition.
            - A copy of the original DataFrame before filtering.

    """
    # create a single address variable and take the address from either core or BMF that is non-empty
    df_allorgs['ADDRESS_final'] = df_allorgs['ADDRESS_x'].fillna(df_allorgs['ADDRESS_y']) 

    # create a single ZIP variable and take the ZIP from either core or BMF that is non-empty
    df_allorgs['ZIP_final'] = df_allorgs['ZIP5_x'].fillna(df_allorgs['ZIP5_y']) 

    # create a single name variable take the name from either core or BMF that is non-empty
    df_allorgs['NAME_final'] = df_allorgs['NAME_x'].fillna(df_allorgs['NAME_y']) 

    # create a boolean variable to indicate whether or not an address is incomplete with just po box information
    df_allorgs['PO BOX'] = df_allorgs['ADDRESS_final'].str.startswith("PO BOX") 
    
    # only keep the columns required in the merged dataframe and remove all other columns 
    df_allorgs = df_allorgs[['EIN', 'SEC_NAME_x', 'NAME_x', 'ADDRESS_x', 'NTEEFINAL_x','ZIP5_x','ORG CODE_x', 'NAME_y', 'ADDRESS_y', 'ZIP5_y', 'NAME_final', 'ADDRESS_final', 'PO BOX', 'ZIP_final', 'ORG CODE_y','TOTREV', 'TOTREV2', '_merge']]

    # make organization name and street lowercase for better comparison
    df_allorgs['NAME_final'] = df_allorgs['NAME_final'].str.lower()
    df_allorgs['ADDRESS_final'] = df_allorgs['ADDRESS_final'].str.lower()
    
    # make a copy of original df_allorgs dataframe to avoid losing data
    df_allorgs_copy = df_allorgs.copy() 
    # keep the entries that do not have a po box condition
    df_allorgs_withpo = df_allorgs[df_allorgs['PO BOX'] == True]  
    df_allorgs = df_allorgs[df_allorgs['PO BOX'] == False]  # without PO box condition
    
    
    return df_allorgs, df_allorgs_copy, df_allorgs_withpo


def format_schools(path):
    """
    Formats the school data.

    This function reads the school data from a CSV file located at the specified path and formats it.
    The school names and street locations are converted to lowercase for better comparison.

    Args:
        path (str): The file path to the CSV file containing the school data.

    Returns:
        pandas.DataFrame: The formatted school data as a DataFrame.

    """
    
    # create dataframe to store school data 
    df_schools = pd.DataFrame()   
    # read in school data from given file path
    df_schools = pd.read_csv(path) 

    # make school name and street lowercase for better comparison
    df_schools['school_name'] = df_schools['school_name'].str.lower()
    df_schools['street_location'] = df_schools['street_location'].str.lower()
    df_schools['school_level'] = df_schools['school_level'].str.lower()
    return df_schools 


def matching_process(df_allorgs, df_schools, df_allorgs_copy, df_allorgs_withpo):
    """
    Performs the matching process between organization and school data.

    This function uses record linkage techniques to perform a two-round matching process between
    the organization data and school data. In the first round, potential matches are identified based
    on exact ZIP, name, and address comparisons. Stronger matches with an exact ZIP are kept. In the
    second round, potential matches are identified based on name comparison only.

    Args:
        df_allorgs (pandas.DataFrame): The organization data DataFrame.
        df_schools (pandas.DataFrame): The school data DataFrame.
        df_allorgs_copy (pandas.DataFrame): A copy of the organization data DataFrame.

    Returns:
        tuple: A tuple containing the potential matches from round 1 and round 2 as DataFrames.
            The first element is the potential matches from round 1, and the second element is
            the potential matches from round 2.

    """
    
    # create an indexer object
    indexer = recordlinkage.Index() 

    # use full indexer to evaluate all potential pairs 
    indexer.full()

    # for round 1, build up all potential pairings to check
    matches_round1 = indexer.index(df_allorgs, df_schools) 

    # now we define how we want to perform their comparison logic 
    compare1 = recordlinkage.Compare() # create a compare object 

    # ROUND 1 OF MATCHING BASED ON EXACT ZIP, NAME AND ADDRESS
    # compare organization name and school name 
    compare1.string('NAME_final', 'school_name', method='jarowinkler', label='Name_Score') 
    # compare organization address and school addres
    compare1.string('ADDRESS_final', 'street_location', method='jarowinkler', label='Address_Score')
    # compare ZIP codes where 0 indicates differet ZIP and 1 indicates exact ZIP
    compare1.exact('ZIP_final', 'zip_location', label='Zip_Score')

    # compute the specified features for each pair 
    features1 = compare1.compute(matches_round1, df_allorgs, df_schools)
    # select the potential matches where the sum of the comparison scores across all features is greater than zero. 
    potential_matches1 = features1[features1.sum(axis=1) > 0].reset_index()

    # create a total score variable which is the sum of all computed similarity scores
    potential_matches1['Total_Score'] = potential_matches1.loc[:, 'Name_Score':'Zip_Score'].sum(axis=1)
    
    # keep stronger matches with an exact zip 
    potential_matches1 = potential_matches1[potential_matches1['Zip_Score'] == 1]

    # for each organization, only keep its match that has the highest score 
    max_score = potential_matches1.groupby('level_0')['Total_Score'].idxmax()
    potential_matches1 = potential_matches1.loc[max_score]

    potential_matches1['EIN'] = df_allorgs.loc[potential_matches1['level_0'], 'EIN'].values
    # create a column to assign the organization names based on the matched indices in the 'level_0' column
    potential_matches1['Organization_Name'] = df_allorgs.loc[potential_matches1['level_0'], 'NAME_final'].values
    potential_matches1['Org_code'] = df_allorgs.loc[potential_matches1['level_0'], 'ORG CODE_x'].values
    # create a column to assign the school names based on the matched indices in the 'level_1' column
    potential_matches1['School_Name'] = df_schools.loc[potential_matches1['level_1'], 'school_name'].values
    potential_matches1['schID'] = df_schools.loc[potential_matches1['level_1'], 'school_id'].values 
    potential_matches1['leaID'] = df_schools.loc[potential_matches1['level_1'], 'leaid'].values
    # create a column to assign the organization address based on the matched indices in the 'level_0' column
    potential_matches1['Organization_Street'] = df_allorgs.loc[potential_matches1['level_0'], 'ADDRESS_final'].values
    # create a column to assign the school address based on the matched indices in the 'level_1' column
    potential_matches1['School_Street'] = df_schools.loc[potential_matches1['level_1'], 'street_location'].values
    potential_matches1['School_level'] = df_schools.loc[potential_matches1['level_1'], 'school_level'].values
    potential_matches1['Revenue'] = df_allorgs.loc[potential_matches1['level_0'], 'TOTREV'].values
    # create a column to keep track of the parameters the match was based on
    potential_matches1['Match_Parameter'] = 'name,address' 

    # get indices of orgs that were already matched in round 1 
    matched_index = potential_matches1['level_0'].values 
    
    # create new dataframe and remove the ones that already matched 
    # this dataframe will now include the ones that were removed because of po box condition 
    # and ones that weren't matched 
    df_nonmatch = df_allorgs.drop(matched_index) 
    df_nonmatch_pobox = pd.concat([df_nonmatch, df_allorgs_withpo], axis=0, ignore_index=True)
    #potential_matches1.to_csv("/Users/malavikakalani/Desktop/2021_round1.csv")
    #df_nonmatch_pobox.to_csv("/Users/malavikakalani/Desktop/check2021.csv")
    
    # ROUND 2 OF MATCHING BASED ON ONLY NAME 
    # same process as round 1

    # use the new organization dataframe that includes organizations left to be matched
    matches_round2 = indexer.index(df_nonmatch_pobox, df_schools)
    compare2 = recordlinkage.Compare()
    compare2.string('NAME_final', 'school_name',method='jarowinkler', label='Name_Score')
    features2 = compare2.compute(matches_round2, df_nonmatch_pobox, df_schools)
    potential_matches2 = features2[features2.sum(axis=1) > 0].reset_index()
    # set address and zip score to 0 since we are not considering that in this round 
    potential_matches2['Address_Score'] = 0.0
    potential_matches2['Zip_Score'] = 0.0
    potential_matches2['Total_Score'] = potential_matches2.loc[:, 'Name_Score':'Zip_Score'].sum(axis=1)
    # for each organization, only keep its match that has the highest score 
    max_name_score = potential_matches2.groupby('level_0')['Total_Score'].idxmax()
    potential_matches2 = potential_matches2.loc[max_name_score]
    potential_matches2['EIN'] = df_nonmatch_pobox.loc[potential_matches2['level_0'], 'EIN'].values
    potential_matches2['Organization_Name'] = df_nonmatch_pobox.loc[potential_matches2['level_0'], 'NAME_final'].values
    potential_matches2['Org_code'] = df_nonmatch_pobox.loc[potential_matches2['level_0'], 'ORG CODE_x'].values
    potential_matches2['School_Name'] = df_schools.loc[potential_matches2['level_1'], 'school_name'].values
    potential_matches2['schID'] = df_schools.loc[potential_matches2['level_1'], 'school_id'].values 
    potential_matches2['leaID'] = df_schools.loc[potential_matches2['level_1'], 'leaid'].values
    potential_matches2['Organization_Street'] = df_nonmatch_pobox.loc[potential_matches2['level_0'], 'ADDRESS_final'].values
    potential_matches2['School_Street'] = df_schools.loc[potential_matches2['level_1'], 'street_location'].values
    potential_matches2['School_level'] = df_schools.loc[potential_matches2['level_1'], 'school_level'].values
    potential_matches2['Revenue'] = df_nonmatch_pobox.loc[potential_matches2['level_0'], 'TOTREV'].values

    
    # to keep track of the parameters the match was based on
    potential_matches2['Match_Parameter'] = 'name' 
  

    return potential_matches1, potential_matches2


#STEP 1: filter BMF data file using the function created 
# TODO: give the correct BMF file path according to file location on your computer 
df_bmf = filter_data("/Users/malavikakalani/Desktop/CRF 2023/BMF data files/2019")

# STEP 1A: if an organization in the BMF data has a secondary name, then use that value instead of primary name 
df_bmf['NAME'] = df_bmf['SEC_NAME'].fillna(df_bmf['NAME'])

# STEP 2: filter core data file using the function created 
# TODO: give the correct core file path according to file location on your computer
df_core = filter_data("/Users/malavikakalani/Desktop/CRF 2023/Core files/2019")

# STEP 3: combine the dataframes of BMF and Core Files to get all organizations
df_allorgs = pd.merge(df_bmf,df_core, on = 'EIN', how = 'outer', indicator = True) # use EIN as merging factor because some organizations are in both files

#df_final_orgs = pd.read_csv("/Users/malavikakalani/Desktop/CRF 2023/allorgs2021.csv")
#df_final_orgs_copy = pd.read_csv("/Users/malavikakalani/Desktop/CRF 2023/allorgs2021_copy.csv")
#df_allorgs_withpo = pd.read_csv("/Users/malavikakalani/Desktop/po_2021.csv")

# STEP 4: formatting all orgs file to only keep the columns we need and creating a copy for easier access 
df_final_orgs, df_final_orgs_copy, df_allorgs_withpo = format_orgs(df_allorgs) # final file that we will be using for matching process 

# STEP 5: formatting schools file and creating the school dataframe 
# TODO: give the correct school file path according to file location on your computer 
df_schools = format_schools("/Users/malavikakalani/Desktop/CRF 2023/schools_2021.csv")

# STEP 6: perform the matching process and retrieve the matches from round 1 and round 2 
matches_round1, matches_round2 = matching_process(df_final_orgs, df_schools, df_final_orgs_copy, df_allorgs_withpo)

# STEP 7: merge the two rounds of matches 
final_matches = pd.merge(matches_round1, matches_round2, how = "outer")

# STEP 8: additional conditions to filter out bad matches 
# keep the rows that do not satisfy the given conditions
# bad match if both name and address score are less than 0.7 
final_matches = final_matches[
    ~(
        (final_matches['Match_Parameter'] == "name,address") &
        (final_matches['Name_Score'] < 0.7) &
        (final_matches['Address_Score'] < 0.7)
    )
]
# bad match is name score is less than 0.7 
final_matches = final_matches[
    ~(
        (final_matches['Match_Parameter'] == "name") &
        (final_matches['Name_Score'] < 0.7)
    )
]

# STEP 9: convert your match results to a CSV file for further use 
# TODO: give the correct file path for where you want to save the matches on your computer
final_matches.to_csv("/Users/malavikakalani/Desktop/finalmatches2021.csv")





     













