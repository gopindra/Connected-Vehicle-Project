'''
Code to extract and aggregate select columns from American Community Survey Summary FTP download.
Code written specifically for 2018 Block Group Level ACS Dataset.
'''

import os
import pandas as pd
import numpy as np

# Set the folder where the ACS summary data and template files are stored
ACS_FOLDER = r"D:\Wejo Project\Data\GIS\ACS TX 2018"

def generate_ACS2018_description(acs_folder):
    ''' Compiles variable names from all the files in the template folder.

    Parameters
    ----------
    acs_folder : str
        Path of folder containing ACS data template files. The folder 
        "2018_5yr_Summary_FileTemplates" is expected to be inside this folder.

    Returns
    -------
    variable_descriptions : pd.DataFrame
        DataFrame with two columns.
        index - Variable name
        Column 1 - file_index - Index of file containing the variable
        Column 2 - description - Description of the variable

    '''
    
    template_folder = os.path.join(acs_folder, "2018_5yr_Summary_FileTemplates")
    template_files = os.listdir(template_folder)
    template_files = [ (int(file.split(".")[0][3:]), file) for file in template_files if file[0:3] == "seq"]
    template_files.sort()
    
    all_templates = list()
    for index, file in template_files:
        template = pd.read_excel(os.path.join(template_folder, file))
        template = template.drop(["FILEID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO"], axis=1)
        all_templates.append(pd.DataFrame({"file_index": index, "description": template.iloc[0,]}, index=template.columns))
    
    variable_descriptions = pd.concat(all_templates)
    variable_descriptions.index.name = 'variable'
    return variable_descriptions

# Compiling the ACS file templates into single DataFrame and saving it.
variable_descriptions = generate_ACS2018_description(ACS_FOLDER)
variable_descriptions.to_csv(os.path.join(ACS_FOLDER, "ACS_variable_descriptions.csv"))

''' 
compilation tells how the columns in the ACS files are to be aggregated.
It is a list of tuples.
First element of the tuple says what the name of the new column should be.
Second element is the list of ACS columns to be added to form the new column.
Eg. ("age_25_34", ["B01001_011", "B01001_012", "B01001_035", "B01001_036"])
says to form a new column age_25_34 as 
B01001_011 + B01001_012 + B01001_035 + B01001_036
'''
compilation = [ ("population"         , ["B01001_001"]),
               
                ("age_lte_4"          , ["B01001_003", "B01001_027"]),
                ("age_5_18"           , ["B01001_004", "B01001_005", "B01001_006", "B01001_028", "B01001_029", "B01001_030"]),
                ("age_19_24"          , ["B01001_007", "B01001_008", "B01001_009","B01001_010",
                                         "B01001_031", "B01001_032", "B01001_033","B01001_034"]),
                ("age_25_34"          , ["B01001_011", "B01001_012", "B01001_035", "B01001_036"]),
                ("age_35_44"          , ["B01001_013", "B01001_014", "B01001_037", "B01001_038"]),
                ("age_45-54"          , ["B01001_015","B01001_016", "B01001_039","B01001_040"]),
                ("age_55_64"          , ["B01001_017", "B01001_018", "B01001_019",
                                         "B01001_041", "B01001_042", "B01001_043"]),
                ("age_65_74"          , ["B01001_020", "B01001_021", "B01001_022",
                                         "B01001_044", "B01001_045", "B01001_046"]),
                ("age_75_84"          , ["B01001_023", "B01001_024", "B01001_047", "B01001_048"]),
                ("age_gte_85"         , ["B01001_025", "B01001_049"]),

                ("households"         , ["B25009_001"]),    # Includes only occupied houseing units       
                
                ("hhs1"               , ["B25009_003", "B25009_011"]),
                ("hhs2"               , ["B25009_004", "B25009_012"]),
                ("hhs3"               , ["B25009_005", "B25009_013"]),
                ("hhs4"               , ["B25009_006", "B25009_014"]),
                ("hhs5"               , ["B25009_007", "B25009_015"]),
                ("hhs6"               , ["B25009_008", "B25009_016"]),
                ("hhs7p"              , ["B25009_009", "B25009_017"]),
                         
                ("hhvcl0"             , ["B25044_003", "B25044_010"]), # B08201_002
                ("hhvcl1"             , ["B25044_004", "B25044_011"]),
                ("hhvcl2"             , ["B25044_005", "B25044_012"]),
                ("hhvcl3"             , ["B25044_006", "B25044_013"]),
                ("hhvcl4"             , ["B25044_007", "B25044_014"]),
                ("hhvcl5p"            , ["B25044_008", "B25044_015"]),
                 
                ("hhretire"           , ['B19059_002']),
                ("hhnoretire"         , ['B19059_003']),
                
                ("hhinc_lt_25"        , ["B19001_002", "B19001_003", "B19001_004", "B19001_005"]),
                ("hhinc_25_50"        , ["B19001_006", "B19001_007", "B19001_008", "B19001_009", "B19001_010"]),
                ("hhinc_50_75"        , ["B19001_011", "B19001_012"]),
                ("hhinc_75_100"       , ["B19001_013"]),
                ("hhinc_100_125"      , ["B19001_014"]),
                ("hhinc_125_150"      , ["B19001_015"]),
                ("hhinc_gte_150"      , ["B19001_016", "B19001_017"]),

                # Worker variables are not available at block group level
                # They are available only in the county-census tract level
                
                ("workers"            , ["B08124_001"]),
                
                ("wkmanager"          , ["B08124_044"]),
                ("wkservice"          , ["B08124_045"]),
                ("wkoffice"           , ["B08124_046"]),
                ("wkconstruct"        , ["B08124_047"]),
                ("wktransport"        , ["B08124_048"]),
                ("wkmilitary"         , ["B08124_049"]),
                
                ("wk2agro"            , ["B08126_002"]),
                ("wk2construct"       , ["B08126_003"]),
                ("wk2manufacture"     , ["B08126_004"]),
                ("wk2wholesale"       , ["B08126_005"]),
                ("wk2retail"          , ["B08126_006"]),
                ("wk2transpo"         , ["B08126_007"]),
                ("wk2info"            , ["B08126_008"]),
                ("wk2finance"         , ["B08126_009"]),
                ("wk2professional"    , ["B08126_010"]),
                ("wk2edu"             , ["B08126_011"]),
                ("wk2recreation"      , ["B08126_012"]),
                ("wk2other"           , ["B08126_013"]),
                ("wk2public"          , ["B08126_014"]),
                ("wk2armed"           , ["B08126_015"]),
                
                # Employment data is not available in the dataset
                # (all blanks). So commenting out.
                
                # ("employment"         , ["B08524_001"]),
                
                # ("empmanager"         , ["B08524_002"]),
                # ("empservice"         , ["B08524_003"]),
                # ("empoffice"          , ["B08524_004"]),
                # ("empconstruct"       , ["B08524_005"]),
                # ("emptransport"       , ["B08524_006"]),
                # ("empmilitary"        , ["B08524_007"]),
                
                # ("emp2agro"            , ["B08526_002"]),
                # ("emp2construct"       , ["B08526_003"]),
                # ("emp2manufacture"     , ["B08526_004"]),
                # ("emp2wholesale"       , ["B08526_005"]),
                # ("emp2retail"          , ["B08526_006"]),
                # ("emp2transpo"         , ["B08526_007"]),
                # ("emp2info"            , ["B08526_008"]),
                # ("emp2finance"         , ["B08526_009"]),
                # ("emp2professional"    , ["B08526_010"]),
                # ("emp2edu"             , ["B08526_011"]),
                # ("emp2recreation"      , ["B08526_012"]),
                # ("emp2other"           , ["B08526_013"]),
                # ("emp2public"          , ["B08526_014"]),
                # ("emp2armed"           , ["B08526_015"]),
                
                ("vehicles"           , ["B25046_001"]),
                
                ("median_income"      , ["B18140_001"]),
                
                ("median_age"         , ["B01002_001"])
                
                ]

def compile_ACS2018_dat(compilation, acs_folder):
    ''' Generate a new dataframe from the ACS dataset based on compilation.

    Parameters
    ----------
    compilation : list of tuples
        Describes how the columns in ACS are to be aggregated.
    acs_folder : str
        Path of folder containing ACS data template files. The folder 
        "2018_5yr_Summary_FileTemplates" and the folder 
        "Texas_Tracts_Block_Groups_Only" are expected to be inside this folder.

    Raises
    ------
    Exception
        New columns may be formed only by aggregating ACS columns in the same
        ACS file. If the columns that need to be added come from different 
        files, an exception will be raised.

    Returns
    -------
    acs_compile_dat : pd.DataFrame
        A dataframe with columns generated as per `compilation`.

    '''
    
    variable_descriptions = generate_ACS2018_description(acs_folder)
    template_folder = os.path.join(acs_folder, "2018_5yr_Summary_FileTemplates")
    file_index = 1
    template = pd.read_excel(os.path.join(template_folder, "seq%d.xlsx" % file_index))
    acs_data = pd.read_csv(os.path.join(acs_folder, "Texas_Tracts_Block_Groups_Only", "e20185tx%04d000.txt" % file_index), names=template.columns)
    acs_compile_dat = pd.DataFrame(acs_data["LOGRECNO"])

    for my_var, acs_vars in compilation:
        
        file_index = variable_descriptions.loc[acs_vars, "file_index"]
        if sum(~file_index.duplicated()) != 1:
            raise Exception("Current code does not allow calculating a variable by summing ACS variables from multiple files")
        else:
            file_index = int(file_index[0])
            
        template = pd.read_excel(os.path.join(template_folder, "seq%d.xlsx" % file_index))
        acs_data = pd.read_csv(os.path.join(acs_folder, "Texas_Tracts_Block_Groups_Only", "e20185tx%04d000.txt" % file_index), names=template.columns)
        acs_data[my_var] = acs_data[acs_vars].sum(axis=1, skipna=False)
        acs_compile_dat = acs_compile_dat.merge(acs_data[["LOGRECNO", my_var]], on = "LOGRECNO", validate="1:1")
        
    return(acs_compile_dat)

# Extracted columns from ACS dataset.
acs_compile_dat = compile_ACS2018_dat(compilation, ACS_FOLDER)

def fix_nonnumeric_cols(acs_compile_dat):
    ''' Fixes non-numeric entries in some of the columns.
    
    The 'vehicles', 'median_income', and 'median_age' does not have the
    expected float64 format. This is because of the presence of '.' as some of
    the column entries - typically occurs when there are no households in that
    block group. This function converts those entries into 0 for vehickles and
    np.nan for the other columns. Then all these columns are cast into the 
    np.float64 datatype. This operation is NOT inplace.

    Parameters
    ----------
    acs_compile_dat : pd.DataFrame
        The compiled ACS dataset.

    Returns
    -------
    acs_compile_dat : pd.DataFrame
        Copy of the original dataset with the 'vehicles', 'median_income', and 
        'median_age' modified to be of type float.

    '''
    for var, dtype in acs_compile_dat.dtypes.items():
        if dtype.kind not in 'biufc': print(var, 'is not numeric.')
    
    acs_compile_dat = acs_compile_dat.copy()
    print('Fixing dtype of vehicles.')
    acs_compile_dat['vehicles'] = acs_compile_dat['vehicles'].str.replace("^.$", "0").astype(np.float64)
    print('Fixing dtype of median_income')
    acs_compile_dat['median_income'] = acs_compile_dat['median_income'].str.replace("^.$", "nan").astype(np.float64)
    print('Fixing dtype of median_age.')
    acs_compile_dat['median_age'] = acs_compile_dat['median_age'].str.replace("^.$", "nan").astype(np.float64)
    
    return acs_compile_dat

# Fixing the 'vehicles', 'median_income', and 'median_age' columns to be float64.
acs_compile_dat = fix_nonnumeric_cols(acs_compile_dat)

def generate_bg_ct_relation(acs_folder):
    ''' Generate table for mapping between block groups and county-tract.
    
    The key values (LOGRECNO) used in the ACS dataset are used as identifiers
    for the blobk groups and county-census tracts.
    
    Parameters
    ----------
    acs_folder : str
        Path of folder containing ACS data template files. The folder 
        "Texas_Tracts_Block_Groups_Only" is expected to be inside this folder.

    Returns
    -------
    matched_logrecnos: pd.DataFrame
        A dataframe where each row represents a block group and has columns,
        LOGRECNO - key value for block group in the ACS dataset.
        COUNTY - ID of county where the block group lies.
        TRACT - ID of census tract where the block group lies.
        CTLOGRECNO - key value for the county-tract where the block group lies
            in the ACS dataset.

    '''
    headers = pd.read_excel(os.path.join(acs_folder, "2018_5yr_Summary_FileTemplates", "2018_SFGeoFileTemplate.xlsx"))
    row_dat = pd.read_csv(os.path.join(acs_folder, "Texas_Tracts_Block_Groups_Only", "g20185tx.csv"), names=headers.columns, encoding='latin-1')
    bg_rows = row_dat.loc[row_dat['SUMLEVEL'] == 150, ['LOGRECNO', 'COUNTY', 'TRACT', 'BLKGRP']]
    ct_rows = row_dat.loc[row_dat['SUMLEVEL'] == 140, ['LOGRECNO', 'COUNTY', 'TRACT']]
    ct_rows = ct_rows.rename({'LOGRECNO': 'CTLOGRECNO'}, axis=1)
    matched_logrecnos = bg_rows.merge(ct_rows, on=['COUNTY', 'TRACT'])
    return(matched_logrecnos)

def split_bg_ct_dat(acs_compile_dat, acs_folder):
    ''' Splits compiled ACS dataset into block-group and county-tract datasets.
    
    The county and tract IDs are also added as columns to the block-group
    dataframe.
    
    Parameters
    ----------
    acs_compile_dat : pd.DataFrame
        The compiled ACS dataset.
    acs_folder : str
        Path of folder containing ACS data template files. The folder 
        "Texas_Tracts_Block_Groups_Only" is expected to be inside this folder.

    Returns
    -------
    acs_bg_dat : pd.DataFrame
        Subset of `acs_compile_dat` with block group data.
    acs_ct_dat : pd.DataFrame
        Subset of `acs_compile_dat` with county-tract data.

    '''
    bg_ct_match_dat = generate_bg_ct_relation(acs_folder)
    acs_bg_dat = acs_compile_dat.loc[acs_compile_dat['LOGRECNO'].isin(bg_ct_match_dat['LOGRECNO']),:]
    acs_ct_dat = acs_compile_dat.loc[acs_compile_dat['LOGRECNO'].isin(bg_ct_match_dat['CTLOGRECNO']),:]
    acs_bg_dat = acs_bg_dat.merge(bg_ct_match_dat, on=['LOGRECNO'])
    return acs_bg_dat, acs_ct_dat

acs_bg_dat, acs_ct_dat = split_bg_ct_dat(acs_compile_dat, ACS_FOLDER)

def disintegrate_ct_to_bg(acs_ct_dat, acs_bg_dat, disintegrate_columns, based_on):
    '''Splits the county-tract data into overlapping block-groups.
    
    If `based_on` is None, the value of the county-tract is assigned to all
    overlapping block-groups.
    If `based_on` is a column name, the value in the county-tract is
    apportioned into the component block-groups based on the value of 
    `based_on` in the component areas. For example. if a county-tract is made of
    two block groups with populations 70 and 30 and if the `based_on` column is
    population. If the county-tract has a value of 50 for a column, 
    the two block groups are assigned values of 70/(30+70)*50 = 35 and 
    30/(30+70)*50 = 15 respectively.
    Operation is NOT inplace.

    Parameters
    ----------
    acs_ct_dat : pd.DataFrame
        Subset of `acs_compile_dat` with block group data.
    acs_bg_dat : pd.DataFrame
        Subset of `acs_compile_dat` with county-tract data.
    disintegrate_columns : list of str
        The names of the columns in `acs_ct_dat` that has to be split into
        `acs_bg_dat`.
    based_on : str or None
        If set to a column name, the columns in `disintegrate_columns` are
        split proportional to the `based_on` column of the component block 
        groups.
        Use this if `disintegrate_columns` has counts, size, areas etc.
        If set to None, the values of the county-tract for columns in 
        `disintegrate_columns` are copied into the columns for block-groups.
        Use this if `disintegrate_columns` has columns for median, density etc.

    Returns
    -------
    acs_bg_dat : pd.DataFrame
        Copy of `acs_bg_dat` with `disintegrate_columns` altered based on
        `acs_ct_dat`.
    '''
    
    acs_ct_dat = acs_ct_dat.set_index('LOGRECNO')
    acs_bg_dat = acs_bg_dat.copy()
    
    if based_on is not None: # Split the census tract value between block groups based on 'based_on'
        if abs((acs_bg_dat[based_on].sum() - acs_ct_dat[based_on].sum()) / acs_ct_dat[based_on].sum()) > .001:
            raise Exception(f'{based_on} values not consistent across census tracts and block groups.\nTotal {based_on} across block groups = {acs_bg_dat[based_on].sum()} \nTotal {based_on} across census tracts = {acs_ct_dat[based_on].sum()}')
        
        with np.errstate(divide='ignore', invalid='ignore'): # Supress zero-division warning
            fraction = np.array(acs_bg_dat[based_on]) / np.array(acs_ct_dat.loc[acs_bg_dat['CTLOGRECNO'], based_on])
        fraction[acs_ct_dat.loc[acs_bg_dat['CTLOGRECNO'], based_on]==0] = 0
        fraction = fraction.reshape((-1,1))
        acs_bg_dat[disintegrate_columns] = (acs_ct_dat.loc[acs_bg_dat['CTLOGRECNO'], disintegrate_columns]*fraction).values
    else: # based_on is None - Apply the census tract value to all component block groups
        acs_bg_dat[disintegrate_columns] = acs_ct_dat.loc[acs_bg_dat['CTLOGRECNO'], disintegrate_columns].values
    return acs_bg_dat

# Splitting worker variables from county-tracts into block groups
acs_bg_dat = disintegrate_ct_to_bg(acs_ct_dat, acs_bg_dat, 
                                   disintegrate_columns=acs_compile_dat.filter(regex="^worker|^wk").columns, 
                                   based_on='population')
# Copying median_income of county-tracts into block groups.
acs_bg_dat = disintegrate_ct_to_bg(acs_ct_dat, acs_bg_dat, 
                                   disintegrate_columns=['median_income'], 
                                   based_on=None)

def impute_num_vehicles(acs_bg_dat):
    '''Imputing missing values in `vehicles` column.
    
    There are some np.nan in the `vehicles` column. But there are no missing
    values in the columns for household vehicle ownership - hhvcl0, hhvcl1,
    hhvcl2, hhvcl3, hhvcl4 and hhvcl5p. The number of these households can be
    multiplied with the number of vehicles in these households to get the total 
    number of vehicles for each block-group.
    
    The number of vehicles owned by each hhvcl5p household may be different. 
    We only know that it is >= 5. Therefore, we first compute the average 
    number of vehicles owned by hhvcl5p households. This is then used to
    compute the total number of vehicles in block-groups.
    
    Operation is NOT inplace.

    Parameters
    ----------
    acs_bg_dat : pd.DataFrame
        Block group data.

    Returns
    -------
    acs_bg_dat : pd.DataFrame
        Copy of the input `acs_bg_dat` with no missing values in `vehicles`.

    '''
    acs_bg_dat = acs_bg_dat.copy()
    avg_gt5_nvcl = ((acs_bg_dat['vehicles'] - (acs_bg_dat['hhvcl1'] + acs_bg_dat['hhvcl2']*2 + acs_bg_dat['hhvcl3']*3 + acs_bg_dat['hhvcl4']*4)) / acs_bg_dat['hhvcl5p']).mean()
    print(f'Average number of vehicles owened by 5 vehicle households = {avg_gt5_nvcl}')
    my_vehicles = acs_bg_dat['hhvcl1'] + acs_bg_dat['hhvcl2']*2 + acs_bg_dat['hhvcl3']*3 + acs_bg_dat['hhvcl4']*4 + acs_bg_dat['hhvcl5p']*avg_gt5_nvcl
    acs_bg_dat.loc[acs_bg_dat['vehicles'].isna(), 'vehicles'] = my_vehicles[acs_bg_dat['vehicles'].isna()]
    return(acs_bg_dat)

# Imputing missing values in vehicles column
acs_bg_dat = impute_num_vehicles(acs_bg_dat)

# Saving extracted dataset.
acs_bg_dat.to_csv(os.path.join(ACS_FOLDER, 'ACS_bg_extract.csv'), index=False)
