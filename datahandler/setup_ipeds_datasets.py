# -*- coding: utf-8 -*-
"""
Go to the bottom of the script to specify the data folder location
Use get_data() method to get the data

2021-07-10
Revision by TakaDoi for the new project UNIPEDS 

@author: takahirodoi
Created on Sun Jul 26 11:16:26 2015
"""

import pandas as pd
import numpy as np

class SetupIPEDSData:
    '''
    Read, filter, merge two IPEDS datasets:     
    1) IPEDS Analytics Delta Cost Analytics Data 1987-201
    2) Distance data
    
    Also, detect nan in the retentionrate entry and add that info to
    the main dataframe
    
    2016-02-21
    merging distance dataset to main dataset is halted for now     
    instead, reading geolocation data and merge it to main dataset 
    
    '''
    def __init__(self):
        '''
        Create data frames for main dataset, distance dataset, Geolocation dataset
        '''
        self.df_main = pd.DataFrame()
        self.df_dist = pd.DataFrame()
        self.df_geol = pd.DataFrame()
       
   # ***************** main dataset  *****************  
    def read_maindata(self):
        self.df_main = pd.read_csv(
             self.__filein_main,
             sep=',',
             # read the specified variables as object
             dtype={'academicyear':'object',
                     'groupid':'object',
                     'unitid':'object',
                     'sector':'object',
                     'sector_revised':'object'
                    },

             # 975 columns exist... So preselect some
             # ft: full time
             # pt: part time  
             usecols=[
                "groupid",
                "unitid",
                "academicyear",
                "acadsupp01",
                "liabilities07",
                "assets11",
                "ftretention_rate",
                "ptretention_rate",
                "fall_cohort_num",
                "fall_cohort_pct",
                "fall_cohort_num_outofstate",
                "fall_cohort_pct_outofstate",
                "fall_cohort_num_resunknown",
                "fall_cohort_pct_resunknown",
                "fall_total_undergrad",
                "total_enrollment_amin_tot",
                "total_enrollment_asian_tot",
                "total_enrollment_black_tot",
                "total_enrollment_hisp_tot",
                "total_enrollment_white_tot",
                "total_enrollment_multi_tot",
                "total_enrollment_unkn_tot",
                "total_enrollment_nonres_tot",
                "any_aid_num",
                "any_aid_pct",
                "tuitionfee02_tf",
                "eandg01_sum",
                "grad_rate_150_p",
                "all_employees",
                "ft_faculty_salary",
                "full_time_employee_100fte",
                "sector",
                "sector_revised",
                "zip",
                "instname",
                "grscohort",
                "fall_cohort_num_indistrict",
                "fall_cohort_pct_indistrict",
                "fall_cohort_num_instate",
                "fall_cohort_pct_instate",
                "year_cohort_num",
                "applcn",
                "applcnm",
                "applcnw",
                "admssn",
                "ft_faculty_per_100fte",
                "total_faculty_all",
                "total01"
             ]
        )

    def filter_maindata(self):
        '''
        Filter the main data based on 
        1) year 2012
        2) sectors: 1: 'Public 4-year or above'
                    2: 'Private nonprofit 4-year or above',
                    3: 'Private for-profit 4-year or above'
           (we use sector_revised, where 4-year univs that don't grant diplomas are removed)
        '''
        tmp = self.df_main
        # tmp = tmp[tmp['academicyear']=='2012']
        tmp = tmp[tmp['sector_revised'].isin(['1', '2', '3'])] # note that sector is read as object
        self.df_main = tmp

    def detect_nan_retrate(self):
        '''
        detect nan elements in ftretention_rate column
        and put it into newly created 'not_missing' column
        '''
        self.df_main['missing_ret'] = np.isnan(self.df_main.ftretention_rate)

    def get_data(self):
        self.read_maindata()
        self.filter_maindata()
        self.detect_nan_retrate()
        # self.read_geoldata()
        # self.merge_geoldata()
        return self.df_main

    def print_data_path(self):
        print('loading:')
        print(self.__filein_main)
        
   # ***************** distance education dataset  *****************   
    def read_distdata(self):
        self.df_dist = pd.read_csv(self.__filein_dist)
        
    def rename_distdata(self):
        '''
        Later we want to merge the main dataframe and distance education dataframe, with 
        the common column 'unitid' as the key.
        '''
        self.df_dist.rename(columns={'UNITID': 'unitid'}, inplace=True)

    def filter_distdata(self):
        '''
        focus on degre seekng studens
        '''
        tmp = self.df_dist
        tmp = tmp[tmp['EFDELEV']==3]
        self.df_dist = tmp

    def get_distdata(self):
        self.read_distdata()
        self.rename_distdata()
        self.filter_distdata()
        return self.df_dist
 
   # ***************** geo location dataset *****************   
    def read_geoldata(self):
        self.df_geol = pd.read_csv(self.__filein_geol, dtype = {'unitid':'str'})
    
    def merge_geoldata(self):        
        self.df_geol = self.df_geol[list(['unitid','lat','long'])]         
        self.df_main = pd.merge(self.df_main, self.df_geol, left_on='unitid', right_on='unitid')

    def get_geoldata(self):
        self.read_geoldata()
        return self.df_geol

  # ***************** below is for defining (private) variables  *****************   
    data_location = '~/Dropbox/Data/IPEDS/IPEDS_Analytics_DCP_87_12_CSV/' 
    __filein_main = data_location + 'delta_public_00_12.csv'

    __filein_dist = './data/ef2012a_dist_rv.csv'
    __filein_geol = './data/GeolocData.csv'
