## Importing Libs
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


import pymssql
import pandas as pd

class DataPreprocessor:

    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password

    def _connect_to_db(self):
        self.conn = pymssql.connect(self.server, self.username, self.password, self.database)

    def _disconnect_from_db(self):
        if self.conn:
            self.conn.close()

    def fetch_data_from_sql(self, file_path):
        self._connect_to_db()
        
        with open(file_path, 'r') as f:
            sql = f.read()

        df = pd.read_sql(sql, self.conn)

        self._disconnect_from_db()

        return df

    def drop_columns(self, df, columns_to_drop):
        return df.drop(columns=columns_to_drop, errors='ignore')

    def _merge_dataframes(self, main, co):
        return pd.concat([main, co])


    def apply_transformations(self, main, co):
        
        main = main[~pd.isna(main.UCScore)].copy()

        main.loc[:, 'Applicationtype'] = 0

        co = co.copy()
        co['Applicationtype'] = np.where(
            (co['HasCoapp'] == 1) & (co['CoappSameAddress'] == 1), 1,
            np.where(
                (co['HasCoapp'] == 1) & (co['CoappSameAddress'] == 0), 2,
                np.nan  # Default value for other conditions
            )
        )


        df = self._merge_dataframes(main, co)


        df['ReceivedDate'] = pd.to_datetime(df['ReceivedDate'])
        df = df.sort_values(by='ReceivedDate')


        for now in range(len(df['ReceivedDate'])-1):

            if df['ReceivedDate'].iloc[now] > df['ReceivedDate'].iloc[now+1]:
                print('NOT Sorted')


        ## save for later 
        #d = df['ReceivedDate'] 
        #d.to_csv('ReceivedDate.csv', index=False)


        # Get today's date without time
        today = pd.Timestamp('today').floor('D')

        df['BirthDate'] = pd.to_datetime(df['BirthDate'])

        # Compute the age based solely on years
        df['age'] = today.year -  df['BirthDate'].dt.year

        # Adjust for cases where the birthdate hasn't occurred this year yet
        df['age'] = np.where((today.month < df['BirthDate'].dt.month) | 
                            ((today.month == df['BirthDate'].dt.month) & (today.day < df['BirthDate'].dt.day)), 
                            df['age']-1, 
                            df['age'])






        credit_data_columns = [
            'PaymentRemarksNo',
            'PaymentRemarksAmount',
            "CreditCardsNo",
            "ApprovedCardsLimit",
            "CreditAccountsVolume",
            "CapitalIncome",
            "PassiveBusinessIncome2",
            "CapitalIncome2",
            "ActiveBusinessDeficit2",
            "KFMPublicClaimsAmount",
            "KFMTotalAmount",
            'KFMPrivateClaimsAmount',   # Added the missing comma here
            "KFMPublicClaimsNo",
            "KFMPrivateClaimsNo",
            "HouseTaxValue",
            "MortgageLoansHouseVolume",
            'MortgageLoansApartmentVolume',
            'AvgUtilizationRatio12M',
            'EmploymentIncome',
            'EmploymentIncome2'

        ]

        print(type(df.MortgageLoansHouseVolume))
               

        # Ensure the specified columns are float and fill NaN with 0
        for column in credit_data_columns:
            if column in df.columns:  # Only apply to columns that exist in the dataframe
                df[column] = df[column].astype(float).fillna(0)




        loan_columns = [
            "InstallmentLoansNo",
            "IndebtednessRatio",
            "AvgIndebtednessRatio12M",
            "InstallmentLoansVolume",
            "VolumeChange12MExMortgage",
            "VolumeChange12MUnsecuredLoans",
            "VolumeChange12MInstallmentLoans",
            "VolumeChange12MCreditAccounts",
            "VolumeChange12MMortgageLoans",
            "AvgUtilizationRatio12M",
            "CreditCardsUtilizationRatio",
            "UnsecuredLoansVolume",
            "NumberOfLenders",
            "CapitalDeficit",
            "CapitalDeficit2",
            "NewUnsecuredLoans12M",
            "NewInstallmentLoans12M",
            "NewCreditAccounts12M",
            "VolumeUsed",
            "ApprovedCreditVolume"
            ,'NumberOfBlancoLoans'
            ,'NumberOfCreditCards'
            ,'NewMortgageLoans12M'
            ,	'TotalNewExMortgage12M'

            ,  "NumberOfMortgageLoans",
            "SharedVolumeMortgageLoans",
            "SharedVolumeCreditCards",
            "NumberOfUnsecuredLoans",
            "SharedVolumeUnsecuredLoans",
            "NumberOfInstallmentLoans",
            "SharedVolumeInstallmentLoans",
            "NumberOfCreditAccounts",
            "SharedVolumeCrerditAccounts"
            ,'UnsecuredLoansNo'
            , 'IncomeDelta_1Year'
            ,'kids_number'

            ,'Inquiries12M'

        ]



        # Ensure the specified columns are float and fill NaN with -1
        for column in loan_columns:
            if column in df.columns:  # Only apply to columns that exist in the dataframe
                df[column] = df[column].astype(float).fillna(-1)


        loan_columns = [
        "CapitalDeficit_Delta_1Year","UtilizationRatio",'housing_cost']



        # Ensure the specified columns are float and fill NaN with -1
        for column in loan_columns:
            if column in df.columns:  # Only apply to columns that exist in the dataframe
                df[column] = df[column].astype(float).fillna(-100)



        inf_columns = ['CapitalDeficit_Delta_1Year',
                    'IncomeDelta_1Year',
                    'ActiveCreditAccounts']
            
        for col in inf_columns:
            if col in df.columns:
                df[col] = df[col].replace([np.inf, -np.inf], -100)


        ## the rest

        for Cname in df.columns:

            if str(df[Cname].dtype) == 'object':
                df[Cname].fillna('Unknown', inplace=True)
                df[Cname].replace('None', 'Unknown', inplace=True)


        df['PropertyVolume'] = np.where( df.MortgageLoansHouseVolume > 0, df.MortgageLoansHouseVolume,
            np.where( df.MortgageLoansApartmentVolume > 0, df.MortgageLoansApartmentVolume, 0))


        return df

    def process_data(self, main_sql_file_path, co_sql_file_path):
            
            # Fetch data for 'main'
            main = self.fetch_data_from_sql(main_sql_file_path)
            # Drop columns for 'main'
            #main = self.drop_columns(main, columns_to_drop_main)
            
            # Fetch data for 'co'
            co = self.fetch_data_from_sql(co_sql_file_path)
            # Drop columns for 'co'
            #co = self.drop_columns(co, columns_to_drop_co)
            
            # Apply transformations on both main and co
            final_df = self.apply_transformations(main, co)
            
            return final_df