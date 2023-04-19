import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor


pd.set_option('display.float_format', lambda x: '%.6f' % x)
np.set_printoptions(suppress=True)


def cumsum_columns(ProdData):
    ProdData['cum_days'] = ProdData['ActDaysOn'].groupby(ProdData['API']).cumsum()
    ProdData['cum_oil'] = ProdData['Oil'].groupby(ProdData['API']).cumsum()
    ProdData['cum_gas'] = ProdData['Gas'].groupby(ProdData['API']).cumsum()
    ProdData['cum_boe'] = ProdData['cum_oil'] + ProdData['cum_gas'] / 6
    ProdData['cum_water'] = ProdData['Water'].groupby(ProdData['API']).cumsum()
    ProdData['cum_water_inj'] = ProdData['Water_Inj'].groupby(ProdData['API']).cumsum()
    return ProdData

def calculate_cumulatives(well_id, ProdData, dayslist):
    cumulatives = []
    for j in dayslist:
        tempdf = ProdData[ProdData.API == well_id]
        cumulativesht = tempdf.loc[(tempdf['cum_days'] >= j).idxmax(axis=0)]

        cumoilans = round((cumulativesht.cum_oil - ((cumulativesht.cum_days - j) * cumulativesht.daily_oil)), 0)
        cumwaterans = round((cumulativesht.cum_water - ((cumulativesht.cum_days - j) * cumulativesht.daily_Water)), 0)
        cumgasans = round((cumulativesht.cum_gas - ((cumulativesht.cum_days - j) * cumulativesht.daily_gas)), 0)

        cumulatives.append((well_id, j, cumoilans, cumwaterans, cumgasans))
    return cumulatives

def process_well(ProdData, dayslist, well_id):
    cumulatives = calculate_cumulatives(well_id, ProdData, dayslist)
    return pd.DataFrame(cumulatives, columns=["API", "Cum_Time_Frame_Days", "Cum_Oil_Answer", "Cum_Water_Answer", "Cum_Gas_Answer"])

def main():
    
    global dayscums_df
    
    # Load your ProdData DataFrame here
    ProdData = pd.read_csv("D:/ND Prod Work 103121/ND_Prod/NDprod08-22.csv")
    ProdData = ProdData.drop(["Unnamed: 0"], axis=1)
    ProdData = ProdData.rename(columns={'0': 'State_Id_Num', '1': 'API', '2': 'Formation', '3': 'Date', '4':'Oil', 
                                  '5':'Gas', '6':'Flared', '7':'ActDaysOn', '8':'Water', '9':'Water_Inj', '10':'unknown'})
    
    #Probably need to rearrange the titles above - but they will differ state to state
    #For ND, you have to break out some wells by formation, too.
    
    ProdData["daily_oil"]  = ProdData["Oil"]/ProdData["ActDaysOn"]
    ProdData["daily_Water"]  = ProdData["Water"]/ProdData["ActDaysOn"]
    ProdData["daily_gas"]  = ProdData["Gas"]/ProdData["ActDaysOn"]

    ProdData = cumsum_columns(ProdData)
    wellid = ProdData.API.unique()
    dayslist = [30, 90, 180, 365, 545, 730]  # <-You can change these time frame values (days) to whatever number days you want.

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda well_id: process_well(ProdData, dayslist, well_id), wellid))

    dayscums = pd.concat(results)
    dayscums = dayscums.reset_index(drop=True)
    dayscums_df = pd.DataFrame(dayscums)
    print(dayscums)

if __name__ == '__main__':
    main()
