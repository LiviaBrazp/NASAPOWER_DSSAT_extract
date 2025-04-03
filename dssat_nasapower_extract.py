#Script developed by Lívia Braz Pereira - 2025
#University of São Paulo
#Contact email: livia.braz@usp.br


import os
import pandas as pd
import requests
from datetime import datetime

#function to collect the dat
def get_daily_nasa_power_data(latitude, longitude, start_date, end_date):
    #climatic variables (daily data)
    parameters = [
        'T2M',        #average air temperature at 2 meters (°C)
        'T2M_MAX',    #maximum temperature at 2 meters (°C)
        'T2M_MIN',    #minimum temperature at 2 meters (°C)
        'PRECTOTCORR', #corrected precipitation (mm/d)
        'ALLSKY_SFC_SW_DWN', #solar radiatiom (MJ/m²/d)
        'RH2M',       #relative humidity at 2 meters (%)
        'WS2M'        #wind speed at 2 meters (m/s)
    ]
    
    #convert to datetime
    start_dt = datetime.strptime(start_date, '%Y%m%d')
    end_dt = datetime.strptime(end_date, '%Y%m%d')
    
    #check if the period does not exceed 30 years (climatological normal) 
    #this period can be adjusted
    if (end_dt - start_dt).days > 365 * 30:
        raise ValueError("maximum period is 30 years")
    
    #URL nasapower - daily data
    base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    params = {
        'parameters': ','.join(parameters),
        'community': 'AG',  #specifies the data class (data for agriculture in this case)
        'longitude': longitude,
        'latitude': latitude,
        'start': start_date,
        'end': end_date,
        'format': 'JSON'
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        #processes the daily data
        daily_data = {param: data['properties']['parameter'][param] for param in parameters}
        #dataframe with the daily data
        df = pd.DataFrame(daily_data)
        df.index = pd.to_datetime(df.index)
        df.index.name = 'DATE'
        full_range = pd.date_range(start=start_date, end=end_date)
        df = df.reindex(full_range)
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        return None

#function to save the .WTH file
def save_wth_file(df, site_code, latitude, longitude, elevation, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    
    #renames the columns to the DSSAT standard
    required_columns = {
        'T2M': 'TAVG',
        'T2M_MAX': 'TMAX',
        'T2M_MIN': 'TMIN',
        'PRECTOTCORR': 'RAIN',
        'ALLSKY_SFC_SW_DWN': 'SRAD',
        'RH2M': 'RHUM',
        'WS2M': 'WIND'
    }
    df = df.rename(columns=required_columns)
    
    #calculates the annual average values for the header
    tavg = df['TAVG'].mean()
    amp = (df['TMAX'].max() - df['TMIN'].min()) / 2
    
    #header of the .WTH file
    header = (
        f"*WEATHER DATA : {site_code}\n"
        "@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n"
        f"  {site_code}  {latitude:6.3f} {longitude:6.3f} {elevation:5.0f}  {tavg:4.1f}  {amp:4.1f}     2     2\n"
        "@DATE  SRAD  TMAX  TMIN  RAIN  RHUM  WIND\n"
    )
    
    #creates the daily data rows
    data_lines = []
    for date, row in df.iterrows():
        julian_day = date.timetuple().tm_yday
        short_year = date.strftime('%y')
        dssat_date = f"{short_year}{julian_day:03d}"
        
        #-99 for missing data
        srad = row.get('SRAD', -99)
        tmax = row.get('TMAX', -99)
        tmin = row.get('TMIN', -99)
        rain = row.get('RAIN', -99)
        rhum = row.get('RHUM', -99)
        wind = row.get('WIND', -99)
        
        line = f"{dssat_date} {srad:5.1f} {tmax:5.1f} {tmin:5.1f} {rain:5.1f} {rhum:5.0f} {wind:5.1f}"
        data_lines.append(line)
    
    #output file
    output_file = os.path.join(output_folder, f"W{site_code}01.WTH")
    
    #assembles the .WTH file
    with open(output_file, 'w') as f:
        f.write(header)
        f.write('\n'.join(data_lines))
    
    print(f"WTH file created successfully: {output_file}")


########### now insert the information for the location of interest

#main function to collect the data
def main():
    latitude = -13.54
    longitude = -58.82
    start_date = '19950101'  #format YYYYMMDD - NASAPower standard
    end_date = '20241231'  
    site_code = 'BRMT'       #DSSAT uses the site_code to link meteorological files to experiments
    elevation = 370          #elevation in meters

    #the data is collected in two periods to avoid exceeding the NASA Power download limit
    period1_data = get_daily_nasa_power_data(latitude, longitude, '19940101', '20141231')
    period2_data = get_daily_nasa_power_data(latitude, longitude, '20150101', '20241231')
    
    #then these periods are combined into a single time series
    if period1_data is not None and period2_data is not None:
        daily_data = pd.concat([period1_data, period2_data])
    
    if daily_data is not None:
        save_wth_file(daily_data, site_code, latitude, longitude, elevation, './output')
        
        #data summary
        print("\nSummary of the collected data:")
        print(daily_data.describe())

if __name__ == "__main__":
    main()
