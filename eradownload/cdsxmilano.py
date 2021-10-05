import calendar
import cdsapi
import sys
from datetime import date
from calendar import monthrange
c = cdsapi.Client()


def download_day(year, month, day):
    single_date = date(year, month, day)
    hours = ["%02d:00" % h for h in range(0, 24)]
    #area = [75, -40, -5, 50]
    area = [80, -40, 22, 56]
    days = [str(day)] #, str(day+1)]
    
    c.retrieve(
        'reanalysis-era5-pressure-levels',
        {
            'product_type': 'reanalysis',
            'year': str(year),
            'month': str(month),
            'day': days,
            'time': hours,
            'format': 'grib',
            'variable': [
                'relative_humidity', 'temperature', 'u_component_of_wind',
                'v_component_of_wind', 'specific_humidity', 'geopotential'
            ],
            'pressure_level': [
                '1', '2', '3',
                '5', '7', '10',
                '20', '30', '50',
                '70', '100', '125',
                '150', '175', '200',
                '225', '250', '300',
                '350', '400', '450',
                '500', '550', '600',
                '650', '700', '750',
                '775', '800', '825',
                '850', '875', '900',
                '925', '950', '975',
                '1000',
            ],
            'area': area,
        },
        single_date.strftime("multi-level-%Y-%m-%d") + ".grb" 
    )
    
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'area': area,
            'product_type': 'reanalysis',
            'variable': [
                '10m_u_component_of_wind',
                '10m_v_component_of_wind', 
                '2m_temperature', 
                '2m_dewpoint_temperature',
                'land_sea_mask',
                'surface_pressure',
                'mean_sea_level_pressure',
                'skin_temperature',
                'sea_ice_cover',
                'sea_surface_temperature',
                'snow_density',
                'snow_depth',
                'soil_temperature_level_1',
                'soil_temperature_level_2',
                'soil_temperature_level_3',
                'soil_temperature_level_4',
                'volumetric_soil_water_layer_1',
                'volumetric_soil_water_layer_2',
                'volumetric_soil_water_layer_3',
                'volumetric_soil_water_layer_4',
            ],
            'year': str(year),
            'month': str(month),
            'day': days,
            'time': hours,
            'format': 'grib',
        },
        single_date.strftime("single-level-%Y-%m-%d") + ".grb" 
    )
    

def download_month(year, month):
    start_date = date(year, month, 1)
    end_date = date(year, month, calendar.monthrange(year, month)[1])

    for day in range(1, 1 + calendar.monthrange(year, month)[1]):
        download_day(year,month, day)
                
#download_day(2018, 6, 9)
#download_month(2015, 6)
#download_month(2015, 1)
download_day(2015, 7, 1)
download_day(2015, 2, 1)
download_day(2015, 6, 30)
download_day(2015, 1, 31)
