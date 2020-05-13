import cdsapi
import sys

c = cdsapi.Client()

#date =  sys.argv[1]
year, month, day = 2018, 12, 25  # date[0:4], date[4:6], date[6:8]
targetFile = "result.,grb"  # sys.argv[2]


c.retrieve(
    'reanalysis-era5-pressure-levels',
    {
        'product_type': 'reanalysis',
        'year': '2018',
        'month': '12',
        'day': '01',
        'time': [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ],
        'format': 'grib',
        'variable': [
            'relative_humidity', 'temperature', 'u_component_of_wind',
            'v_component_of_wind',
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
        'area': [-14.0, 32.0, 59.0, 27.0],
    },
    'multi-level.grb')


c.retrieve(
    'reanalysis-era5-single-levels',
    {
        'area': [-14.0, 32.0, 59.0, 27.0],
        'variable': [
            'geopotential',
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
        'year': '2018',
        'month': '12',
        'day': '01',
        'time': [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ],
        'format': 'grib',
    },
    'single-level.grb')
