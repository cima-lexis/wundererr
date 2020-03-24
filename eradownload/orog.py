import cdsapi
import sys

c = cdsapi.Client()

date = sys.argv[1]
year, month, day = date[0:4], date[4:6], date[6:8]
targetFile = sys.argv[2]

c.retrieve(
    'reanalysis-era5-single-levels',
    {
        'product_type': 'reanalysis',
        'variable': 'orography',
        'year': year,
        'month': month,
        'day': day,
        'time': '00:00',
        'format': 'netcdf',
    },
    targetFile
)
