    from netCDF4 import Dataset
    dataset = Dataset("D:/internship/Data/v2temp/temp_Rotterdam_hom.nc")
    # print(dataset.file_format) will return "NETCDF4_CLASSIC"
    # print(dataset.dimensions.keys()) will return 'time'
    time = dataset.variables['time'][:]
    # time is a masked array: 
    # arrays that may have missing or invalid entries
    for idx in range(828):
        year = 1951 + int(idx/12)
        month = idx%12 + 1
        print("time:{0}-{1}".format(year, month))
        print("temperature:{0}".format(dataset['T'][idx]))
    dataset.close()