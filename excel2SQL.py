#   -------------------------------------------------------------------------------------------------------
#   NOTIFICATION
#   -------------------------------------------------------------------------------------------------------
#   1 Change the file paths in the READ section. (folder, filename, filename1, filename2, also the region)
#   2 Change the column names according to your files' column name in the STORE section.
#     Also in the Update lithology or waterlevel part.
#     e.g. lu.rename(columns={'HydroID':'original_db_ID'}, inplace = True)
#          change 'HydroID' into the name of the original_db_ID column in your file.
#     e.g. rli = r['Description']
#          change 'Description' into the name of the lithology record column in your file.
#   3 If some database-related issues occur, run 'conn.rollback()'.
#     It is likely you will see this when you did not run the line :
#     InternalError: current transaction is aborted, commands ignored until end of transaction block
#   4 On water level record: rwl = surface_e - r['result'] or rwl = r['result']. 
#     Change the formula in regards to whether you have the water level or depth to water record.
#     It is in the Update water level table subsection under the STORE section.

    import pandas as pd
    import numpy as np
    import psycopg2 as pg2 # This package is made for database connection.
    import os
#   -------------------------------------------------------------------------------------------------------
#   CONNECTION
#   -------------------------------------------------------------------------------------------------------
#   Make connection to the PostgreSQL PgAdmin4 server.
    conn = pg2.connect(dbname='HydroGeoDB', user='postgres', password='alphabetagamma')
    cur = conn.cursor()
    print('HydroGeoDB database is connected.')
#   -------------------------------------------------------------------------------------------------------
#   READ
#   -------------------------------------------------------------------------------------------------------
#   Read in the data from the Excel format and store them in a dataframe.
#    folder = 'F:/dbMay/Australia/shp_ACT/'
    folder = 'F:/dbMay/Australia/gdb_NT/'
    filename2 = 'NGIS_BoreholeLog.csv' 
    filename1 = 'coordinates.csv'
    filename = 'level_NT.csv'  
    #   wl: water level and related info from the original file, format: dataframe
    wl = pd.read_csv(folder + filename)
    #   lu: lookup table
    lu = pd.read_csv(folder + filename1)
    #   li: lithology table
    li = pd.read_csv(folder + filename2)
    region = 'Northern Territory'
#    -------------------------------------------------------------------------------------------------------
#   STORE 
#   -------------------------------------------------------------------------------------------------------
#   Store the data in PostgreSQL.
    #   Change the column names
    lu.rename(columns={'HydroID':'original_db_ID'}, inplace = True)
    lu.rename(columns={'LandElev':'land_surface_elevation'}, inplace = True)
    lu.rename(columns={'Longitude':'x_coordinate'}, inplace=True)
    lu.rename(columns={'Latitude':'y_coordinate'}, inplace=True)
    lu.rename(columns={'BoreDepth':'well_depth'}, inplace=True)
    lu.rename(columns={'FTypeClass':'water_use'}, inplace=True)
    #   header: column name of the original file, format: numpy.ndarray
    header = wl.columns.values
    header2 = lu.columns.values
    sql_insert_wl = 'INSERT INTO public.waterlevel' 
    sql_insert_lk = 'INSERT INTO public.lookup'    
    sql_set = 'UPDATE public.lookup SET ' # Edit an existent row
    #--------------------------Update the lookup table.
    #   Take the largest B_ID from the database, and name the new record with B_ID + 1.
    #   DO NOT RUN THIS IF THE ROW ALREADY EXISTS!
    for index, row in lu.iterrows():
        cur.execute('SELECT max("B_ID") FROM public.lookup;')
        conn.commit()
        # The new primary key (B_ID, an integer) is taken as the integer
        # larger than the existent maximal B_ID.
        newpk = int(cur.fetchall()[0][0]) + 1
        sql_execute = sql_insert_lk + '(\"B_ID\") VALUES(' + str(newpk) + ');'
        cur.execute(sql_execute)
        conn.commit() # If commit() is not called, the effect of any data manipulation will be lost.
        sql_execute = sql_set + '\"original_db_ID\" = {0} WHERE \"B_ID\" = {1};'.format(row['original_db_ID'], str(newpk))
        cur.execute(sql_execute)
        conn.commit()
        sql_execute = 'UPDATE public.lookup SET country= \'Australia\', region=\'{0}\' WHERE \"B_ID\" = {1};'.format(region, str(newpk))
        cur.execute(sql_execute)
        conn.commit()
    #   RUN THE FOLLOWING WHEN THE ROW ALREADY EXISTS.
    #   Now the new rows are created in the databse, the following is to update the columns of each row.
    sql_column = 'SELECT column_name FROM information_schema.columns where table_name =\'lookup\';'
    cur.execute(sql_column)
    conn.commit()
    columns = cur.fetchall() #   The list 'column' stores the current columns of the table from the database.    
    del columns[0]# Delete the first column (the primary key B_ID), because it is used just now.
    sql_dtype = 'SELECT data_type FROM information_schema.columns where table_name =\'lookup\';' 
    cur.execute(sql_dtype)
    conn.commit()
    dtype = cur.fetchall()
    for index, row in lu.iterrows():
        cur.execute('SELECT "B_ID" FROM public.lookup WHERE \"original_db_ID\" = \'{0}\';'.format(str(row['original_db_ID'])))
        conn.commit()
        B_ID = cur.fetchall()
        if B_ID == []:
            continue
        B_ID = B_ID[0][0]
        for column in columns:
            column = column[0] # The original column here is a tuple, and this will change it into a string.
            if column in lu.columns.values: # Read in the columns that also exist in the database. 
                if pd.isna(row[column]): #   When the item is missing, NULL value needs to be filled in.
                    sql_execute = sql_set + '\"'+ column + '\"' +'= NULL WHERE \"B_ID\" =' + str(B_ID) + ';'
                    cur.execute(sql_execute)
                    conn.commit()
                else:
                    sql_execute = sql_set + '\"'+column + '\"'+'=\'' + str(row[column]) + '\' WHERE \"B_ID\" =' + str(B_ID) + ';'
                    cur.execute(sql_execute)
                    conn.commit()
    print('The file below is stored in the database: \n' + filename1)     
    #--------------------------Update the water level table
    for row in range(len(wl)):
#    for row in range(100):
        # r: Record for month/year/water level/well id/
        r = wl.loc[row]
        cur.execute('SELECT "land_surface_elevation" FROM public.lookup WHERE \"original_db_ID\" = \'{0}\';'.format(str(r['hydroid'])))
        conn.commit()
        surface_e0 = cur.fetchall() 
        if surface_e0 == []:
            continue
        surface_e = float(surface_e0[0][0])
        rwl = surface_e - r['result'] # We want water level to the reference elevation
        # while r['result'] is depth to water so the water level = surface elevation - depth to water
        ryear = pd.to_datetime(r['bore_date']).year
        rmonth = pd.to_datetime(r['bore_date']).month
        rid = r['hydroid']
        rtype = 'onemeasurement'
        if ryear < 1940 or ryear >2018 or pd.isna(rwl) or pd.isna(ryear) or rwl == 0:
            continue
        cur.execute('SELECT "B_ID" FROM public.lookup WHERE "original_db_ID" = \'{0}\' and "B_ID"> \'{1}\''.format(rid, 1124873))
        conn.commit()
        W_ID = cur.fetchall()
        cur.execute('SELECT \"month\" FROM public.waterlevel WHERE \"W_ID\" = {0} and \"month\" = \'{1}\''.format(str(W_ID[0][0]), rmonth))
        conn.commit()
        flag = cur.fetchall()
        if flag != []:
            continue
        try:
            cur.execute('INSERT INTO public.waterlevel' + ' (\"wlevel_{0}\" , type,  \"W_ID\", month) VALUES({1}, \'{2}\', \'{3}\', {4});'.format(ryear, rwl,rtype,  str(W_ID[0][0]), rmonth))
        except:
            conn.rollback()
            cur.execute('UPDATE public.waterlevel SET \"wlevel_{0}\" = {1}, type = \'{2}\' WHERE \"W_ID\" = {3} AND month = \'{4}\''.format(ryear, rwl,rtype,  str(W_ID[0][0]), rmonth))
        conn.commit()
        cur.execute('UPDATE public.lookup SET has_timeseries_water_level=\'{0}\' WHERE \"B_ID\"= \'{1}\';'.format(1, str(W_ID[0][0])))
        conn.commit()
        print(str(row))
    print('The file below is stored in the database: \n' + filename)
    #--------------------------Update the lithology table.
    for row in range(len(li)):
        # r: Record for month/year/water level/well id/x coordinate/y coordinate
        r = li.loc[row]
        rli = r['Description']
        rid = r['BoreID']
        rstart = int(r['FromDepth'])
        rend = int(r['ToDepth'])
        cur.execute('SELECT "B_ID" FROM public.lookup WHERE "original_db_ID" = \'{0}\' and "B_ID"> \'{1}\''.format(rid, 17698))
        conn.commit()
        W_ID = cur.fetchall()
        if W_ID == []:
            continue
        for record in range(rstart + 1, rend + 1):
            if record > 1000:
                continue
            if record > 500:
                sql_insert_li = 'INSERT INTO public.lithologydown'
                sql_set = 'UPDATE public.lithologydown SET '
            else:
                sql_insert_li = 'INSERT INTO public.lithologyprofile'
                sql_set = 'UPDATE public.lithologyprofile SET '
            columnname = 'lithology_{0}m'.format(str(record))
            try:
                cur.execute(sql_insert_li + ' (\"B_ID\", \"{0}\") VALUES({1}, \'{2}\');'.format(columnname, str(W_ID[0][0]), rli))                
            except:
                conn.rollback()
                cur.execute(sql_set + '\"{0}\" = \'{1}\' WHERE \"B_ID\" = {2}'.format(columnname, rli, W_ID[0][0]))
            conn.commit()
            cur.execute('UPDATE public.lookup SET has_lithology_profile = \'{0}\' WHERE \"B_ID\"= \'{1}\';'.format(1, str(W_ID[0][0])))
        conn.commit()
        print(str(row))
    print('The file below is stored in the database: \n' + filename2)
#   -------------------------------------------------------------------------------------------------------
#   DISCONNECTION
#   -------------------------------------------------------------------------------------------------------
#   Shut down the connection to the database.
    conn.close() 
    print('HydroGeoDB database is closed.')        

 