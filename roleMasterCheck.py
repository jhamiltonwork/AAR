import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import time


headcount = 'headcount042016.csv'
rolemaster = 'roleMaster.csv'


def ereader(cfile, headtype):
    try:
        pcsv = pd.read_csv(cfile, sep=',', header=headtype, dtype=str, skip_blank_lines=True, encoding='utf-8')
        pcsv.columns.str.strip()
        if not UnicodeDecodeError:
            print('CSV is utf-8 encoded')
        else:
            return pcsv
    except UnicodeDecodeError:
        pcsv = pd.read_csv(cfile, sep=',', header=headtype, dtype=str, skip_blank_lines=True, encoding='cp1252')
        pcsv.columns.str.strip()
        return pcsv

def formatter(colname, rvalues):
    edf = pd.DataFrame({colname: rvalues})
    fedf = edf[colname].str.lower()
    fsedf = fedf.str.strip()
    fdf = pd.DataFrame({colname: fsedf})
    return fdf


def compare_names(hfile, radfile):
    rcol = ['NameConcat', 'Email Address:']
    raddf = pd.DataFrame(radfile, index=None, columns=rcol)
    hcol = ['First Name', 'Last Name', 'Email Address:']
    hdf = pd.DataFrame(hfile, index=None, columns=hcol)
    hdf['NameConcat'] = hdf['First Name'].map(str) + str(' ') + hdf['Last Name'].map(str)
    hccol = ['NameConcat', 'Email Address:']
    hcdf = pd.DataFrame(hdf, index=None, columns=hccol)
    fhnamecol = formatter('name', (hcdf['NameConcat']))
    frnamecol = formatter('name', (raddf['NameConcat']))
    fhemailcol = formatter('email', (hcdf['Email Address:']))
    #fremailcol = formatter('email', (raddf['Email Address:']))
    fhdf = pd.concat([fhnamecol['name'], fhemailcol['email']], axis=1, keys=['name', 'email'])
    common = pd.merge(fhnamecol, frnamecol, how='inner', on=['name'])
    common2 = common['name']
    mcommon = (fhnamecol[~fhnamecol.name.isin(common2)]).reset_index(level=None, drop=True)
    result = pd.merge(mcommon, fhdf, how='inner', on=['name'])
    print(result)

def new_employee():
    rolem = ereader(rolemaster, 0)
    heads = ereader(headcount, 0)
    get_new = compare_names(heads, rolem)


#-------------------MAIN--------------------------------------------------------#
if __name__ == '__main__':

    starttime = time.time()
    new_employee()
    endtime = time.time() - starttime
    print('Duration: ' + str(endtime))