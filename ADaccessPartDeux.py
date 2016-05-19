import pandas as pd

pd.set_option('display.max_rows', 2000)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

import numpy as np
import time

rsngroupsNeeded = 'rsnADgroupsNeeded.csv'
rsngroupsNeededTermRem = 'rsnADgroupsNeededTermRem.csv'
rsnADmaster = 'rsnADmaster.csv'
rsnAllActives = 'rsnAllActiveUsers.csv'

def ereader(cfile, headtype):
    try:
        pcsv = pd.read_csv(cfile, sep=',', header=headtype, dtype=str, skip_blank_lines=True, encoding='utf-8')
        pcsv.columns.str.strip()
        if not UnicodeDecodeError:
            print('CSV is utf-8 encoded')
        else:
            return pcsv
    except UnicodeDecodeError:
        print('CSV is cp1252 encoded')
        pcsv = pd.read_csv(cfile, sep=',', header=headtype, dtype=str, skip_blank_lines=True, encoding='cp1252')
        pcsv.columns.str.strip()
        return pcsv


def get_groups():
    rsnneedgroups = ereader(rsngroupsNeeded, 0)
    rsnmaster = ereader(rsnADmaster, 0)
    rsnAllusers = ereader(rsnAllActives, 1)
    rsnneedgroupsTermRem = ereader(rsngroupsNeededTermRem, 0)
    mergemasttongroups = pd.merge(rsnAllusers, rsnneedgroupsTermRem, how='inner', on='SamAccountName')
    groupsFound = pd.DataFrame(mergemasttongroups, index=None, columns=['name', 'EmailAddress', 'SamAccountName',
                                                                        'Title_x', 'MemberOf_x'])
    groupsFound = groupsFound.rename(columns={'EmailAddress': 'email', 'Title_x': 'Title', 'MemberOf_x': 'MemberOf'})
    outliers = (rsnneedgroupsTermRem[~rsnneedgroupsTermRem.SamAccountName.isin(groupsFound.SamAccountName)])
    outliers = outliers.reset_index(level=None, drop=True)
    appendtomaster = append_to_master(groupsFound, rsnmaster)


def append_to_master(groupstoappend, radmaster):



def writeresults(returnfile, filename, colstouse):
    returnfile.to_csv(filename, sep=',', usecols=colstouse, header=True, index=False, dtype=str,
                      skip_blank_lines=True, encoding='utf-8')


# -------------------MAIN--------------------------------------------------------#
if __name__ == '__main__':
    starttime = time.time()
    get_groups()
    endtime = time.time() - starttime
    print('Duration: ' + str(endtime))
