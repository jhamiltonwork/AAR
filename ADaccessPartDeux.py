import pandas as pd

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

import numpy as np
import datetime
import time

rsngroupsNeeded = 'rsnADgroupsNeeded.csv'
rsngroupsNeededTermRem = 'rsnADgroupsNeededTermRem.csv'
rsnADmaster = 'rsnADmaster.csv'
rsnAllActives = 'rsnAllActiveUsers.csv'
hrlist = 'headcount042016.csv'

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
    get_password_checks(rsnAllusers, rsnmaster)


def get_password_checks(rsnAlls, rsnmaster):
    rsnalldf = pd.DataFrame(rsnAlls, index=None, columns=['SamAccountName', 'LastBadPasswordAttempt', 'LastLogonDate',
                                                          'PasswordLastSet', 'PasswordNeverExpires',
                                                          'PasswordNotRequired', 'Modified'])
    currentusers = (rsnalldf[rsnalldf.SamAccountName.isin(rsnmaster.SamAccountName)]).reset_index(level=None, drop=True)
    currentPNR = currentusers[currentusers['PasswordNotRequired'] == True]
    writeresults(currentPNR, 'rsnPasswordNotRequired.csv', ['SamAccountName'])
    last_logon(rsnmaster)


def last_logon(rmaster):
    rsndf = pd.read_csv(rsnAllActives, sep=',', header=1, dtype=str, skip_blank_lines=True, keep_date_col=True,
                        parse_dates=[16, 17, 18, 19, 22], encoding='utf-8')
    rsndfdropna = ['LastLogonDate']
    rsndf.dropna(axis=0, how='any', subset=rsndfdropna, inplace=True)
    currentdate = pd.datetime.now().date()
    check_date = (currentdate - datetime.timedelta(days=45))
    activeusers = (rsndf[rsndf.SamAccountName.isin(rmaster.SamAccountName)]).reset_index(level=None, drop=True)
    oobusers = pd.DataFrame(activeusers, index=None, columns=['SamAccountName', 'EmailAddress', 'LastLogonDate'])
    oobusers['Logon+45days'] = (oobusers['LastLogonDate'] < check_date)
    getoobdf = (oobusers[oobusers['Logon+45days'] == True]).reset_index(level=None, drop=True)
    results_merge = pd.merge(getoobdf, rmaster, how='inner', on='SamAccountName')
    results = pd.DataFrame(results_merge, index=None, columns=['name', 'SamAccountName', 'email', 'Title',
                                                               'LastLogonDate'])
    results = (results.sort_values('name', axis=0, ascending=True)).reset_index(level=None, drop=True)
    results['DaysSinceLogon'] = pd.to_datetime(results['LastLogonDate'], errors='coerce') - pd.datetime.now().date()
    writeresults(results, 'LogonMoreThan45days.csv', ['name', 'SamAccountName', 'email', 'Title', 'LastLogonDate',
                                                      'DaysSinceLogon'])
    pass_last_set(activeusers, rmaster)


def pass_last_set(ausers, rmaster):
    ausersdf = pd.DataFrame(ausers, index=None, columns=['SamAccountName', 'EmailAddress', 'Title',
                                                         'PasswordNeverExpires', 'PasswordLastSet'])
    currentdate = pd.datetime.now().date()
    check_date = (currentdate - datetime.timedelta(days=225))
    ausersdf['PassChange+245days'] = (ausersdf['PasswordLastSet'] < check_date)
    getoobudf = (ausersdf[ausersdf['PassChange+245days'] == True]).reset_index(level=None, drop=True)
    results_merge = pd.merge(getoobudf, rmaster, how='inner', on='SamAccountName')
    results = pd.DataFrame(results_merge, index=None, columns=['name', 'SamAccountName', 'email', 'Title_y',
                                                               'PasswordNeverExpires', 'PasswordLastSet'])
    results = results.rename(columns={'Title_y': 'Title'})
    results = (results.sort_values('name', axis=0, ascending=True)).reset_index(level=None, drop=True)
    results['DaysSincePassChange'] = pd.to_datetime(results['PasswordLastSet'], errors='coerce') - pd.datetime.now().date()
    writeresults(results, 'PassLastChangeMoreThan225Days.csv', ['name', 'SamAccountName', 'email', 'Title',
                                                                'PasswordLastSet', 'DaysSincePassChange'])
    pass_never_expires(ausers, rmaster)


def pass_never_expires(actusers, rmaster):
    pnedf = pd.DataFrame(actusers, index=None, columns=['SamAccountName', 'EmailAddress', 'Title',
                                                        'PasswordNeverExpires', 'PasswordLastSet'])
    pnedf = (pnedf[pnedf['PasswordNeverExpires'] == 'True']).reset_index(level=None, drop=True)
    results_merge = pd.merge(pnedf, rmaster, how='inner', on='SamAccountName')
    results = pd.DataFrame(results_merge, index=None, columns=['name', 'SamAccountName', 'email', 'Title_y',
                                                               'PasswordNeverExpires', 'PasswordLastSet'])
    results = results.rename(columns={'Title_y': 'Title'})
    results = (results.sort_values('name', axis=0, ascending=True)).reset_index(level=None, drop=True)
    results['DaysSincePassChange'] = pd.to_datetime(results['PasswordLastSet'], errors='coerce') - pd.datetime.now().date()
    writeresults(results, 'PassWordNeverExpires.csv', ['name', 'SamAccountName', 'email', 'Title_y',
                                                       'PasswordNeverExpires', 'PasswordLastSet',
                                                       'DaysSincePassChange'])

def append_to_master(groupstoappend, radmaster):
    commons = groupstoappend[groupstoappend.name.isin(radmaster.name)]
    remcomm = (radmaster[~radmaster.name.isin(commons.name)]).reset_index(level=None, drop=True)
    concatdf = [remcomm, groupstoappend]
    results = pd.concat(concatdf)
    results2 = results.sort_values(['name'], axis=0, ascending=True, kind='mergesort')
    results2 = results2.reset_index(level=None, drop=True)
    writeresults(results2, 'rsnADmaster.csv', ['name', 'email', 'SamAccountName', 'Title', 'MemberOf'])
    combine_to_hr(results2)


def combine_to_hr(rsnADmaster):
    hrdf = ereader(hrlist, 0)
    hrdf = format_hr(hrdf)
    combinehrtoad = pd.merge(hrdf, rsnADmaster, how='inner', on='name')
    combinehrtoad = combinehrtoad.rename(columns={'Title_x': 'HR_Title', 'email': 'AD_email',
                                                  'Title_y': 'AD_Title', 'SamAccountName': 'Login Name'})
    results = pd.DataFrame(combinehrtoad, index=None, columns=['name', 'Login Name', 'Last Name', 'First Name', 'HR_Title',
                                                               'AD_Title', 'HR_email', 'AD_email', 'Direct Supervisor',
                                                               'Department', 'Job Functional Area', 'Office Location'],
                           dtype=str)
    results = results.sort_values(['name'], axis=0, ascending=True, kind='mergesort')
    results = results.reset_index(level=None, drop=True)
    writeresults(results, 'newrolemaster.csv', ['name', 'Login Name', 'Last Name', 'First Name', 'HR_Title',
                                                               'AD_Title', 'HR_email', 'AD_email', 'Direct Supervisor',
                                                               'Department', 'Job Functional Area', 'Office Location'])


def format_hr(hrcount):
    hrdf = pd.DataFrame(hrcount, index=None, columns=['Last Name', 'First Name', 'Title', 'Email Address:',
                                                      'Direct Supervisor', 'Department', 'Job Functional Area',
                                                      'Office Location'], dtype=str)
    hrdf['name'] = hrdf['First Name'].map(str) + str(' ') + hrdf['Last Name'].map(str)
    cols = hrdf.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    hrdf = hrdf[cols]
    hrdf['name'] = hrdf['name'].apply(lambda x: x.lower().strip())
    hrdf = hrdf.rename(columns={'Email Address:': 'HR_email'})
    hrdf['HR_email'] = hrdf['HR_email'].apply(lambda x: x.lower().strip())
    return hrdf


def writeresults(returnfile, filename, colstouse):
    returnfile.to_csv(filename, sep=',', usecols=colstouse, header=True, index=False, dtype=str,
                      skip_blank_lines=True, encoding='utf-8')


# -------------------MAIN--------------------------------------------------------#
if __name__ == '__main__':
    starttime = time.time()
    get_groups()
    endtime = time.time() - starttime
    print('Duration: ' + str(endtime))
