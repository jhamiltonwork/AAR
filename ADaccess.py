import pandas as pd

pd.set_option('display.max_rows', 2000)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

import numpy as np
import time


genpact = 'genpact.csv'
headcount = 'headcount042016.csv'
rsnAD = 'rsnAllActiveUsers.csv'
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
        print('CSV is cp1252 encoded')
        pcsv = pd.read_csv(cfile, sep=',', header=headtype, dtype=str, skip_blank_lines=True, encoding='cp1252')
        pcsv.columns.str.strip()
        return pcsv


def dfformatter(colname, rvalues):
    edf = pd.DataFrame({colname: rvalues})
    fedf = edf[colname].str.lower()
    fsedf = fedf.str.strip()
    fdf = pd.DataFrame({colname: fsedf})
    return fdf


def emailformatter(emails):
    edf = pd.DataFrame({'email': emails})
    fedf = edf['email'].str.lower()
    fsedf = fedf.str.strip()
    fdf = pd.DataFrame({'email': fsedf})
    return fdf


def findnomatch(hcol, rcol):
    cdf = hcol.merge(rcol, on='email')
    ccdf = (cdf['email'])
    mdf = (hcol[~hcol.email.isin(ccdf)]).reset_index(level=None, drop=True)
    return mdf


def hremailmatchad():
    hcsv = ereader(headcount, 0)
    rcsv = ereader(rsnAD, 1)
    recsv = rcsv['EmailAddress']
    hecsv = hcsv['Email Address:']
    rdf = emailformatter(recsv)
    hdf = emailformatter(hecsv)
    rdf.sort_values(['email'], ascending=True, inplace=True, kind='mergesort', na_position='last')
    hdf.sort_values(['email'], ascending=True, inplace=True, kind='mergesort', na_position='last')
    fdf = findnomatch(hdf, rdf)
    hrname = hcsv['First Name'].map(str) + str(' ') + hcsv['Last Name'].map(str)
    fhrname = dfformatter('name', hrname)
    hrdf = pd.DataFrame(fhrname['name'])
    hrdf['email'] = hdf['email']
    results = pd.merge(fdf, hrdf, how='inner', on='email')
    fresults = pd.DataFrame(results, columns=['name', 'email'])
    matchtorolemast = nomatchemailtotemplate(results)
    matchtorolemast = matchtorolemast.reset_index(level=None, drop=True)
    comparename = pd.merge(matchtorolemast, hrdf, how='inner', on='name').reset_index(level=None, drop=True)
    comparename.drop('email_y', axis=1, level=None, inplace=True)
    comparename = comparename.rename(columns={'email_x': 'email'})
    comparenameresults = comparename['name']
    remainingnomatches = (fresults[~fresults.name.isin(comparenameresults)]).reset_index(level=None, drop=True)
    hrdfmincompname = (hrdf[~hrdf.name.isin(comparenameresults)]).reset_index(level=None, drop=True)
    newrolecol = [hrdfmincompname, comparename]
    newrolemaster = pd.concat(newrolecol).reset_index(level=None, drop=True)
    (emptysams, outliers) = buildrsnad(rcsv, newrolemaster)
    fillemptysams(emptysams, fresults, outliers)


def fillemptysams(esams, adgroupsneeded, outliers):
    rolecsv = ereader(rolemaster, 0)
    rolecsv = rolecsv.rename(columns={'NameConcat': 'name', 'Email Address:': 'email'})
    rolecsv['name'] = rolecsv['name'].apply(lambda x: x.lower().strip())
    rolecsv['email'] = rolecsv['email'].apply(lambda x: x.lower().strip())
    roledfcol = ['Role', 'Department', 'name', 'email', 'Office Location', 'User name', 'Title', 'AD RSN', 'AD RSN Admin']
    roledf = pd.DataFrame(rolecsv, columns=roledfcol)
    originalesams = esams
    esamsdf = pd.DataFrame(esams)
    esams = esams['SamAccountName'].replace('NaN', np.nan, inplace=True)
    emptycol = ['SamAccountName']
    esamsdf.dropna(axis=0, how='any', subset=emptycol, inplace=True)
    esamsname = esamsdf['SamAccountName']
    getempty = (originalesams[~originalesams.SamAccountName.isin(esamsname)]).reset_index(level=None, drop=True)
    emptymergeonrole = pd.merge(getempty, rolecsv, how='inner', on='email')
    emptymergeonrole['SamAccountName'] = emptymergeonrole['User name']
    emptymergeonrole['Title_x'] = emptymergeonrole['Title_y']
    femptycol = ['Role', 'name_x', 'email', 'SamAccountName', 'Title_x', 'Department', 'Office Location']
    writecol = ['SamAccountName']
    femptymergeonrole = pd.DataFrame(emptymergeonrole, columns=femptycol, dtype=str)
    results = (esamsdf[~esamsdf.SamAccountName.isin(writecol)]).reset_index(level=None, drop=True)
    resultsmergeadneeded = pd.merge(adgroupsneeded, results, how='left', on='name')
    resultsmergeadneeded = resultsmergeadneeded.rename(columns={'email_x': 'email-hr', 'email_y': 'email-rolemast'})
    adgroupscol = ['name', 'email-hr', 'email-rolemast', 'SamAccountName']
    writeresults(resultsmergeadneeded, 'rsnADgroupsNeeded.csv', adgroupscol)
    resultsmergeadname = resultsmergeadneeded['name']
    resultstoadgroupsetup = (originalesams[~originalesams.name.isin(resultsmergeadname)]).reset_index(level=None, drop=True)
    checkrunner(roledf, resultstoadgroupsetup, outliers, esamsdf)


def checkrunner(rolemdf, newgroups, outliers, esamie):
    orole = rolemdf
    onewgroups = newgroups
    titlechange = newtitlecheck(rolemdf, newgroups)
    newemployee = newemployeechecker(rolemdf, newgroups)
    groupchange = groupstochecker(rolemdf, newgroups)
    (genp_and_gl, outlierlist, adminlist, rsnrad) = genpactreport(newgroups, outliers)
    combine_results(titlechange, newemployee, groupchange, genp_and_gl, outlierlist, adminlist, rsnrad, rolemdf, esamie)


def combine_results(titles, newhire, changegroups, genpact, outliers, admincheck, rsnad, rolesheet, newrolemaster):
    fnewhire = format_newhire(newhire)
    ftitles = format_titlechange(titles, rolesheet, fnewhire)
    fchangegroups = format_groupchange(changegroups, fnewhire)
    fgenpact = format_genpact(genpact)
    foutliers = format_outliers(outliers, rsnad)
    (fadmin, adminOutliers) = format_admin(admincheck, rsnad, newrolemaster)
    writeresults(fnewhire, 'newHire.csv', ['name', 'SamAccountName', 'email', 'Title', 'MemberOf'])
    writeresults(ftitles, 'titleChanges.csv', ['name', 'SamAccountName', 'email', 'formerTitle',
                                               'newTitle', 'MemberOf'])
    writeresults(fchangegroups, 'rsnADgroupChanges.csv', ['name', 'SamAccountName', 'email', 'Title', 'MemberOf'])
    writeresults(fgenpact, 'currentGenPactGlogicEmployees.csv', ['name', 'SamAccountName', 'DisplayName', 'email',
                                                                 'Title', 'MemberOf'])
    writeresults(foutliers, 'HRtoRSNadNoMatches.csv', ['name', 'SamAccountName', 'Displayname', 'email',
                                                       'Title', 'MemberOf'])
    writeresults(fadmin, 'rsnAdmin.csv', ['name', 'adminName', 'SamAccountName', 'email', 'adminEmail',
                                          'Title', 'MemberOf', 'AdminMemberOf'])
    writeresults(adminOutliers, 'rsnAdminNoMatchHr.csv', ['AdminSamAccountName', 'name', 'AdminMemberOf'])
    writeresults(newrolemaster, 'rsnADmaster.csv', ['name', 'email', 'SamAccountName', 'Title', 'MemberOf'])


def format_admin(admins, rsnrad, newroler):
    rsnad = pd.merge(rsnrad, admins, how='inner', on='SamAccountName')
    addf = pd.DataFrame(rsnad)
    addf['AdminSamAccountName'] = addf['SamAccountName']
    addf['SamAccountName'] = addf['SamAccountName'].str.replace('a-', '')
    addfdropcol = ['Enabled_x', 'Title_x', 'Displayname_x', 'Enabled_y', 'Displayname_y', 'email_y', 'Title_y', 'MemberOf_y', 'name_y']
    addf = addf.drop(addfdropcol, axis=1, level=None)
    addfmerge = pd.merge(newroler, addf, how='inner', on='SamAccountName')
    results = pd.DataFrame(addfmerge, index=None, columns=['name', 'name_x', 'SamAccountName', 'AdminSamAccountName',
                                                           'email', 'email_x', 'Title', 'MemberOf', 'MemberOf_x'])
    results = results.rename(columns={'name_x': 'adminName', 'email_x': 'adminEmail', 'MemberOf_x': 'AdminMemberOf'})
    adminOutliers = (addf[~addf.SamAccountName.isin(results.SamAccountName)]).reset_index(level=None, drop=True)
    adminOutliers = pd.DataFrame(adminOutliers, index=None, columns=['AdminSamAccountName', 'name_x', 'MemberOf_x'])
    adminOutliers = adminOutliers.rename(columns={'name_x': 'name', 'MemberOf_x': 'AdminMemberOf'})
    return results, adminOutliers


def format_outliers(outies, radical):
    outdf = pd.merge(radical, outies, how='inner', on='email')
    outdf = pd.DataFrame(outdf, index=None, columns=['name', 'SamAccountName_x', 'Displayname', 'email',
                                                     'Title_x', 'MemberOf_x'])
    outdf = outdf.rename(columns={'SamAccountName_x': 'SamAccountName', 'Title_x': 'Title', 'MemberOf_x': 'MemberOf'})
    return outdf

def format_genpact(genie):
    gdf = pd.DataFrame(genie, index=None, columns=['name', 'SamAccountName', 'Displayname', 'email', 'Title', 'MemberOf'])
    return gdf


def format_groupchange(changeling, newbs):
    mergetochange = pd.merge(changeling, newbs, how='inner', on='name')
    remnewbs = (changeling[~changeling.name.isin(mergetochange.name)]).reset_index(level=None, drop=True)
    results = pd.DataFrame(remnewbs, index=None, columns=['name', 'SamAccountName', 'email', 'Title', 'MemberOf'])
    return results


def format_newhire(newbs):
    newbs = newbs.rename(columns={'New Employee': 'newHire'})
    filternew = (newbs[newbs.newHire.str.contains('True')]).reset_index(level=None, drop=True)
    results = pd.DataFrame(filternew, index=None, columns=['name', 'SamAccountName', 'email', 'Title', 'MemberOf'])
    return results


def format_titlechange(tchange, roleformer, newb):
    rfdf = pd.DataFrame(roleformer, index=None, columns=['name', 'email', 'Title'], dtype=str)
    rfdf['Title'] = rfdf['Title'].apply(lambda x: x.lower().strip())
    rfdf.sort_values(['name'], ascending=True, inplace=True, kind='mergesort', na_position='last')
    tchange = tchange.rename(columns={'Title Changed': 'titleChange'})
    filtertch = tchange[tchange.titleChange.str.contains('False')]
    #newb = newb.rename(columns={'New Employee': 'newHire'})
    #filternew = newb[newb.newHire.str.contains('True')]
    combtchnewb = pd.merge(filtertch, newb, how='inner', on='name')
    remnewbs = (filtertch[~filtertch.name.isin(combtchnewb.name)]).reset_index(level=None, drop=True)
    combtorolef = pd.merge(rfdf, remnewbs, how='inner', on='name')
    results = pd.DataFrame(combtorolef, index=None, columns=['name', 'SamAccountName', 'email_y', 'Title_x', 'Title_y',
                                                             'MemberOf'])
    results = results.rename(columns={'Title_x': 'formerTitle', 'Title_y': 'newTitle', 'email_y': 'email'})
    return results

def newemployeechecker(rolem, newemplist):
    rolemast = rolem
    newemper = newemplist
    rolemast['name'] = rolemast['name'].apply(lambda x: x.lower().strip())
    newemper['name'] = newemper['name'].apply(lambda x: x.lower().strip())
    rolemname = rolemast['name']
    newempname = newemper['name']
    rolearray = pd.unique(rolemname.ravel())
    newemparray = pd.unique(newempname.ravel())
    rolearray.sort()
    newemparray.sort()
    newemployeebool = np.in1d(newemparray, rolearray)
    combnewdf = pd.DataFrame({'New Employee': newemployeebool, 'name': newemparray})
    convert = {True: 'False', False: np.nan}
    combnewdf['New Employee'].apply(lambda x: convert[x])
    mergenewemps = pd.merge(newemper, combnewdf, how='left', on='name')
    mergenewemps['New Employee'].replace(to_replace=(False, True), value=('True', 'False'), inplace=True)
    resultcol = ['name', 'New Employee', 'email', 'SamAccountName', 'Title', 'MemberOf']
    results = pd.DataFrame(mergenewemps, index=None, columns=resultcol, dtype=str)
    return results


def groupstochecker(roller, newergroupies):
    newadgroups = adgroupsetup(newergroupies)
    rolecheckt = rolemastsetcheckt(roller)
    rolecheckgroups = adgroupsetup(rolecheckt)
    groupchange = newadgroupchecktoroller(rolecheckgroups, newadgroups)
    return groupchange


def newtitlecheck(roletemp, newad):
    roletitles = ((roletemp['Title']).str.lower())
    newadtitles = ((newad['Title']).str.lower())
    roletitles.str.strip()
    newadtitles.str.strip()
    roleunique = pd.unique(roletitles.ravel())
    newunique = pd.unique(newadtitles.ravel())
    roleunique.sort()
    newunique.sort()
    newtitles = np.in1d(newunique, roleunique)
    combnewarray = np.vstack((newtitles, newunique)).T
    combshape = combnewarray.shape
    combshape2 = (combshape[0])
    combnewarray2 = combnewarray.reshape(combshape2, 2)
    combnewdf = pd.DataFrame({'Title Changed': newtitles, 'Title': newunique})
    convert = {True: 'True', False: np.nan}
    combnewdf['Title Changed'].apply(lambda x: convert[x])
    newad['Title'] = newad['Title'].apply(lambda x: x.lower().strip())
    mergenewtoad = pd.merge(newad, combnewdf, how='left', on='Title')
    mergenewtoad['Title Changed'].replace(to_replace=np.nan, value='False', inplace=True)
    resultscol = ['name', 'email', 'SamAccountName', 'Title Changed', 'Title', 'MemberOf']
    resultsdf = pd.DataFrame(mergenewtoad, index=None, columns=resultscol, dtype=str)
    return resultsdf


def newadgroupchecktoroller(rolemgroups, newgrouplist):
    commons = pd.merge(rolemgroups, newgrouplist, how='inner', on=['name', 'MemberOf'])
    commons = commons.rename(columns={'email_x': 'email', 'SamAccountName_x': 'SamAccountName', 'Title_x': 'Title'})
    commondropcol = ['email_y', 'SamAccountName_y', 'Title_y']
    fcommons = commons.drop(commondropcol, axis=1, level=None).reset_index(level=None, drop=True)
    newgrouplistcomp = pd.DataFrame(newgrouplist, index=None, columns=['name', 'MemberOf'])
    fcommonscol = pd.DataFrame(fcommons, index=None, columns=['name', 'MemberOf'])
    results = newgrouplistcomp[~(newgrouplistcomp.name + newgrouplistcomp.MemberOf).isin(fcommonscol.name + fcommonscol.MemberOf)]
    results = results.reset_index(level=None, drop=True)
    results2 = pd.merge(newgrouplist, results, how='inner', on=['name', 'MemberOf'])
    return results2


def rolemastsetcheckt(rollin):
    rolesetcol = ['name', 'email', 'User name', 'Title', 'AD RSN']
    rolechecktdf = pd.DataFrame(rollin, index=None, columns=rolesetcol, dtype=str, copy=False)
    rolechecktdf = rolechecktdf.rename(columns={'User name': 'SamAccountName', 'AD RSN': 'MemberOf'})
    return rolechecktdf



def adgroupsetup(groupdf):
    ogroupcol = ['name', 'email', 'SamAccountName', 'Title']
    ogroupdf = pd.DataFrame(groupdf, index=None, columns=ogroupcol, dtype=str, copy=False)
    a = pd.DataFrame(groupdf.MemberOf.str.split(',').tolist(), index=groupdf.name).stack()
    a = a.reset_index()[[0, 'name']]
    a.columns = ['MemberOf', 'name']
    fgroupdf = pd.DataFrame(a, index=None, columns=['name', 'MemberOf'])
    newgroups = pd.merge(fgroupdf, ogroupdf, how='left', sort=False)
    newgroups['Title'] = newgroups['Title'].apply(lambda x: x.lower().strip())
    newgroups['MemberOf'] = newgroups['MemberOf'].apply(lambda x: x.lower().strip())
    return newgroups


def outlierreport(outlierlist):
    outdf = pd.DataFrame(outlierlist, index=None, columns=['SamAccountName', 'email', 'Title', 'MemberOf'])
    outdf['SamAccountName'] = outdf['SamAccountName'].apply(lambda x: x.lower().strip())
    results = outdf[~outdf.SamAccountName.str.contains('account|a-|test|question|svc|wellness|sfs|jobs|amazon|apple|billing')]
    results2 = results[~results.SamAccountName.str.contains('emea|plano|toronto|gsd|hotmail|impression|internal|jira|marketing')]
    results3 = results2[~results2.SamAccountName.str.contains('webex|westcoast|social|payroll|nyc|sm|00|abuse|acm|vop|apac')]
    results4 = results3[~results3.SamAccountName.str.contains('window|yahoo|tracking|user|uspms|tos|team|svn|sts|strateg|eop|over')]
    results5 = results4[~results4.SamAccountName.str.contains('mr_london|member|macserver|lmstrain|limeadmin|jtech|health|eri')]
    results6 = results5[~results5.SamAccountName.str.contains('rsn|sales|rnmobile|rn_|offshore|open|netadmin|emi-info|compass|adimens')]
    results7 = results6[~results6.SamAccountName.str.contains('application|voucher|researchnow|mrpplymout|info|invoic|ighost')]
    results8 = results7[~results7.SamAccountName.str.contains('domain|donot|emiunsub|h&p|mobil|survey|sqa|spdhd')]
    results8 = results8.reset_index(level=None, drop=True)
    return results8



def genpactreport(rsnnewrps, outlist):
    gcsv = ereader(genpact, 0)
    gdf = pd.DataFrame(gcsv, index=None, columns=['TITLE', 'NAME', 'EMAIL'], dtype=str)
    gdf = gdf.rename(columns={'TITLE': 'Title', 'NAME': 'name', 'EMAIL': 'email'})
    gdf['email'] = gdf['email'].apply(lambda x: x.lower().strip())
    gdf['name'] = gdf['name'].apply(lambda x: x.lower().strip())
    radcsv = ereader(rsnAD, 1)
    raddf = pd.DataFrame(radcsv, index=None, columns=['Enabled', 'SamAccountName', 'Displayname', 'GivenName', 'sn',
                                                     'EmailAddress', 'Title', 'MemberOf'], dtype=str)
    raddf['name'] = raddf['GivenName'].map(str) + str(' ') + raddf['sn'].map(str)
    raddfdropcol = ['GivenName', 'sn']
    raddf.drop(raddfdropcol, axis=1, level=None, inplace=True)
    raddf = raddf.rename(columns={'EmailAddress': 'email'})
    raddf['email'] = raddf['email'].apply(lambda x: x.lower().strip())
    raddf['name'] = raddf['name'].apply(lambda x: x.lower().strip())
    gdfemail = gdf['email']
    gdfcomp = (raddf[raddf.email.isin(gdfemail)]).reset_index(level=None, drop=None)
    gdfcomp['email'].replace('nan', np.nan, inplace=True)
    gdfcomp.dropna(axis=0, how='any', subset=['email'], inplace=True)
    gdfcomp = gdfcomp.reset_index(level=None, drop=True)
    gdfcomp.drop('index', axis=1, level=None, inplace=True)
    results = pd.DataFrame(gdfcomp, index=None, columns=['Enabled', 'name', 'SamAccountName', 'Displayname',
                                                         'email', 'Title', 'MemberOf'])
    results = results.reset_index(level=None, drop=True)
    gennotfound = (gdf[~gdf.email.isin(results.email)]).reset_index(level=None, drop=True)
    writeresults(gennotfound, 'genpactNotFound.csv', ['Title', 'name', 'email'])
    outliermingen = (outlist[~outlist.email.isin(results.email)])
    outliers = outlierreport(outliermingen)
    admin = adminreport(raddf)
    return results, outliers, admin, raddf


def adminreport(rsnaddf):
    adminlist = pd.DataFrame(rsnaddf, index=None, columns=['Enabled', 'name', 'SamAccountName', 'Displayname',
                                                           'email', 'Title', 'MemberOf'], dtype=str)
    adminlist = adminlist[adminlist.SamAccountName.str.contains('a-')]
    adminremsvc = (adminlist[~adminlist.name.str.contains('svc|emea|ca-web|cloudera')]).reset_index(level=None, drop=True)
    return adminremsvc


def buildrsnad(rsnadcsv, newroler):
    rsncol = ['SamAccountName', 'EmailAddress',
              'Title', 'MemberOf']
    rsnaddf = pd.DataFrame(rsnadcsv, columns=rsncol, dtype=str)
    rsnaddf = rsnaddf.rename(columns={'EmailAddress': 'email'})
    rsnaddf['email'].replace('nan', np.nan, inplace=True)
    rsndropcol = ['email']
    rsnaddf.dropna(axis=0, how='any', subset=rsndropcol, inplace=True)
    rsnaddf['email'] = rsnaddf['email'].apply(lambda x: x.lower().strip())
    admatches = pd.merge(newroler, rsnaddf, how='inner', on='email')
    rsnadroledf = pd.merge(newroler, rsnaddf, how='outer', on='email')
    rsnadrolecol = ['name']
    admatches.dropna(axis=0, how='any', subset=rsnadrolecol, inplace=True)
    rsnadroledf.dropna(axis=0, how='any', subset=rsnadrolecol, inplace=True)
    admatches.sort_values(['name'], ascending=True, inplace=True, kind='mergesort', na_position='last')
    rsnadroledf.sort_values(['name'], ascending=True, inplace=True, kind='mergesort', na_position='last')
    admatches = admatches.reset_index(level=None, drop=True)
    rsnadroledf = rsnadroledf.reset_index(level=None, drop=True)
    admatchesemail = admatches['email']
    rsnadroleeemail = rsnadroledf['email']
    adnomatchdf = (newroler[~newroler.email.isin(admatchesemail)]).reset_index(level=None, drop=True)
    outliers = rsnaddf[~rsnaddf.email.isin(rsnadroleeemail)]
    return rsnadroledf, outliers


def nomatchemailtotemplate(hremailmatchadresults):
    rolecsv = ereader(rolemaster, 0)
    remaildf = rolecsv['Email Address:']
    rnamedf = dfformatter('name', (rolecsv['NameConcat']))
    rolenameemail = pd.DataFrame(rnamedf, columns=['name'])
    rolenameemail['name'].replace('', np.nan, inplace=True)
    rolenameemail.dropna(axis=0, how='any', inplace=True)
    rolenameemail['email'] = remaildf
    rolenameemail['name'] = rolenameemail['name'].apply(lambda x: x.lower().strip())
    rolenameemail['email'] = rolenameemail['email'].apply(lambda x: x.lower().strip())
    roleemaildf = rolenameemail['email']
    rolenamedf = rolenameemail['name']
    hrtoadnomatchdf = pd.DataFrame(hremailmatchadresults)
    commonemail = hrtoadnomatchdf.merge(rolenameemail, on=['email'])
    commonemail.drop('name_y', axis=1, level=None, inplace=True)
    commonemail = commonemail.rename(columns={'name_x': 'name'})
    fcommonemail = pd.DataFrame(commonemail, columns=['name', 'email'])
    commonname = hrtoadnomatchdf.merge(rolenameemail, on=['name'])
    commonname.drop('email_x', axis=1, level=None, inplace=True)
    commonname = commonname.rename(columns={'email_y': 'email'})
    commons = [commonname, fcommonemail]
    comcat = pd.concat(commons).reset_index(level=None, drop=True)
    comcat.sort_values(['name'], axis=0, ascending=True, inplace=True, kind='mergesort')
    comcat.drop_duplicates(subset='email', keep='first', inplace=True)
    nopeanuts = comcat[comcat['email'].str.contains("@researchnow.com")]
    return nopeanuts



def writeresults(returnfile, filename, colstouse):
    returnfile.to_csv(filename, sep=',', usecols=colstouse, header=True, index=False, dtype=str,
                      skip_blank_lines=True, encoding='utf-8')


# -------------------MAIN--------------------------------------------------------#
if __name__ == '__main__':
    starttime = time.time()
    hremailmatchad()
    endtime = time.time() - starttime
    print('Duration: ' + str(endtime))
