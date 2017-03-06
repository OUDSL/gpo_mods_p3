#!/anaconda3/bin/python

import requests, sys, re,json
from bs4 import BeautifulSoup
from time import sleep
from datetime import datetime
#from pymongo import MongoClient
from xmltodict import parse
from urllib.parse import urlparse , parse_qs

headers ={"Content-Type":"application/json","Authorization":"Token {0}"}
log_template="{url}\t{status}\t{date}\n"
#db = MongoClient("dsl_search_mongo",27017)
base_url ="https://www.gpo.gov"
url_template= base_url + "/fdsys/search/search.action?sr={0}&originalSearch=collection:CHRG&st=collection:CHRG&ps=100&na=__congressnum&se=__{1}true&sb=dno&timeFrame=&dateBrowse=&govAuthBrowse=&collection=&historical=true"
#modsURL_template = "https://www.gpo.gov/fdsys/pkg/{0}/mods.xml"
def get_chrg_ids(s,url_template,log,page=1,congress=99):
    try:
        r=s.get(url_template.format(page,congress))
        soup=BeautifulSoup(r.text,'html.parser')
    except:
        sleep(15)
        r=s.get(url_template.format(page,congress))
        soup=BeautifulSoup(r.text,'html.parser')
    log.write(log_template.format(url=url_template.format(page,congress),status=r.status_code,date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    links=[]
    for link in soup.findAll('a'):
        if link.get('href'):
            links.append(link.get('href'))
    valid_ids=[]
    for link in links:
        end_url=link.split('/')[-1]
        if re.match('^pagedeta*',end_url,re.IGNORECASE):
            link_parse=urlparse(link)
            query=parse_qs(link_parse.query)
            tag=query['packageId'][0]
            rd = s.get(base_url + link)
            soupd = BeautifulSoup(rd.text,'html.parser')
            mods_url=""
            for linkd in soupd.findAll('a'):
                if linkd.get('href'):
                    temp=linkd.get('href')
                    if temp.split('/')[-1]=="mods.xml":
                        mods_url=temp
                        break
            valid_ids.append({'tag':tag,'mods_url':mods_url})
    return valid_ids

def get_ids(s,url_template,congress,log):
    cum_ids=[]
    for itm in range(1,1000):
        ids = get_chrg_ids(s,url_template,log,page=itm,congress=congress)
        if ids==[]:
            break
        cum_ids = cum_ids + ids
    return cum_ids


def modsParser(s,tag,xmlURL,log):
    try:
        r = s.get(xmlURL)
    except:
        sleep(15)
        r = s.get(xmlURL)
    log.write(log_template.format(url=xmlURL,status=r.status_code,date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    try:
        load_xml_json(r,tag,xmlURL)
    except:
        log.write("ERROR: {0} {1}\n".format(tag,xmlURL))

def load_xml_json(r, tag,mods_url):
    soup = BeautifulSoup(r.text,'html.parser')
    namesList=[]
    congmemberList=[]
    origininfoList=[]
    languageList=[]
    extensionList=[]
    titleinfoList=[]
    url=""
    pdf=""
    identifier=""
    congcommitteeList=[]
    witnessList=[]
    helddate=""

    for x in soup('name'):
        if "namepart" in json.dumps(parse(str(x))):
            namesList.append(json.loads(json.dumps(parse(str(x))['name']).replace("@",'').replace("#",'')))

    for x in soup('congmember'):
        congmemberList.append(json.loads(json.dumps(parse(str(x))['congmember']).replace("@",'').replace("#",'')))

    for x in soup('origininfo'):
        origininfoList.append(json.loads(json.dumps(parse(str(x))['origininfo']).replace("@",'').replace("#",'')))

    for x in soup('language'):
        languageList.append(json.loads(json.dumps(parse(str(x))['language']).replace("@",'').replace("#",'')))

    for x in soup('extension'):
        extensionList.append(json.loads(json.dumps(parse(str(x))['extension']).replace("@",'').replace("#",'')))

    for x in soup('titleinfo'):
        if "@type" not in json.dumps(parse(str(x))):
            titleinfoList.append(json.loads(json.dumps(parse(str(x))['titleinfo']).replace("@",'').replace("#",'')))

    for x in soup('url'):
        if x['displaylabel'] == "HTML rendition":
            url=x.getText()
        if x['displaylabel'] == "PDF rendition":
            pdf = x.getText()

    for x in soup('identifier'):
        if "uri" in json.dumps(parse(str(x))):
            identifier=json.loads(json.dumps(parse(str(x))['identifier']).replace("@",'').replace("#",''))

    for x in soup('congcommittee'):
        congcommitteeList.append(json.loads(json.dumps(parse(str(x))['congcommittee']).replace("@",'').replace("#",'')))

    for x in soup('witness'):
        witnessList.append(json.loads(json.dumps(parse(str(x))['witness']).replace("@",'').replace("#",'')))

    for x in soup('extension'):
        if x.helddate:
            helddate=parse(str(x.helddate))['helddate']

    if helddate=="":
        for x in soup('origininfo'):
            helddate =json.dumps(parse(str(x.dateissued))['dateissued']['#text'])

    data = {'TAG':tag,'MODS_URL':mods_url,'HELD_DATE':helddate,'URL':url,'PDF':pdf,'NAMES':namesList,'CONG_MEMBERS':congmemberList,'ORIGIN_INFO':origininfoList,'EXTENSIONS':extensionList,'TITLE_INFO':titleinfoList,'IDENTIFIER':identifier,'CONG_COMMITTEE':congcommitteeList,'WITNESS':witnessList}
    _save_hearing_data(data) #db.congressional.hearings.save(data)

def _save_hearing_data(data):
    url ="https://cc.lib.ou.edu/api-dsl/data_store/data/congressional/hearings/"
    req = requests.post(url,data=json.dumps(data),headers=headers)
    if not req.status_code < 400:
        raise Exception("Error Saving to API")
def _check_tag_exists(tag):
    url ="https://cc.lib.ou.edu/api-dsl/data_store/data/congressional/hearings/.json?query={'filter':{'TAG':'%s'}}" % (tag)
    req= requests.get(url)
    data=req.json()
    if data['count']>0:
        return True
    else:
        return False

if __name__ == "__main__":
    s = requests.Session()
    log = open(sys.argv[3],'w')
    headers["Authorization"]= "Token {0}".format(sys.argv[4])
    results=[]
    for congress in range(int(sys.argv[1]),int(sys.argv[2])):
        counter=0
        for itm in get_ids(s,url_template,congress,log):
            counter+=1
            if not _check_tag_exists(itm['tag']): 
                modsParser(s,itm['tag'],itm['mods_url'],log)
        results.append("Congress: {0}, Processed: {1}".format(congress,counter))
    log.write(str(results))
    log.close()
