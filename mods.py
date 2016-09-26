#!/anaconda3/bin/python

import requests, sys, re,json
from bs4 import BeautifulSoup
from time import sleep
from datetime import datetime
from pymongo import MongoClient
from xmltodict import parse

log_template="{url}\t{status}\t{date}\n"
db = MongoClient("dsl_search_mongo",27017)
url_template="https://www.gpo.gov/fdsys/search/search.action?sr={0}&originalSearch=collection:CHRG&st=collection:CHRG&ps=100&na=__congressnum&se=__{1}true&sb=dno&timeFrame=&dateBrowse=&govAuthBrowse=&collection=&historical=true"

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
        if re.match('^CHRG*',end_url,re.IGNORECASE):
            valid_ids.append(end_url.split('.')[0])
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
    temp=xmlURL.replace(tag,'{0}')
    temp_tag=tag.replace('-err','').replace('-ptERR','').replace('-pterr','')
    temp_tag=temp_tag.split('-pt')[0]
    r = s.get(temp.format(temp_tag))
    log.write(log_template.format(url=xmlURL,status=r.status_code,date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    try:
        load_xml_json(r,tag)
    except:
        try:
            try_url="https://www.gpo.gov/fdsys/granule/{0}/{1}/mods.xml"
            r = s.get(try_url.format(tag.split('-v')[0],tag))
            load_xml_json(r,tag) 
        except:
            log.write("ERROR: {0} {1}\n".format(tag,xmlURL))

def load_xml_json(r, tag):
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

    data = {'TAG':tag,'HELD_DATE':helddate,'URL':url,'PDF':pdf,'NAMES':namesList,'CONG_MEMBERS':congmemberList,'ORIGIN_INFO':origininfoList,'EXTENSIONS':extensionList,'TITLE_INFO':titleinfoList,'IDENTIFIER':identifier,'CONG_COMMITTEE':congcommitteeList,'WITNESS':witnessList}
    db.congressional.hearings.save(data)

#def load_xml_json(r,tag):
#    data = parse(r.text)
#    data["tag"]=tag
#    x = json.loads(json.dumps(data).replace("@",'').replace("#",''))
#    db.congressional.hearings.save(x)

if __name__ == "__main__":
    v_ids=[]
    #db = MongoClient("dsl_search_mongo",27017)
    s = requests.Session()
    modsURL_template = "https://www.gpo.gov/fdsys/pkg/{0}/mods.xml"
    log = open(sys.argv[3],'w')
    for congress in range(int(sys.argv[1]),int(sys.argv[2])):
        v_ids = v_ids + get_ids(s,url_template,congress,log)
    for itm in v_ids:
        if db.congressional.hearings.find({'tag':itm}).count()<1:
            modsParser(s,itm,modsURL_template.format(itm),log)
    log.write(str(v_ids))
    log.close()
