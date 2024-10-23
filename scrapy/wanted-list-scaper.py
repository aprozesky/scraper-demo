import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import re
import os
from datetime import datetime
import urllib.request

# this gets the directory where this script is running from.
# By default it will look for and write the csv to the same location
file_path = os.path.realpath(__file__).replace('wanted-list-scaper.py', '')
output_path = file_path
table_name = "wanted_list.csv"
csv_exists = os.path.exists(output_path + table_name)

img_dir = output_path + 'photos/'
if not os.path.exists(img_dir):
    os.makedirs(img_dir)

# will only add records if npis is not already in csv
if csv_exists:
    npis_list = pd.read_csv(output_path+table_name)['npis'].to_list()
    npis_list = [str(x) for x in npis_list]
else:
    npis_list = []

base_url = 'https://www.saps.gov.za/crimestop/wanted/'

records_added = 0
img_error = []

class wanted_spider(scrapy.Spider):
    name = "wanted_spider_saps"

    def start_requests(self):
        start_url = base_url + "list.php"
        yield scrapy.Request(url = start_url, callback = self.parse_list)
 
    # parse list of wanted persons
    def parse_list(self, response):
        links_to_people = response.css('.cust-td-border a::attr(href)').extract()
        links_to_people = [base_url +f'{x}' for x in links_to_people]
        
        for url in links_to_people:
            yield response.follow(url = url, callback = self.parse_people)
      
    # parse details of person
    def parse_people(self, response):
      
        full_table = response.xpath('/html/body/div[5]/div/div/div/div[2]/table[1]')
        
        npis = full_table.get().split('src="')[1].split('</td>')[0]
        npis = re.findall(r".+?>\s?(.+?)\s", npis)
        npis = npis[0]
        npis = npis.replace('/', '-')
       
        if npis not in npis_list:
            
            global records_added 
            records_added+=1
            
            surname_prefix_list = ['Du', 'Le', 'De', 'Der', 'Den', 'Van', 'Jansen']

            full_name = response.css('.panel-body > h2::text').get().title()
            name_list = full_name.split(' ')
            first_name = name_list[0]
            last_name = name_list[-1]
            name_list.pop(0)
            name_list.pop(-1)
            if len(name_list) > 0:
                for name in reversed(name_list):
                    if name in surname_prefix_list:
                        last_name = name + ' ' + last_name
                        name_list.remove(name)
                middle_names = ' '.join(name_list)
                
                middle_names = middle_names.strip() 
            else:
                middle_names = ''

            status1 = response.css('.panel-body font[color="blue"]::text').getall()
            status2 = response.css('.panel-body font[color="red"]::text').getall()
            status = ', '.join(status1 + status2)
            img_url = full_table.get().split('src="')[1].split('"')[0]
            img_url = base_url + img_url

            try:
                urllib.request.urlretrieve(img_url, img_dir + npis + '.jpg')
            except urllib.error.HTTPError:
                img_error.append(full_name + ' ' + npis + ' ' + img_url)
            
            crime_circumstance = full_table.css('tr:nth-child(2) > td:nth-child(2) > p ::text').getall()
            to_remove = ['\r', '\n', '\t', '\xa0', ',', ';']
            for char in to_remove:
                crime_circumstance = [item.replace(char, '') for item in crime_circumstance]
            crime_circumstance = ''.join(crime_circumstance)
            
            # convert info in table to dictionary for easy access, some tables have additional fields
            rows = full_table.css('tr')
            
            table = {}
            for i, row in zip(range(len(rows) - 2), rows):
                table[rows[i].css('b::text').get().strip(': ')] = rows[i].css('td::text').get()
                
            if '(' in table['Station']:
                station = table['Station'].split(' (')[0]
                province = table['Station'].split(' (')[1].split(')')[0]
            else:
                station = table['Station']
                province = ''
            case_no = re.findall(r"\d+(?:/|-)\d{1,2}(?:/|-)\d{4}", table['Case Number'])
            case_no = ' '.join(case_no).strip()
            warrant_no = re.findall(r"\d{4}/W/\d+", table['Case Number'])
            warrant_no = ' '.join(warrant_no).strip()
            
            alias_nan_list = ['0', 'Unkown', 'n/a', 'N/A']
            if 'Aliases' not in table:
                alias = ''
            elif (table['Aliases'] == '0') or (table['Aliases'] == 'Unknown'):
                alias = ''
            else:
                alias = table['Aliases']

            wanted_dict['first_name'].append(first_name)
            wanted_dict['middle_names'].append(middle_names)
            wanted_dict['last_name'].append(last_name)
            wanted_dict['status'].append(status)
            wanted_dict['img_url'].append(img_url)
            wanted_dict['npis'].append(npis)
            wanted_dict['crime'].append(table['Crime'])
            wanted_dict['crime_circumstance'].append(crime_circumstance)
            wanted_dict['crime_date'].append(table['Crime Date'])
            wanted_dict['gender'].append(table['Gender'])
            wanted_dict['station'].append(station)
            wanted_dict['province'].append(province)
            wanted_dict['case_no'].append(case_no)
            wanted_dict['warrant_no'].append(warrant_no)
            wanted_dict['io'].append(table['Investigating Officer'])
            wanted_dict['alias'].append(alias)
            wanted_dict['date_scraped'].append(datetime.now().strftime("%Y/%m/%d %H:%M"))
            wanted_dict['url'].append(response.url)
            
            for key in wanted_dict.keys():
               if len(wanted_dict[key]) != len(wanted_dict['first_name']):
                   print('EORROR AT', full_name)
                   exit()


wanted_dict = {'first_name' : [],
               'middle_names' : [],
               'last_name' : [],
               'status' : [],
               'img_url' : [],
               'npis' : [],
               'crime' : [],
               'crime_circumstance' : [],
               'crime_date' : [],
               'alias' : [],
               'gender' : [],
               'station' : [],
               'province' : [],
               'case_no' : [],
               'warrant_no' : [],
               'io' : [],
               'date_scraped' : [],
               'url' : []}

process = CrawlerProcess()
process.crawl(wanted_spider)
process.start()

wanted_df = pd.DataFrame(wanted_dict)
records_added = wanted_df.shape[0]

if records_added > 0:
    wanted_df.to_csv(output_path+table_name, mode='a', header=not csv_exists, index=False)
    
print('\n\nNumber of records added: ', records_added)
if len(img_error) == 0:
    print('\nAll images downloaded successfully.')
else:
    print('\nErrors encountered when trying to download images. Try manual download for the following persons:\n')
    for name in img_error:
        print(name)