import os
import zipfile
import requests
import sys
import datetime
import csv
import concurrent.futures

base_path = os.path.dirname(os.path.abspath(__file__))
download_path = os.path.join(base_path, '')
zip_extract_path = os.path.join('')


def download_file(url):
    print('downloading', url)
    local_filename = url.split('/')[-1]
    local_filename = os.path.join(download_path, local_filename)
    r = requests.get(url, stream=True)
    if not r.ok:
        print(f'request returned with code {r.status_code}')
        return None
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: 
                f.write(chunk)
    return local_filename

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(download_file, get.url_targets, get.gkg_targets, get.mentions_targets, get.translation_url_targets, 
                                    get.translation_gkg_targets, get.translation_mentions_targets) 



def unzip(path):
    print('unzipping:', path)
    zip_ref = zipfile.ZipFile(path, 'r')
    zip_ref.extractall(zip_extract_path)
    zip_ref.close()


# start date specified as string in the formay of YEAR, MONTH, DAY, 00:00:00 hour in 15 minute intervals 
def get(start_date='20210323000000'):


    feeds = [
            'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt',
            'http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt'
        ]


    for feed in feeds:

        res = requests.get(feed)
        url = None
        gkg_url = None
        mentions_url = None
        resStr = str(res.content)
        url_targets = []  
        gkg_targets = []
        mentions_targets = []
        translation_url_targets = []
        translation_gkg_targets = []
        translation_mentions_targets = []
        

        for line in resStr.split("\\n"):
            if not line:
                continue
               
            # each of the following 'if' blocks parses for specific date and adds that url type to it's specific target list above
            if line.count('.export.CSV.zip') > 0:
                url = line.split(' ')[2]  
                parsed_date = url[37:50]  # slices url string to parse for date portion
                if parsed_date < start_date:
                    print('skipping:', parsed_date)  # to confirm that unwanted dates are skipped over
                    pass
                else:
                    url_targets.append(url)
                    print('appended', url, 'to url_targets list') # to confirm that wanted dates are added to download list
         

            if line.count('gkg.csv.zip') > 0:   
                gkg_url = line.split(' ')[2]
                parsed_date = gkg_url[37:50]
                if parsed_date < start_date:
                    print('skipping:', parsed_date)
                    pass
                else:
                    gkg_targets.append(gkg_url)
                    print('appended', gkg_url, 'to gkg_targets list')
                    

            if line.count('mentions.CSV.zip') > 0:
                mentions_url = line.split(' ')[2]
                parsed_date = mentions_url[37:50]
                if parsed_date < start_date:
                    print('skipping:', parsed_date)
                    pass
                else: 
                    mentions_targets.append(mentions_url)
                    print('appended', mentions_url, 'to mentions_targets list')

            
            if line.count('translation.export.CSV.zip') > 0:
                translations_url = line.split(' ')[2]
                parsed_date = translations_url[37:50]
                if parsed_date < start_date:
                    print('skipping:', parsed_date)
                    pass
                else:
                    translation_url_targets.append(translations_url)
                    print('appended', translations_url, 'to translation_url_targets list')

            
            if line.count('translation.gkg.csv.zip') > 0:
                translations_gkg_url = line.split(' ')[2]
                parsed_date = translations_gkg_url[37:50]
                if parsed_date < start_date:
                    print('skipping:', parsed_date)
                    pass
                else:
                    translation_gkg_targets.append(translations_gkg_url)
                    print('appended', translations_gkg_url, 'to translation_gkg_targets list')

            if line.count('translation.mentions.CSV.zip') > 0:
                translations_mentions_url = line.split(' ')[2]
                parsed_date = translations_mentions_url[37:50]
                if parsed_date < start_date:
                    print('skipping:', parsed_date)
                    pass
                else:
                    translation_mentions_targets.append(translations_mentions_url)
                    print('appended', translations_mentions_url, 'to translation_mentions_targets list')



        for url in url_targets: 
            filename = download_file(url)
            try:
                unzip(filename)
                os.remove(filename)

            except (TypeError, AttributeError):
                pass


        for url in gkg_targets:
            filename = download_file(url)
            try:
                unzip(filename)
                os.remove(filename)

            except (TypeError, AttributeError):
                pass


        for url in mentions_targets:
            filename = download_file(url)
            try:
                unzip(filename)
                os.remove(filename)

            except (TypeError, AttributeError):
                pass

        
        for url in translation_url_targets:
            filename = download_file(url)
            try:
                unzip(filename)
                os.remove(filename)

            except (TypeError, AttributeError):
                pass


        for url in translation_gkg_targets:
            filename = download_file(url)
            try:
                unzip(filename)
                os.remove(filename)

            except (TypeError, AttributeError):
                pass


        for url in translation_mentions_targets:
            filename = download_file(url)
            try:
                unzip(filename)
                os.remove(filename)

            except (TypeError, AttributeError):
                pass



if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass
    get()


