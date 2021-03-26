import os
import zipfile
import requests
import sys
import csv
import concurrent.futures


base_path = os.path.dirname(os.path.abspath(__file__))
download_path = os.path.join(base_path, '')  # add path to desired download directory.
zip_extract_path = os.path.join('')          # add path to desired unzip directory.


def unpacker(list):
    """Iterates through URLs in target_list, passes them to the download_file() and unzip() function
    
    Parameters: 
    list (str): set to target_list variable which refers to value lists within suffix_dict.
    
    """
    for target_url in Collector.target_list:
        filename = download_file(target_url)
        try:
            unzip(filename)
            os.remove(filename)

        except (TypeError, AttributeError):
            pass


def download_file(url):
    """Downloads file. Removes prefix URL before 14 digit numeric date.
    
    Parameters:
    url (str): Downloads from the URL contained within the variable passed to it within the upacker() function.

    """
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
        executor.map(download_file, Collector.target_list)



def unzip(path):
    """ unzips file to path specified. 
    
    Parameters:
    path (str): Path to unzip directory. 
    
    """
    print('unzipping:', path)
    zip_ref = zipfile.ZipFile(path, 'r')
    zip_ref.extractall(zip_extract_path)
    zip_ref.close()



class Collector:
    """Parses, processes, and appends URLs to lists for download.
    
    Parameters:
    target_suffix (str): the suffix starting after the date portion of the desired GDELT file to process.
    target_list: The list to add url to for subsequent download.

    """

    def __init__(self, target_suffix, target_list):
        self = Collector
        self.target_suffix = target_suffix
        self.target_list = target_list


    def target_collector(self, feeds, start_date):
        """Reads each line for feed in feeds. If line has one of the six corresponding gdelt suffixes, beginning from desired start date,
        a dictionary key is created for it and parsed URL is added as key value. 

        Parameters:
        feeds (str): The gdelt masterfile URLs to be parsed and downloaded from.
        start_date (str): numerical 14 digit string formatted as YEAR MONTH DAY 00 (hour) 00 (min) 00 (sec) e.g.'20150218224500'.
        """
        
        for feed in feeds:

            res = requests.get(feed)
            resStr = str(res.content)


            for line in resStr.split("\\n"):
                if not line:
                    continue


                suffix_dict = {}    # stores key_suffix and associated target_url(s)

                key_suffix = ('.export.CSV.zip',
                            '.gkg.csv.zip',
                            '.mentions.CSV.zip', 
                            '.translation.export.CSV.zip',
                            '.translation.gkg.csv.zip',
                            '.translation.mentions.CSV.zip'
                            )


                if line.endswith(key_suffix):               # checks if line being read ends in one of the suffixes in 'key_suffix' tuple
                    target_url = line.split(' ')[2]         # splits the line at the second ' ' (white space) and assigns remainder of line as variable 'target_url'
                    suffix = target_url[52:]                # slices 'target_url' at index value 52 and assigns remainder to variable 'suffix' 
                                                            
                    if suffix not in suffix_dict:           
                        suffix_dict[suffix] = []            # if suffix not in suffix_dict, a key of suffix is created and it's value is set to an empty list []
                    if suffix in suffix_dict.keys():        # if suffix already a key in suffix_dict, it skips it
                        pass

                    parsed_date = target_url[37:50]         # line is then parsed for the numeric portion corresponding to a date, number assigned to 'parsed_date' variable
                    if parsed_date < start_date:            # if parsed date before specified start date, it skips it
                        pass
                            
                    else:
                        Collector(target_suffix=suffix, target_list=suffix_dict[suffix])    # sets targets for Collector instance
                        Collector.target_list.append(target_url)                            # Collector appends 'target_url' to 'target_list'
                        unpacker(Collector.target_list)                                     # calls unpacker function on 'target_list'



if __name__ == "__main__":
    if len(sys.argv) > 1:
        pass

    
    # feeds contains URLs to be scraped
    feeds = [
            'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt',
            'http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt'
            ]


    # start_date is formatted as YEAR MONTH DAY 00 (hour) 00 (min) 00 (sec). GDELT feeds update every 15 minutes  
    # start_date = '20150218224500'    # corresponds to first date in 'http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt', uncomment to collect all v2 GDELT data since first availale.
    start_date = '20210324000000'
    

    Collector.target_collector(self=Collector, feeds=feeds, start_date=start_date)
 
