import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import os
from time import sleep

url_drs = 'http://dipbt.bundestag.de/extrakt/bt/drs/WP19/'
url_pp = 'http://dipbt.bundestag.de/extrakt/bt/plpr/WP19/'
data_path_drs = '../data/Bundestag/Drucksachen/drs19-data/'
data_path_pp = '../data/Bundestag/Plenarprotokolle/pp19-data/'


def create_crawler(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'
                ' Chrome/74.0.3729.169 Safari/537.36'}
    result = requests.get(url, headers=headers)
    src = result.content
    soup = BeautifulSoup(src, 'lxml')
    return soup


def get_info_list(url):
    scraper = create_crawler(url)
    even = scraper.find_all('tr', class_='evenRow')
    odd = scraper.find_all('tr', class_='oddRow')
    data_list = [None] * (len(even) + len(odd))
    data_list[::2] = odd
    data_list[1::2] = even
    return data_list


def download_wp_19(url, data_path):
    data_list = get_info_list(url)
    names = []
    urls = []
    for data_point in data_list:
        pdf_url = data_point.find('a')['href']
        pdf_name = data_point.find('a').text
        pdf_name = re.sub('/', '_', pdf_name)
        if pdf_url.endswith('.pdf'):
            urls.append(pdf_url)
            names.append(pdf_name)

    names_urls = zip(names, urls)
    for n, u in names_urls:
        nr = os.path.join(data_path, n)
        os.system(f'wget -O {nr}.pdf {u}')
        sleep(0.1)


def pdf_to_text(data_path):
    os.chdir(data_path)
    os.system('for file in *.pdf; do pdftotext -layout "$file"; done')


def clean_data(txt_file_path):
    with open(txt_file_path, 'r+') as f:
        text = f.read()
        text = text.lower()
        text = re.sub('[.,?!():;_"“‘’„”«»]', ' ', text)
        text = re.sub('\s+', ' ', text)
        f.seek(0)
        f.write(text)
        f.truncate()


def combine_info_drs():
    final = []
    tmp = []
    tmp2 = []
    data_list = get_info_list(url_drs)
    for data_point in data_list:
        for td in data_point:
            tmp.append(td.text)
        try:
            # WAHLPERIODE
            tmp2.append('19')
            # DOKUMENTART
            tmp2.append('DRUCKSACHEN')
            # DRS_TYP
            tmp2.append(tmp[2])
            # NR
            tmp2.append(tmp[0])
            # DATUM
            tmp2.append(tmp[3])
            # TITEL
            tmp2.append(tmp[5])
            # TEXT
            filename = re.sub('/', '_', tmp[0])
            filename = f'{filename}.txt'
            filename = os.path.join(data_path_drs, filename)

            clean_data(filename)
            with open(filename, 'r') as txt:
                tmp2.append(txt.read())
                tmp2.append(len(txt.read()))

            d, m, y = tmp[3].split('.')
            # Day
            tmp2.append(d)
            # Month
            tmp2.append(m)
            # Year
            tmp2.append(y)
        except:
            pass

        final.append(tmp2)
        # Reset tmp
        tmp, tmp2 = [], []
    return final


def combine_info_pp():
    final = []
    tmp = []
    tmp2 = []
    data_list = get_info_list(url_pp)
    for data_point in data_list:
        for td in data_point:
            tmp.append(td.text)
        try:
            # WAHLPERIODE
            tmp2.append('19')
            # NR
            tmp2.append(tmp[0])
            # DATUM
            tmp2.append(tmp[3])
            # TEXT
            filename = re.sub('/', '_', tmp[0])
            filename = f'{filename}.txt'
            filename = os.path.join(data_path_pp, filename)

            clean_data(filename)
            with open(filename, 'r') as txt:
                tmp2.append(txt.read())
                # This one is wrong only counts lines but gets overridden in dfs_cleanup anyways
                tmp2.append(len(txt.read()))

            d, m, y = tmp[3].split('.')
            # Day
            tmp2.append(d)
            # Month
            tmp2.append(m)
            # Year
            tmp2.append(y)
        except:
            pass

        final.append(tmp2)
        # Reset tmp
        tmp, tmp2 = [], []
    return final


def create_df_from_txt_files(dir_path):
    pattern = re.compile(r'(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|1[012]|[1-9])[- /.](20[0-9][0-9])')
    for i in range(1, 100000):
        name = ''
        try:
            with open(f'19_{i}.txt', 'r') as f:
                data = f.read()
                match = pattern.search(data)
                match = match.groups(0)
                d, m, y = match
                name = f'{y}_{m}_{d}'
            os.rename(f'19_{i}.txt', f'{name}.txt')
        except:
            print(f'{i} doesnt exist')


def to_df(info_list, drs=True):
    if drs:
        df = pd.DataFrame(info_list, columns=['wahlperiode', 'dokumentart', 'drs_typ', 'nr', 'datum', 'titel', 'text',
                                              'wordcount', 'day', 'month', 'year'])
    else:
        df = pd.DataFrame(info_list, columns=['wahlperiode', 'nr', 'datum', 'text', 'wordcount', 'day', 'month', 'year'])
    return df


def load_df(path):
    return pd.read_json(path)


def save_df(df, name):
    df.to_json(name)


def main():
    #download_wp_19(url_pp, data_path_pp)
    #pdf_to_text(data_path_pp)
    # final = combine_info_pp()
    # final = to_df(final, drs=False)
    # save_df(final, 'plenar_19_df.json')
    create_df_from_txt_files('../thesis/')

if __name__ == '__main__':
    main()
