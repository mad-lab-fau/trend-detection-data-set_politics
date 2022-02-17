import os
import pandas as pd
import xml.etree.ElementTree as ET
import re

data_dir_path_plenar = "../data/Bundestag/Plenarprotokolle/"
data_dir_path_druck = "../data/Bundestag/Drucksachen/drs18-data"


def txt_data_to_df(dir_path):
    columns = ['text', 'wordcount', 'month', 'year']
    tmp = []
    for dirpath, dirnames, filenames in os.walk(dir_path):
        for filename in [f for f in filenames]:
            path = os.path.join(dirpath, filename)
            year, month, day = str(filename).split('_')
            text = ''
            wordcount = 0
            with open(path, 'r') as f:
                text = f.read()
                text = text.lower()
                text = re.sub('[.,?!():;_"“‘’„”«»]',' ', text)
                text = re.sub('\s+', ' ', text)
                wordcount = len(text.split(' '))

            data = [text, wordcount, month, year]
            data_dict = dict(zip(columns, data))
            tmp.append(data_dict)

    df = pd.DataFrame(tmp)
    # df = df.astype({'article': 'int16', 'month': 'int8', 'year': 'int16'})

    return df


def xml_data_to_df(dir_path):
    tmp = []
    for dirpath, dirnames, filenames in os.walk(dir_path):
        for filename in [f for f in filenames if f.endswith(".xml")]:
            path = os.path.join(dirpath, filename)
            xml_data = open(path, 'r').read()
            root = ET.XML(xml_data)

            data = []
            tags = []
            for i, child in enumerate(root):
                if child.tag != 'K_URHEBER' and child.tag != 'P_URHEBER':
                    tags.append(child.tag)
                else:
                    tags.append(child.text)
                data.append(child.text)
            dict1 = dict(zip(tags, data))
            tmp.append(dict1)

    # Create dataframe with names as specified above
    df = pd.DataFrame(tmp)

    # Make all entries lower case
    df = df.apply(lambda x: x.astype(str).str.lower())

    # Make all columns lower case
    df.columns = df.columns.str.lower()

    # next two lines only work if the text column is actually called 'text' in
    # the first place (in the downloaded docs)
    # Replace any punctuations except forward slashes in the Text field
    df.text = df.text.str.replace('[.,?!():;_"“‘’„”«»]', ' ')

    # Replace the newline and Tab characters in the Text column
    df.text = df.text.str.replace('\s+', ' ', regex=True)

    # Make additional column with wordcount of Text column
    df['wordcount'] = df.text.str.count(' ') + 1

    # Change 'datum' column from string to datetime
    df['datum'] = pd.to_datetime(df['datum'], dayfirst=True)

    # Create day / month / year columns for each date
    df['day'] = df['datum'].dt.day
    df['month'] = df['datum'].dt.month
    df['year'] = df['datum'].dt.year

    return df


def load_df(path):
    return pd.read_json(path)


def save_df(df, name):
    df.to_json(name)


def get_buzzwords_list(buzzword_path):
    # load list of buzzwords from data section
    buzzlist = []
    with open(buzzword_path) as buzz:
        for line in buzz:
            buzzlist.append(line.rstrip('\n').lower())
    return buzzlist


def add_buzzword_columns_to_df(dataframe, buzzwordlist):
    df = dataframe
    buzzlist = buzzwordlist
    # Create a column and count for each word in the buzzwordlist
    # Weird way of using regex i guess but was the only way to get it to work
    for pattern in buzzlist:
        df[pattern] = df['text'].str.findall(r'\b'+r''.join(pattern)+r'\b')
        df[pattern] = df[pattern].str.len()
    return df


def concat_dfs(df_list, savename):
    final = pd.concat(df_list)
    final.to_json(savename)


def main():
    years = [14, 15, 16, 17, 18]
    for i in years:
         df = xml_data_to_df(f'../data/Bundestag/Drucksachen/drs{i}-data/')
         save_df(df, f'test{i}.json')
    df = txt_data_to_df('../data/Bundestag/Drucksachen/drs19-data')
    save_df(df, 'drs19_right.json')


if __name__ == '__main__':
    main()

