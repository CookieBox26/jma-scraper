"""
Scrapes weather data from the JMA (Japan Meteorological Agency) historical weather data search.
https://www.data.jma.go.jp/stats/etrn/index.php

Ex. python scrape.py 2025-09-01 2025-12-31

Running this script generates cache and data files as follows:

./
├─ scrape.py  # This script
├─ cache/  # Cache directory for web pages (created if not exists)
├─ tar_gz/  # Directory for monthly compressed cache files (created if not exists)
└─ out/
    ├ weather_japan_master.csv  # Output file (location master)
    └ weather_japan_org.csv  # Output file (weather data)

If a compressed cache file exists, it reads from the cache instead of accessing the web page.
"""
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
import tarfile


cache_path = Path('cache')  # Cache directory for web pages
tar_gz_path = Path('tar_gz')  # Directory for monthly compressed cache files
out_path = Path('out')
out_file_master = out_path / 'weather_japan_master.csv'  # Output file (location master)
out_file = out_path / 'weather_japan_org.csv'  # Output file (weather data)
prec_blocks_target = [  # List of target prec_name::block_name
    # The following lists prefectural capitals.
    # * Saitama: Saitama city is AMeDAS only, not an observatory. Kumagaya was chosen over Chichibu.
    # * Shiga: Otsu is AMeDAS only, not an observatory. Hikone was chosen over Ibukiyama.
    '石狩地方::札幌', '青森県::青森', '秋田県::秋田', '岩手県::盛岡', '宮城県::仙台',
    '山形県::山形', '福島県::福島', '茨城県::水戸', '栃木県::宇都宮', '群馬県::前橋',
    '埼玉県::熊谷', '東京都::東京', '千葉県::千葉', '神奈川県::横浜', '長野県::長野',
    '山梨県::甲府', '静岡県::静岡', '愛知県::名古屋', '岐阜県::岐阜', '三重県::津',
    '新潟県::新潟', '富山県::富山', '石川県::金沢', '福井県::福井', '滋賀県::彦根',
    '京都府::京都', '大阪府::大阪', '兵庫県::神戸', '奈良県::奈良', '和歌山県::和歌山',
    '岡山県::岡山', '広島県::広島', '島根県::松江', '鳥取県::鳥取', '徳島県::徳島',
    '香川県::高松', '愛媛県::松山', '高知県::高知', '山口県::山口', '福岡県::福岡',
    '大分県::大分', '長崎県::長崎', '佐賀県::佐賀', '熊本県::熊本', '宮崎県::宮崎',
    '鹿児島県::鹿児島', '沖縄県::那覇'
]
dict_block_name_en = {
    '札幌': 'sapporo', '青森': 'aomori', '秋田': 'akita', '盛岡': 'morioka', '仙台': 'sendai',
    '山形': 'yamagata', '福島': 'fukushima', '水戸': 'mito', '宇都宮': 'utsunomiya', '前橋': 'maebashi',
    '熊谷': 'kumagaya', '東京': 'tokyo', '千葉': 'chiba', '横浜': 'yokohama', '長野': 'nagano',
    '甲府': 'kofu', '静岡': 'shizuoka', '名古屋': 'nagoya', '岐阜': 'gifu', '津': 'tsu',
    '新潟': 'niigata', '富山': 'toyama', '金沢': 'kanazawa', '福井': 'fukui', '彦根': 'hikone',
    '京都': 'kyoto', '大阪': 'osaka', '神戸': 'kobe', '奈良': 'nara', '和歌山': 'wakayama',
    '岡山': 'okayama', '広島': 'hiroshima', '松江': 'matsue', '鳥取': 'tottori', '徳島': 'tokushima',
    '高松': 'takamatsu', '松山': 'matsuyama', '高知': 'kochi', '山口': 'yamaguchi', '福岡': 'fukuoka',
    '大分': 'oita', '長崎': 'nagasaki', '佐賀': 'saga', '熊本': 'kumamoto', '宮崎': 'miyazaki',
    '鹿児島': 'kagoshima', '那覇': 'naha',
}


def compress_impl(tar_gz_file, cache_files, remove_cache=True):
    with tarfile.open(tar_gz_file, 'w:gz') as tar:
        for filename in cache_files:
            tar.add(cache_path / filename, arcname=filename)
    print(f'Compressed: {tar_gz_file}')
    if remove_cache:
        for filename in cache_files:
            (cache_path / filename).unlink()


def extract_impl(tar_gz_file):
    if not tar_gz_file.is_file():
        return
    with tarfile.open(tar_gz_file, 'r:gz') as tar:
        tar.extractall(path=cache_path, filter='data')
    print(f'Extracted {tar_gz_file}')


def compress_month(year_month, remove_cache=True):
    """Compresses cache files for the specified month (YYYY-MM)."""
    def is_target(filename):
        li = filename.split('_')
        if len(li) != 3:
            return False
        return li[2].startswith(year_month)
    cache_files = [f.name for f in cache_path.iterdir() if is_target(f.name)]
    print(f'Found {len(cache_files)} files for {year_month}')
    tar_gz_file = tar_gz_path / f'{year_month}.tar.gz'
    compress_impl(tar_gz_file, cache_files, remove_cache)


def extract_month(year_month):
    """Extracts cache files for the specified month (YYYY-MM)."""
    tar_gz_file = tar_gz_path / f'{year_month}.tar.gz'
    extract_impl(tar_gz_file)


def compress_precs(remove_cache=True):
    """Compresses cache files for the prefecture list and block lists within each prefecture."""
    cache_files = [
        f.name for f in cache_path.iterdir()
        if f.name.startswith('prefecture')]
    tar_gz_file = tar_gz_path / 'prefectures.tar.gz'
    compress_impl(tar_gz_file, cache_files, remove_cache)


def extract_precs():
    """Extracts cache files for the prefecture list and block lists within each prefecture."""
    tar_gz_file = tar_gz_path / 'prefectures.tar.gz'
    extract_impl(tar_gz_file)


def get_page(url, cache_filename):
    """Gets a web page (reads from cache file if available)."""
    cache_file = cache_path / cache_filename
    if cache_file.exists():
        with open(cache_file, 'r', encoding='utf8') as ifile:
            content = ifile.read()
    else:
        print('Not cached yet:', cache_file)
        resp = requests.get(url)
        resp.raise_for_status()
        content = resp.text
        time.sleep(2.5)
        with open(cache_file, 'w', encoding='utf8') as ofile:
            ofile.write(content)
    return content


def get_page_block_date(prec_no=51, block_no=47636, year=2024, month=1, day=1):
    """Gets hourly weather data for a specific prefecture, block, and date."""
    cache_filename = f'{prec_no}_{block_no}_{year}-{month:02}-{day:02}.txt'
    url = (
        'https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php'
        f'?prec_no={prec_no}&block_no={block_no}'
        f'&year={year}&month={month}&day={day}&view='
    )
    return get_page(url, cache_filename)


def get_page_blocks(prec_no=14):
    """Gets the list of blocks within a prefecture."""
    cache_filename = f'prefecture_{prec_no}.txt'
    url = f'https://www.data.jma.go.jp/stats/etrn/select/prefecture.php?prec_no={prec_no}'
    return get_page(url, cache_filename)


def get_page_precs():
    """Gets the list of prefectures."""
    cache_filename = 'prefectures.txt'
    url = 'https://www.data.jma.go.jp/stats/etrn/select/prefecture00.php'
    return get_page(url, cache_filename)


def get_df_block_date(prec_no=44, block_no=47662, year=2024, month=1, day=1):
    """Returns a DataFrame of hourly weather data for a specific prefecture, block, and date."""
    content = get_page_block_date(prec_no, block_no, year, month, day)
    soup = BeautifulSoup(content, 'html.parser')
    tables = soup.find_all('table')
    data_rows = []
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) != 26:
            continue
        for row in rows:
            cols = row.find_all('td')
            if len(cols) != 17:
                continue
            img = cols[14].find('img')
            data_rows.append({
                '地域番号': prec_no,
                '地点番号': block_no,
                '年月日': f'{year}{month:02}{day:02}',
                '時': cols[0].get_text(),
                '降水量': cols[3].get_text(),
                '気温': cols[4].get_text(),
                '湿度': cols[7].get_text(),
                '風速': cols[8].get_text(),
                '降雪': cols[12].get_text(),
                '積雪': cols[13].get_text(),
                '天気': '' if (img is None) else img.get('alt'),
            })
    return pd.DataFrame(data_rows)


def get_df_block_dates(prec_no, block_no, dates):
    """Returns a DataFrame of hourly weather data for a specific prefecture and block over a date range."""
    dfs = []
    for date in dates:
        year, month, day = map(int, date.split('-'))
        dfs.append(get_df_block_date(prec_no, block_no, year, month, day))
    return pd.concat(dfs) if dfs else pd.DataFrame()


def get_dates(start_date, end_date):
    """Returns dates (YYYY-MM-DD) in the specified range as a dict with month keys."""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    dic_dates = {}
    for i in range((end - start).days + 1):
        date = (start + timedelta(days=i)).strftime('%Y-%m-%d')
        year_month = date[:7]
        if year_month not in dic_dates:
            dic_dates[year_month] = []
        dic_dates[year_month].append(date)
    return dic_dates


def get_master():
    """Gets the master data of prefecture numbers and block numbers."""
    print('Generating master data.')
    content_precs = get_page_precs()
    soup_precs = BeautifulSoup(content_precs, 'html.parser')
    precs = soup_precs.find_all('area')
    set_prec_block = set()
    df_m = pd.DataFrame(columns=[
        '地域番号',
        '地域名',
        '地点番号',
        '地点名',
        '地点名英字',
        '緯度',
        '経度',
    ])
    for prec in precs:
        prec_no = int(prec.get('href').split('prec_no=')[1].split('&')[0])
        prec_name = prec.get('alt')
        if prec_name == '南極':
            continue
        content_blocks = get_page_blocks(prec_no)
        soup_blocks = BeautifulSoup(content_blocks, 'html.parser')
        blocks = soup_blocks.find_all('area')
        for block in blocks:
            s = block.get('onmouseover')
            if s is None:
                continue
            li = s.split('(')[1].split(')')[0].split(',')
            li = [x[1:-1] for x in li]
            if li[0] != 's':  # Observatory, not AMeDAS
                continue
            block_no = int(li[1])
            block_name = li[2]
            lat = float(li[4]) + float(li[5]) / 60.0
            lng = float(li[6]) + float(li[7]) / 60.0
            prec_block = prec_name + '::' + block_name
            if prec_block not in prec_blocks_target:
                continue
            if prec_block in set_prec_block:
                continue
            set_prec_block.add(prec_block)
            print(prec_no, prec_name, block_no, block_name)
            df_m.loc[len(df_m)] = [
                prec_no,
                prec_name,
                block_no,
                block_name,
                dict_block_name_en[block_name],
                lat,
                lng,
            ]
    df_m.to_csv(out_file_master, index=False, lineterminator='\n')
    print(f'Generated: {out_file_master}')
    return df_m


def main(start_date, end_date):
    """Main function."""
    cache_path.mkdir(exist_ok=True)
    tar_gz_path.mkdir(exist_ok=True)
    out_path.mkdir(exist_ok=True)

    # Get the master data of prefecture numbers and block numbers
    extract_precs()
    df_m = get_master()
    compress_precs()

    # Get weather data for the specified date range (by month)
    dic_dates = get_dates(start_date, end_date)
    dfs = []
    for year_month, dates in dic_dates.items():
        extract_month(year_month)
        print(f'===== {year_month} =====')
        for _, row_m in df_m.iterrows():
            print('-----', row_m['地域名'], row_m['地点名'], '-----')
            dfs.append(get_df_block_dates(row_m['地域番号'], row_m['地点番号'], dates))
        compress_month(year_month)
    if dfs:
        df = pd.concat(dfs)
        df.to_csv(out_file, index=False, lineterminator='\n')
        file_size_mb = out_file.stat().st_size / (1024 * 1024)
        print(f'Generated: {out_file} ({file_size_mb:.2f} MB)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape weather data from JMA.')
    parser.add_argument('start_date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    main(args.start_date, args.end_date)
