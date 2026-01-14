"""
Validates weather data and reports missing values.

Ex. python validate.py 2025-09-01 2025-12-31
"""
import argparse
import pandas as pd


dict_valiable_name_en = {
    '気温': 'temp',
    '湿度': 'humid',
    '降水量': 'precip',
    '風速': 'wind',
    '降雪': 'snowfall',
    '積雪': 'snow',
}


def format(date_str):
    return f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}'


def is_numeric(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def to_numeric_safe(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return float('nan')


def get_jma_url(row):
    """Returns the JMA hourly data URL for the given row."""
    prec_no = row['地域番号']
    block_no = row['地点番号']
    date_str = row['年月日']
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    return (
        'https://www.data.jma.go.jp/stats/etrn/view/hourly_s1.php'
        f'?prec_no={prec_no}&block_no={block_no}'
        f'&year={year}&month={month}&day={day}&view='
    )


def reshape_weather(weather: pd.DataFrame, master: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    weather = weather.copy()
    weather['_hour'] = weather['時'].astype(int)
    weather['_date'] = pd.to_datetime(weather['年月日'], format='%Y%m%d')
    weather['timestamp'] = weather['_date'] + pd.to_timedelta(weather['_hour'], unit='h')
    for var in variables:
        weather[var] = weather[var].apply(to_numeric_safe)
    weather['block_name'] = weather['地点番号'].map(dict(zip(master['地点番号'], master['地点名英字'])))
    result = pd.DataFrame({'timestamp': sorted(weather['timestamp'].unique())})
    for var in variables:
        var_abbr = dict_valiable_name_en[var]
        pivot = weather.pivot(index='timestamp', columns='block_name', values=var)
        pivot.columns = [f'{var_abbr}_{block_name}' for block_name in pivot.columns]
        result = result.merge(pivot, on='timestamp', how='left')
    return result


def validate_column(
    weather: pd.DataFrame,
    master: pd.DataFrame,
    column: str,
    n_max_block: int = 3,
    n_max_hour: int = 3,
    output_valid: bool = False,
):
    """Validates the specified column and prints a summary of missing values."""
    actual_start = format(weather['年月日'].min())
    actual_end = format(weather['年月日'].max())
    total_days = weather['年月日'].nunique()
    print(f'Date range: {actual_start} to {actual_end} ({total_days} days)')

    weather = weather.merge(
        master[['地域番号', '地点番号', '地点名英字']],
        on=['地域番号', '地点番号'],
        how='left'
    )
    weather['_valid'] = weather[column].apply(is_numeric)
    total_blocks = weather['地点名英字'].nunique()
    missing_df = weather[~weather['_valid']]
    blocks_with_missing = missing_df['地点名英字'].nunique()
    blocks_without_missing = total_blocks - blocks_with_missing

    print(f'# {column}')
    print(f'- Total       : {total_blocks} blocks')
    print(f'- No missing  : {blocks_without_missing} blocks')
    print(f'- Has missing : {blocks_with_missing} blocks')
    if blocks_with_missing > 0:
        print('  - Missing blocks/datetimes')
        missing_counts = missing_df.groupby('地点名英字').size().sort_values(ascending=False)
        shown_blocks = 0
        for block_name, count in missing_counts.items():
            if shown_blocks >= n_max_block:
                print(f'    - ... and {len(missing_counts) - n_max_block} more blocks')
                break
            print(f'    - {block_name} : {count} missing')
            block_missing = missing_df[missing_df['地点名英字'] == block_name]
            for i, (_, row) in enumerate(block_missing.iterrows()):
                if i >= n_max_hour:
                    print(f'      - ... and {len(block_missing) - n_max_hour} more')
                    break
                date_str = row['年月日']
                hour = row['時']
                print(f"      - {format(date_str)} {hour}:00")
                print(f'        {get_jma_url(row)}')
            shown_blocks += 1

    if not output_valid:
        return

    invalid_blocks = weather[~weather['_valid']][['地域番号', '地点番号']].drop_duplicates()
    valid_weather = weather.merge(invalid_blocks, on=['地域番号', '地点番号'], how='left', indicator=True)
    valid_weather = valid_weather[valid_weather['_merge'] == 'left_only'].drop(columns=['_merge', '_valid'])
    n_blocks = valid_weather[['地域番号', '地点番号']].drop_duplicates().shape[0]
    out_file = f'out/weather_japan_{actual_start}_{actual_start}_{n_blocks}_blocks.csv'
    result = reshape_weather(weather, master, [column])
    result.to_csv(out_file, index=False, lineterminator='\n')
    print(f'Generated: {out_file}')


def main(lb, ub, output_valid):
    weather = pd.read_csv('out/weather_japan_org.csv', dtype=str)
    master = pd.read_csv('out/weather_japan_master.csv', dtype=str)
    weather = weather[(weather['年月日'] >= lb.replace('-', '')) & (weather['年月日'] <= ub.replace('-', ''))]

    column = '気温'
    validate_column(weather, master, column, output_valid=output_valid)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Validates weather data and reports missing values.')
    parser.add_argument('lb', help='Lower bound date (YYYY-MM-DD)')
    parser.add_argument('ub', help='Upper bound date (YYYY-MM-DD)')
    parser.add_argument('-o', '--output_valid', action='store_true')
    args = parser.parse_args()
    main(args.lb, args.ub, args.output_valid)
