import pandas as pd

def process_data_chunks(main_data_path, additional_data_path, chunk_size=50000):
    additional_data = pd.read_csv(additional_data_path, low_memory=False)
    additional_columns = [col for col in additional_data.columns if col.startswith('WNPI_') or col.startswith('RATE_') or col.startswith('COM_')]
    additional_data.set_index('CID', inplace=True)

    columns_to_keep = ['CID', 'L1', 'MHP', 'DOI', 'TYP', 'MAT', 'D_ADV', 'MODE', 'L1_C', 'L2_C', 'L3_C', 'L4_C', 'L5_C']
    columns_to_unstack = ['NPI']

    chunk_list = []
    for chunk in pd.read_csv(main_data_path, chunksize=chunk_size, low_memory=False):
        if 'MONTH_YEAR' not in chunk.columns:
            raise KeyError("Missing 'MONTH_YEAR' column in the main data")

        pivot_df = chunk.pivot_table(index='CID', columns='MONTH_YEAR', values=columns_to_unstack, aggfunc='sum').fillna(0)
        pivot_df.columns = [f'{col[0]}_{col[1]}' for col in pivot_df.columns]

        data_to_keep = chunk[columns_to_keep].drop_duplicates(subset='CID').set_index('CID')
        merged_chunk = data_to_keep.join(pivot_df, how='outer')
        chunk_list.append(merged_chunk)

    merged_df = pd.concat(chunk_list)
    merged_df = merged_df.sort_index()
    merged_df['TC'] = merged_df[['L1_C', 'L2_C', 'L3_C', 'L4_C', 'L5_C']].sum(axis=1)
    final_df = merged_df.join(additional_data[additional_columns], how='left')
    final_df = final_df.sort_index()

    return final_df

def add_next_month_rate(final_df, last_rate_column, next_month, next_month_str):
    next_rate_column = f'RATE_{next_month_str}'
    final_df[next_rate_column] = final_df[last_rate_column]

    next_month_start = next_month.replace(day=1)
    next_month_end = (next_month + timedelta(days=31)).replace(day=1) - timedelta(days=1)

    final_df.loc[final_df['MODE'] == 'Trail', next_rate_column] = final_df['TC']
    final_df.loc[(final_df['MODE'] == 'Upfront') &
                 (pd.to_datetime(final_df['DOI'], errors='coerce').between(next_month_start, next_month_end)), next_rate_column] = final_df['TC']

    return final_df
