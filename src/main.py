import pandas as pd
from datetime import datetime, timedelta
from utils import process_data_chunks, add_next_month_rate

def main():
    main_data_path = 'data/ACCRUED_COMPILOR.csv'
    additional_data_path = 'data/Accrued_Commission_Apr24.csv'

    # Process data in chunks
    final_df = process_data_chunks(main_data_path, additional_data_path)

    
    additional_columns = [col for col in final_df.columns if col.startswith('WNPI_') or col.startswith('RATE_') or col.startswith('COM_')]
    last_rate_column = [col for col in additional_columns if col.startswith('RATE_')][-1]
    current_month_str = last_rate_column.split('_')[-1]
    next_month = datetime.strptime(current_month_str, '%Y%m') + timedelta(days=31)
    next_month_str = next_month.strftime('%Y%m')

    
    final_df = add_next_month_rate(final_df, last_rate_column, next_month, next_month_str)

    # Save the updated CSV
    transformed_data_path = 'output/Transformed_Accrued_Commission_Apr24.csv'
    try:
        final_df.to_csv(transformed_data_path)
        print("Transformation completed and saved to:", transformed_data_path)
    except PermissionError:
        print(f"Permission denied: Unable to save the file to {transformed_data_path}. Please check the file permissions.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
