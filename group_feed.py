import pandas as pd

def group_by_feed_url(excel_filepath, sheet=0):
    """
    We group the excel file by the feed_url and put the other elements 
    in a list (feed_id, owner_id, platform_id, platform_name).
    """
    try:
        # Reed the excel file
        df = pd.read_excel(excel_filepath, sheet_name=sheet)
    except FileNotFoundError:
        print(f"Error: Don't found the file at '{excel_filepath}'")
        return None
    except Exception as e:
        print(f"While reading the Excel occurred an error: {e}")
        return None

    # Ensure all columns exist
    required_columns = ['feed_id', 'feed_url', 'owner_id', 'platform_id', 'platform_name']
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: column '{col}' doesn't found in the excel.")
            return None

    # Create a new column (feed_id, owner_id, platform_id, platform_name)
    df['tuple_info'] = df.apply(lambda row: (
        str(row['feed_id']) if pd.notna(row['feed_id']) else '',
        str(row['owner_id']) if pd.notna(row['owner_id']) else '',
        str(row['platform_id']) if pd.notna(row['platform_id']) else '',
        str(row['platform_name']) if pd.notna(row['platform_name']) else ''
    ), axis=1)

    # Group by 'feed url' and add 'tuple_info' in a list
    df_group = df.groupby('feed_url').agg(
        owner_platform_list=('tuple_info', lambda x: list(x))
    ).reset_index()

    return df_group


if __name__ == "__main__":
    # Save the path of the excel that has the feeds whit customers
    file_path = 'Feeds with customers.xlsx'

    df_result = group_by_feed_url(file_path)

    if df_result is not None:
        # See the first rows ot the resulting file
        print("Data group by feed_url:")
        print(df_result.head())

        # Save the result on a new excel
        resultName = "Group_feed_url"
        df_result.to_excel(f'{resultName}.xlsx', index=False)
        print(f"\nResultado guardado en '{resultName}.xlsx'")