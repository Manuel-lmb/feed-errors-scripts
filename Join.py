import pandas as pd

def join_feed_customer_data(main_file_path,
                            main_join_column,
                            customer_file_path,
                            customer_join_column,
                            output_file_path):

    try:
        # Log the main file
        df_main = pd.read_excel(main_file_path, header=0)
        print(f"'{main_file_path}' log successfull. Columns: {df_main.columns.tolist()}")

        # Log the customer file
        df_customer = pd.read_excel(customer_file_path, header=0)
        print(f"'{customer_file_path}' log successfull. Columns: {df_customer.columns.tolist()}")

        # Verify if join columns exists in both file
        if main_join_column not in df_main.columns:
            raise ValueError(f"The join column '{main_join_column}' doesn't found on '{main_file_path}'.")
        if customer_join_column not in df_customer.columns:
            raise ValueError(f"The join column '{customer_join_column}' doesn't found on '{customer_file_path}'. ")

        # --- Realize an INNER JOIN on the files ---
        df_combined = pd.merge(df_main, df_customer,
                               left_on=main_join_column,
                               right_on=customer_join_column,
                               how='inner')

        # --- Manage the duplicated URL columns ---
        if main_join_column != customer_join_column and customer_join_column in df_combined.columns:
            df_combined = df_combined.drop(columns=[customer_join_column]) # keep only one URL column

        # Save the result on a new excel file
        df_combined.to_excel(output_file_path, index=False)
        print(f"\nData join save successfully on the file '{output_file_path}'.")
        print(f"Columns: {df_combined.columns.tolist()}")
        print(f"Rows: {len(df_combined)}")


    except FileNotFoundError as e:
        print(f"Error: Uno de los archivos no fue encontrado. Por favor, verifica la ruta: {e}")
    except ValueError as e:
        print(f"Error en la columna de unión: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    join_feed_customer_data(
        main_file_path="feed_errors.xlsx", # Feed whit their error
        main_join_column="Feed_URL",
        customer_file_path="Group_feed_url.xlsx", # Feed whit customer group by feed_url
        customer_join_column="feed_url",
        output_file_path="final_report.xlsx" # Name of the resulting file
    )