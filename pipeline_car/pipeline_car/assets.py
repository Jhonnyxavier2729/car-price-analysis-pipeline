import dagster as dg
import pandas as pd 
import os 

CSV_URL="https://raw.githubusercontent.com/Azure/carprice/refs/heads/master/dataset/carprice.csv"
CSV_PATH="data/car_file_origin.csv"
CSV_FN="data/car_file_final.csv"

# 1.Extract the raw car data

@dg.asset
def car_data_file(context:dg.AssetExecutionContext):
    """
    extracting CSV path for saving a folder locally
    """
    context.log.info(f"Saving car data to {CSV_PATH}")
    df = pd.read_csv(CSV_URL)
    df.to_csv(CSV_PATH, index=False)
    return df




# 2. Deleting the column
@dg.asset
def column_cleaned(context:dg.AssetExecutionContext, car_data_file: pd.DataFrame): 
    context.log.info("Procesando DataFrame en memoria...")
    
    
    df = car_data_file.drop(['symboling'], axis=1) 
    
    context.log.info(f"Columna 'symboling' eliminada: {df.head()}")
    return df 




# 3. Cleaning and rounding the data
@dg.asset(deps=[column_cleaned])
def data_cleaned(context:dg.AssetExecutionContext, column_cleaned: pd.DataFrame):
    """
    getting data emptys,priceto transform it to data NULL
    fill the values null de normalized-losses with the media at the column
    """

    df = column_cleaned
    
    
    context.log.info(f"Iniciando limpieza final del DataFrame en memoria...")

    #2. transform the data price and normalized to NULL
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['normalized-losses'] = pd.to_numeric(df['normalized-losses'], errors='coerce')

    #3. delete the rows where price have data empty 
    rows_before=len(df)
    df.dropna(subset=['price'], inplace=True)
    rows_after=len(df)
    context.log.info(f"the rows were deleted:{rows_before-rows_after}for have price null")

    #4.pad imput null values in normalized-loss with the mean
    mean_losses=df['normalized-losses'].mean()
    df['normalized-losses'].fillna(mean_losses, inplace=True)
    context.log.info(f"the values normalized losses filed with mean:{mean_losses:.2f}")


    # 5. Round columns for a clean format
    df['price'] = df['price'].round(2).astype(float)
    df['normalized-losses'] = df['normalized-losses'].round(1).astype(float)
    context.log.info("Rounding the columns 'price' and 'normalized-losses.")


    #6. save the cleaned data
    df.to_csv(CSV_FN, index=False)
    context.log.info(f"the cleaned data saved to:{CSV_FN}")
    
    return df

# 4. Split the data by make
@dg.asset
def split_data_mark(context:dg.AssetExecutionContext, data_cleaned:pd.DataFrame):

    """
    grupo by mark throug diferents files CVS
    """
    df=data_cleaned
    output_folders="output_cars"

    #makes the carpet if do not exists
    if not os.path.exists(output_folders):
        os.makedirs(output_folders)

    context.log.info(f"Agrupando datos por marca y guardando en la carpeta '{output_folders}'...")

    #group the make with pandas 

    grouped_by_make=df.groupby("make")


   #group by mark and save each group to a separate CSV file
    for mark, group_df in grouped_by_make:
        # Create a valid filename 
        filename = f"{mark}.csv"
        file_path = os.path.join(output_folders, filename)

        # Guardar el DataFrame de ese grupo en su propio archivo CSV
        group_df.to_csv(file_path, index=False)
        context.log.info(f"Archivo guardado: {file_path}")

    return (f"Se crearon {len(grouped_by_make)} archivos CSV en la carpeta '{output_folders}'.")
