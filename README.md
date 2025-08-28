# Pipeline ETL de Precios de Vehículos con Dagster

Este proyecto contiene un pipeline de datos de extremo a extremo para procesar y limpiar un conjunto de datos sobre precios de vehículos. El flujo de trabajo, orquestado con **Dagster**, transforma los datos crudos en conjuntos de datos limpios, organizados y listos para el análisis.

## DAG del Pipeline


![image_973a52.png]()

## Fases del Pipeline

1.  **Extracción (`car_data_file`):** Obtención de los datos crudos desde una fuente remota (URL).
2.  **Transformación I (`column_cleaned`):** Eliminación de columnas irrelevantes.
3.  **Transformación II (`data_cleaned`):**
    * Limpieza, imputación de datos faltantes y estandarización del formato.
4.  **Carga y Segmentación (`split_data_by_make`):**
    * Segmentación del DataFrame limpio por marca de vehículo y carga en archivos CSV individuales.

## Tecnologías Utilizadas
* **Lenguaje:** Python
* **Bibliotecas:** Pandas, Dagster
