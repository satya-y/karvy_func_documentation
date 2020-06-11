# Karvy Preprocessing Blocks Documentation

Documentation on karvy preprocessing_api Blocks

## alternate_name_validation Block flow

This function helps to select alternate column name if it exists.

```
def alternate_name_validation(df, columns_data):
```
  
  * input parameters for the above function are df,columns_data
  
    df - input dataframe
    
    columns_data - {"column_name":"column_alternative_name"}
    
    Ex: {"UTR NO." : "UTR"}
 
  * at starting we are iterating through columns present in input dataframe.
  
  * then we are checking whether that each column present in mentioned input columns (columns_data.keys())
  
  * if it was not present then we are creating a alternate column list and renaming that column name.
  
  ```
  return_data = {}
  df_columns = df.columns
  columnNames = columns_data.keys()
  for data in df_columns:
      if data in columnNames:
          continue
      else:
          for column in columnNames:
              if not pd.isna(columns_data[column]) and columns_data[column] != "":
                  alt_list = [data.lower() for data in columns_data[column].split(",")]
                  if data.lower() in alt_list:
                      df.rename(columns={data: column}, inplace=True)
  return_data['flag'] = True
  return_data['data'] = {'value': df}
  return return_data
  
  ```
  * sample feed for testing this is CPVIRTUAL_CG99.
  
  * finally we are returning that modified dataframe.
  
## data_type_validation Block flow

This function helps to converting the column data into a specific datatype

```
def data_type_validation(df, columns_data):
```

* input parameters for the above function are df,columns_data
  
    df - input dataframe
    
    columns_data - {"column_name":"Column type"}
    
    Ex: {"Scheme Name" : "Integer"}
* Then we are renaming all datatype names into a certain format by parser

* after that we are calling [type_conversion] function

```
df = type_conversion(df,column_types=column_type_mapping)
```
Ex: column_types = {"Scheme Name" : "int"}

* now we are iterating through each column and calling [conversion_code_apply] function for each column by passing below parameters

```
for column, type_ in column_types.items():
  df = df.apply(conversion_code_apply, column=column, type_=type_, axis=1)
```

* in conversion_code_apply we are converting the type of column data additionally we are filling NA values and replacing commas and handling certain senerios.

```
def conversion_code_apply(row, column, type_):
    try:
        if row[column] and row[column] != 'NA':
            if type_ == 'string' or type_ == 'varchar' or type_ == 'object':
                try:
                    row[column] = str(row[column])
                except:
                    row[rejection_reason_column]=row[rejection_reason_column]+f"Column {column } is not a String format"
                    row[filter_column]='N'
            elif type_ == 'dict' or type_ == 'object':
                try:
                    row[column] = ast.literal_eval(str(row[column]))
                except:
                    row[rejection_reason_column]=row[rejection_reason_column]+f"Column {column } is not a Dictionary format"
                    row[filter_column]='N'
            elif type_ == 'float32' or type_ == 'float':
                try:
                    row[column] = str(row[column]).replace(" ", "")
                    row[column] = float(str(row[column]).replace(",", ""))
                except:
                    row[rejection_reason_column]=row[rejection_reason_column]+f"Column {column } is not a Float format"
                    row[filter_column]='N'
```

 in this function we are handling below datatypes they are:
  
  1.Integer
  2.String
  3.varchar
  3.object
  4.dict
  5.float32 and float64
  6.date
  7.time
  
## mandatory_check Block flow

This function helps to reject the rows which are empty in mandatory columns from input dataframe columns.

  columnsToCheck = list of mandatory columns

```
def mandatory_check(df, columnsToCheck):
    return_data = {}
    for columnName in columnsToCheck:
        rejection_list = []
        column_data = df[columnName].tolist()
        for j, data in enumerate(column_data):
            if pd.isna(data) or data==" " or data=="":
                rejection_list.append(j)
        update_rejection_reason(df, rejection_list, columnName+" is empty")
```
* the update_rejection_reason function will add rejection reason

## upload_csv Block flow

This function will ingest the generated output files (standard and raw) into database tables.

```
def upload_csv(file_path=None,raw_file_path=None,standard_file_path=None,table_name=None,type_=None,rejected_file_path=None,ingest = True):

    query = f"/opt/mssql-tools/bin/bcp karvy_extraction.dbo.{table_name} in '{str(Path(raw_file_path))}' -S {os.environ['HOST_IP']} -U {os.environ['LOCAL_DB_USER']} -P '{os.environ['LOCAL_DB_PASSWORD']}' -t '|' -c -q -e -F2"

```

