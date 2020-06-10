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
 
