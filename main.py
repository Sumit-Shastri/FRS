import pandas
import glob
from typing import Optional, List, Tuple, Dict, Union
model_collateral_df = pandas.read_csv(r"E:\lending-club-data\model_collateral.csv")
#print(model_collateral)

model_config_df = pandas.read_csv(r"E:\lending-club-data\model_config.csv")
#print(model_config)

csv_files = glob.glob(r"E:\lending-club-data\model_auth_Rep/*.csv")
combined_model_auth_rep = pandas.DataFrame()
for csv_file in csv_files:
                          df = pandas.read_csv(csv_file)
                          combined_model_auth_rep = pandas.concat([combined_model_auth_rep,df])
print(combined_model_auth_rep)

'''validating phase'''

def validation_df(df : pandas.DataFrame,
                  n_cols : Optional[int] = None,
                  n_rows : Optional[tuple] = None,
                  columns : Optional[list] = None,
                  column_types : Optional[dict] = None,
                  duplicates : bool = False,
                  null_values : bool = False,
                  unique_columns : Optional[list[str]] = None,
                  column_ranges : Optional[Dict[str, Tuple[Union[int, float], Union[int, float]]]] = None,
                  date_columns : Optional[List[str]] = None,
                  categorical_columns : Optional[Dict[str, List[Union[str, int, float]]]] = None
                  ) -> Tuple[bool, str]:
                                        if n_cols is not None and len(df.columns) != n_cols:
                                                                                  return False,f"Error: Expected {n_cols} columns, but found {len(df.columns)} columns."
                                        if n_rows is not None:
                                                             min_rows , max_rows = n_rows
                                                             if not (min_rows <= len(df) <= max_rows):
                                                                                                     return False, f"Error, Number of rows should be between {min_rows} and {max_rows}."
                                        if columns is not None and not set(columns).issubset(df.columns):
                                                                                                         missing_columns = set(columns) - set(df.columns)
                                                                                                         return False , f"Error : Missing columns are : {missing_columns}"
                                        if column_types is not None:
                                                                    for col, expected_type in column_types.items():
                                                                                                                if col not in df.columns:
                                                                                                                                        return False, f"Error: Column '{col}' not found."
                                                                                                                if not df[col].dtype == expected_type:
                                                                                                                                                return False, f"Error: Column '{col}' should have type {expected_type}."
                                        if duplicates and df.duplicated().any():
                                                                                return False, "Duplicates found in the DataFrame."
                                        if null_values and df.isnull().any().any():
                                                                                    return False, "DataFrame contains null values."
                                        if unique_columns is not None:
                                                                    for col in unique_columns:
                                                                                        if col in df.columns and df[col].duplicated().any():
                                                                                                                 return False, f"Column '{col}' should have only unique values."
                                        if column_ranges is not None:
                                                                    for col, value_range in column_ranges.items():
                                                                                        if col in df.columns and not df[col].between(*value_range).all():
                                                                                                        return False, f"Values in '{col}' should be between {value_range[0]}"
                                        if date_columns is not None:
                                                            for col in date_columns:
                                                                            if col in df.columns:
                                                                                        try:
                                                                                            pandas.to_datetime(df[col], errors='raise')
                                                                                        except ValueError:
                                                                                            return False, f"'{col}' should be in a valid date format."


                                        if categorical_columns is not None:
                                                                for col, allowed_values in categorical_columns.items():
                                                                                    if col in df.columns and not df[col].isin(allowed_values).all():
                                                                                                    return False, f"Values in '{col}' should be {allowed_values}."


                                        return True, "DataFrame has passed all validations."

is_valid, message = validation_df(model_config_df, n_cols=4, duplicates=True)
print(is_valid, message)





