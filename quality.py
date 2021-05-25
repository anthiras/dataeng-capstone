"""Data quality checks"""
import pyspark.sql.functions as F


def verify_not_empty(df):
    """Verify dataframe is not empty"""
    assert df.count() > 0, "Data quality check failed: Dataframe is empty"


def verify_no_missing_values(df, column_name):
    """Verify dataframe does not have missing/empty values in the given column name"""
    missing = df.filter(f"{column_name} IS NULL OR {column_name} = ''").count()
    assert missing == 0, "Data quality check failed: There are missing values in column " + column_name
    #empty = df.where(df[column_name] == "").count()
    #assert empty == 0, "Data quality check failed: There are empty values in column " + column_name


def verify_values(df, column_name, allowed_values):
    invalid_rows = df.filter(df[column_name].isin(allowed_values) == False)
    assert invalid_rows.count() == 0, "Data quality check failed: Column " + column_name + " contains unexpected values"
