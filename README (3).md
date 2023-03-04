# file
dl.cfg : contains the AWS access key and secret key

etl.py : reads song and log data in JSON format, processes the data by extracting necessary columns, renaming columns, creating new columns and filtering the dataset It then writes the processed data in Parquet format

# how to run script
 
in terminal write 
```
python etl.py
```
