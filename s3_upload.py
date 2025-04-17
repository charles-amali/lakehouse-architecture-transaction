import os
import boto3
import pandas as pd
import io


bucket_name = 'delta-lake-bkt01'
s3_base_prefix = 'raw/'

local_folder = 'Data'

s3 = boto3.client('s3')

# --- Loop through files in local folder ---
for filename in os.listdir(local_folder):
    file_path = os.path.join(local_folder, filename)

    if filename.endswith('.xlsx'):
        workbook_name = os.path.splitext(filename)[0]
        print(f"Processing workbook: {filename}")
        xls = pd.ExcelFile(file_path)

        for sheet in xls.sheet_names:
            print(f"Sheet: {sheet}")
            df = pd.read_excel(xls, sheet_name=sheet).dropna(how='all')

            clean_sheet_name = sheet.lower().replace(' ', '_')
            s3_key = f"{s3_base_prefix}{workbook_name}/{clean_sheet_name}.csv"

            # Upload to S3
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
            print(f"Uploaded to s3://{bucket_name}/{s3_key}")

    elif filename.endswith('.csv'):
        base_name = os.path.splitext(filename)[0] 
        print(f"Processing single CSV: {filename}")
        df = pd.read_csv(file_path).dropna(how='all')

        s3_key = f"{s3_base_prefix}{base_name}/{base_name}.csv"

        # Upload to S3
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"Uploaded to s3://{bucket_name}/{s3_key}")
