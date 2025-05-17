# athena_query.py
import boto3
import pandas as pd
import os
import time
from dotenv import load_dotenv

load_dotenv()

def run_athena_query(query, database):
    aws_region = os.getenv("AWS_REGION")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    output = os.getenv("ATHENA_OUTPUT_BUCKET")

    if not all([aws_region, aws_access_key_id, aws_secret_access_key, output]):
        raise EnvironmentError("❌ Missing AWS credentials or S3 output path in .env file.")

    print("✅ AWS credentials and output bucket loaded.")

    athena = boto3.client(
        'athena',
        region_name=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    print("🚀 Starting Athena query...")
    try:
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output}
        )
    except Exception as e:
        print("❌ Failed to start query:", e)
        return

    execution_id = response['QueryExecutionId']
    print(f"🆔 Query Execution ID: {execution_id}")

    # Wait for query to complete
    while True:
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        status = result['QueryExecution']['Status']['State']
        print(f"⏳ Query status: {status}")
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if status != 'SUCCEEDED':
        print("❌ Query failed with status:", status)
        return

    print("✅ Query succeeded. Fetching results...")

    results = athena.get_query_results(QueryExecutionId=execution_id)

    # Parse the results into a DataFrame
    header = [col['VarCharValue'] for col in results['ResultSet']['Rows'][0]['Data']]
    data = []
    for row in results['ResultSet']['Rows'][1:]:
        data.append([col.get('VarCharValue', None) for col in row['Data']])

    df = pd.DataFrame(data, columns=header)
    print(f"📄 Retrieved {len(df)} rows.")
    return df
