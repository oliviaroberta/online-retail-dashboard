from athena_query import run_athena_query

query = "SELECT * FROM retaildb.online_retail_clean LIMIT 5"
df = run_athena_query(query, "retaildb")

if df is not None:
    print(df.head())
