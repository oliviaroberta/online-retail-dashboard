# streamlit_app.py
import streamlit as st
import pandas as pd
from pyathena import connect
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

st.title("📊 Online Retail Analytics Dashboard")

# Connect to Athena
conn = connect(
    s3_staging_dir='s3://retail1/',
    region_name='eu-north-1'
)

@st.cache_data
def run_query(query):
    return pd.read_sql(query, conn)

# Tabs for different analyses
tabs = st.tabs(["Total Sales by Country", "Top Products", "Monthly Sales"])

with tabs[0]:
    query = """SELECT Country, SUM(Quantity * Price) AS TotalSales 
               FROM retaildb.online_retail_clean 
               GROUP BY Country 
               ORDER BY TotalSales DESC 
               LIMIT 10"""
    df = run_query(query)
    st.plotly_chart(px.bar(df, x='Country', y='TotalSales', title='Total Sales by Country'))

with tabs[1]:
    query = """SELECT Description, SUM(Quantity) AS TotalQuantity 
               FROM retaildb.online_retail_clean 
               GROUP BY Description 
               ORDER BY TotalQuantity DESC 
               LIMIT 10"""
    df = run_query(query)
    st.plotly_chart(px.bar(df, x='Description', y='TotalQuantity', title='Top 10 Products by Quantity'))

with tabs[2]:
    query = """SELECT 
    date_format(parse_datetime(InvoiceDate, 'yyyy-MM-dd HH:mm:ss'), '%Y-%m') AS Month,
    SUM(Quantity * Price) AS MonthlySales
FROM retaildb.online_retail_clean
WHERE try(parse_datetime(InvoiceDate, 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL
GROUP BY date_format(parse_datetime(InvoiceDate, 'yyyy-MM-dd HH:mm:ss'), '%Y-%m')
ORDER BY Month;"""
    df = run_query(query)
    st.plotly_chart(px.line(df, x='Month', y='MonthlySales', title='Monthly Sales Trends'))

