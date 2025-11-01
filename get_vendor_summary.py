import sqlite3
import pandas as pd
from ingestion_db.py import ingest_db
def create_vendor_summary(conn):
    vendor_sales_summary = pd.read_sql_query("""
        with FreightSummary as (
        select vendornumber,
        sum(freight) as FreightCost
        from vendor_invoice 
        group by vendornumber
        ),
        PurchaseSummary as (
        SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.PurchasePrice,
        p.Description,
        pp.volume,
        pp.price as actualPrice,
        sum(p.quantity) as total_purchase_quantity,
        sum(p.Dollars) as total_purchase_Dollars
        from purchases p
        join purchase_prices pp
        on p.Brand = pp.brand
        where p.purchasePrice > 0
        group by p.vendorNumber, p.VendorName, p.Brand,p.PurchasePrice,pp.volume,pp.price,pp.volume,p.Description
        order by total_purchase_dollars desc
        ),
        sales_summary as (
        select
        vendorNo,
        Brand,
        sum(salesDollars) as TotalSalesDollars,
        sum(salesPrice) as TotalSalesPrice,
        sum(SalesQuantity) as TotalSalesQuantity,
        sum(ExciseTax) as TotalExciseTax
        from sales
        Group By VendorNo,Brand
        Order by totalsalesdollars
        )
        
        select 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.total_purchase_quantity,
        ps.total_purchase_Dollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
        from purchaseSummary ps
        left join sales_summary ss
        on ps.vendornumber = ss.vendorNo and ps.brand = ss.brand
        left join freightsummary fs
        on ps.vendornumber = fs.vendornumber
        order by ps.total_purchase_Dollars desc
    """,conn)
    return vendor_sales_summary

def clean_data(df):
    # Data Cleaning
    df['volume'] = df['volume'].astype('float')
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    df.fillna(0,inplace=True)

    # creating new columns
    df["GrossProfit"] = df["TotalSalesDollars"] - df['total_purchase_Dollars']
    df["ProfitMargin"] = (df["GrossProfit"] / df['TotalSalesDollars'])*100
    df["StockTurnover"] = df["TotalSalesQuantity"] / df['total_purchase_quantity']
    df["SalestoPurchaseRatio"] = df["TotalSalesDollars"] / df['total_purchase_Dollars']
    
    return df

if __name__ == '__main__':
    conn = sqlite3.connect('inventory.db')
    summary_df = create_vendor_summary(conn)
    cleaned_df = clean_data(summary_df)
    ingest_db(cleaned_df,'vendor_sales_summary',conn)