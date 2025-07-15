import datetime
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use Agg backend to avoid Tkinter issues in scripts
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
#import matplotlib.dates as mdates
import seaborn as sns
#from scipy import stats
import plotly.express as px
from matplotlib.backends.backend_pdf import PdfPages
import sys
sys.path.append('/opt/airflow/Global_Electronics_Retailer')

def Analyze_Sales(Full_Data=None, input_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/combined_data.pkl', return_figs=True):
    if Full_Data is None:
        Full_Data = pd.read_pickle(input_path)

    figs = []

    formatter = ticker.FuncFormatter(lambda x, pos: f'{x*1e-6:.2f}M')  # define here
    
    # Monthly Sales vs Profit
    monthly_metrics = Full_Data.groupby(Full_Data['Order Date'].dt.to_period('M')).agg({
        'Sales Amount USD': 'sum',
        'Profit': 'sum'
    }).reset_index()
    monthly_metrics['Profit_Margin'] = monthly_metrics['Profit'] / monthly_metrics['Sales Amount USD']
    monthly_metrics['Order Date'] = monthly_metrics['Order Date'].dt.to_timestamp()
    fig3, ax3 = plt.subplots(figsize=(12,8))
    ax3.plot(monthly_metrics['Order Date'], monthly_metrics['Sales Amount USD'], label='Revenue', marker='o')
    ax3.plot(monthly_metrics['Order Date'], monthly_metrics['Profit'], label='Profit', marker='o')
    ax3.yaxis.set_major_formatter(formatter)
    ax3.set_title('Monthly Sales vs Profit')
    ax3.set_xlabel('Month')
    ax3.set_ylabel('Sales Amount (USD)')
    ax3.legend()
    figs.append(fig3)
    plt.close(fig3)

    # Sales by Country
    Country_Sales = Full_Data.groupby('Store Country')['Sales Amount USD'].sum().reset_index().sort_values(by='Sales Amount USD', ascending=False)
    fig4 = px.choropleth(
        Country_Sales,
        locations='Store Country',
        locationmode='country names',
        color='Sales Amount USD',
        hover_name='Store Country',
        title='Sales by Country',
        width=1000,
        height=600
    )
    #figs.append(fig4) #Not appending into report as the current PDF function is not supporting plottly. Future work****
    #plt.close(fig4)

    # Sales by Country and State
    Grouped_Country_State = Full_Data.groupby(['Store Country', 'Store State'],observed=False)['Sales Amount USD'].sum().reset_index()
    Country_Totals = Grouped_Country_State.groupby('Store Country')['Sales Amount USD'].sum().sort_values()
    Grouped_Country_State['Label'] = Grouped_Country_State['Store Country'] + ' - ' + Grouped_Country_State['Store State']
    Country_Order = Country_Totals.index.tolist()
    Grouped_Country_State['Store Country'] = pd.Categorical(Grouped_Country_State['Store Country'], categories=Country_Order, ordered=True)
    Grouped_Country_State = Grouped_Country_State.sort_values(['Store Country', 'Sales Amount USD'], ascending=[True, True])
    palette = sns.color_palette("Spectral", n_colors=len(Country_Order))
    country_colors = dict(zip(Country_Order, palette))
    Grouped_Country_State['Color'] = Grouped_Country_State['Store Country'].astype(str).map(country_colors)

    fig5, ax5 = plt.subplots(figsize=(12, 10))
    bars = ax5.barh(Grouped_Country_State['Label'], Grouped_Country_State['Sales Amount USD'], color=Grouped_Country_State['Color'])
    ax5.set_title('Sales by Country and State')
    ax5.set_xlabel('Total Sales (USD)')
    ax5.set_ylabel('Country - State')
    ax5.xaxis.set_major_formatter(formatter)
    ax5.tick_params(axis='y', labelsize=8)
    max_sales = Country_Totals.max()
    for bar in bars:
        width = bar.get_width()
        label = f"${width * 1e-6:.2f}M"
        ax5.text(width + max_sales * 0.01, bar.get_y() + bar.get_height() / 2, label, va='center')
    plt.tight_layout()
    figs.append(fig5)
    plt.close(fig5)

    return figs

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Analyze_Stores(Full_Data=None, input_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/combined_data.pkl', return_figs=True):
    if Full_Data is None:
        Full_Data = pd.read_pickle(input_path)

    figs = []

    formatter = ticker.FuncFormatter(lambda x, pos: f'{x*1e-6:.2f}M')

    # 1. Top 10 and Bottom 10 Stores by Revenue
    Store_Sales = Full_Data.groupby('StoreKey')['Sales Amount USD'].sum().sort_values(ascending=False).reset_index()
    Store_Sales_Top10 = Store_Sales.head(10)
    Store_Sales_Bottom10 = Store_Sales.tail(10)

    fig1, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    sns.barplot(data=Store_Sales_Top10 , x='StoreKey', y='Sales Amount USD',
                order=Store_Sales_Top10.sort_values('Sales Amount USD', ascending=False).StoreKey, ax=ax1)
    ax1.yaxis.set_major_formatter(formatter)
    ax1.set_title('Top 10 Stores by Revenue')
    ax1.set_xlabel('Store Key')
    ax1.set_ylabel('Total Sales (USD)')
    for bar in ax1.patches:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2, height, f'{height*1e-6:.2f}M', ha='center', va='bottom')

    sns.barplot(data=Store_Sales_Bottom10 , x='StoreKey', y='Sales Amount USD',
                order=Store_Sales_Bottom10.sort_values('Sales Amount USD', ascending=True).StoreKey, ax=ax2)
    ax2.yaxis.set_major_formatter(formatter)
    ax2.set_title('Bottom 10 Stores by Revenue')
    ax2.set_xlabel('Store Key')
    ax2.set_ylabel('Total Sales (USD)')
    for bar in ax2.patches:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, height, f'{height*1e-6:.2f}M', ha='center', va='bottom')

    figs.append(fig1)
    plt.close(fig1)

    # 2. Top and Bottom 10 Stores by Average Order Value (AOV)
    Store_AOV = Full_Data.groupby('StoreKey').agg({'Sales Amount USD': 'sum','Order Number': pd.Series.nunique}).reset_index()
    Store_AOV['AOV'] = Store_AOV['Sales Amount USD'] / Store_AOV['Order Number']
    Store_AOV_Top10 = Store_AOV.sort_values('AOV', ascending=False).head(10)
    Store_AOV_Bottom10 = Store_AOV.sort_values('AOV', ascending=True).head(10)  # Use ascending True for bottom

    fig2, (ax3, ax4) = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    sns.barplot(data=Store_AOV_Top10, x='StoreKey', y='AOV',
                order=Store_AOV_Top10.sort_values('AOV', ascending=False).StoreKey, ax=ax3)
    ax3.set_title('Top 10 Stores by Average Order Value')
    ax3.set_xlabel('Store Key')
    ax3.set_ylabel('AOV (USD)')
    for bar in ax3.patches:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2, height, f'{height:.2f}', ha='center', va='bottom', rotation=45)

    sns.barplot(data=Store_AOV_Bottom10, x='StoreKey', y='AOV',
                order=Store_AOV_Bottom10.sort_values('AOV', ascending=True).StoreKey, ax=ax4)
    ax4.set_title('Bottom 10 Stores by Average Order Value')
    ax4.set_xlabel('Store Key')
    ax4.set_ylabel('AOV (USD)')
    for bar in ax4.patches:
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2, height, f'{height:.2f}', ha='center', va='bottom', rotation=45)

    figs.append(fig2)
    plt.close(fig2)

    # 3. Monthly Sales: Online vs Physical Stores
    Full_Data['Store Type'] = np.where(Full_Data['Square Meters'].isnull(), 'Online', 'Physical')
    Store_Type_Sales = Full_Data.groupby([Full_Data['Order Date'].dt.to_period('M'), 'Store Type'],observed=False)['Sales Amount USD'].sum().reset_index()
    Store_Type_Sales['Order Date'] = Store_Type_Sales['Order Date'].dt.to_timestamp()
    pivot_df = Store_Type_Sales.pivot(index='Order Date', columns='Store Type', values='Sales Amount USD').fillna(0)

    fig3, ax5 = plt.subplots(figsize=(12, 8))
    sns.lineplot(data=pivot_df, marker='o', ax=ax5)
    ax5.yaxis.set_major_formatter(formatter)
    ax5.set_title('Monthly Sales: Online vs Physical Stores')
    ax5.set_ylabel('Sales Amount (USD)')
    ax5.set_xlabel('Month')
    ax5.legend(title='Store Type')
    figs.append(fig3)
    plt.close(fig3)

    # 4. Average Sales by Store Age Group
    today = pd.to_datetime('2022-01-01') #As we have data till 2021
    Full_Data['Store Age (Years)'] = (today - Full_Data['Store Open Date']).dt.days / 365
    Full_Data['Store Age Group'] = pd.cut(Full_Data['Store Age (Years)'], bins=[0, 2, 5, 7, 10, 20], labels=['0-2', '2-5', '5-7', '7-10', '10+'])

    Age_Impact = Full_Data.groupby('Store Age Group',observed=False)['Sales Amount USD'].mean().reset_index()

    fig4, ax6 = plt.subplots(figsize=(12, 6))
    bars = sns.barplot(data=Age_Impact, x='Store Age Group', y='Sales Amount USD', ax=ax6)
    for bar in bars.patches:
        height = bar.get_height()
        label = f'${height:.2f}'
        if label != '$0.00':
            ax6.text(bar.get_x() + bar.get_width()/2, height, label, ha='center', va='bottom')
    ax6.set_title('Average Sales by Store Age Group')
    ax6.set_ylabel('Average Sales (USD)')
    ax6.set_xlabel('Store Age Group (Years)')
    figs.append(fig4)
    plt.close(fig4)

    return figs

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Analyze_Product_Performance(Full_Data=None, input_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/combined_data.pkl', return_figs=True):
    if Full_Data is None:
        Full_Data = pd.read_pickle(input_path)

    figs = []

    formatter = ticker.FuncFormatter(lambda x, pos: f'{x*1e-6:.2f}M')
    
    # 1. Top 10 and Bottom 10 Products by Revenue
    Product_Sales = Full_Data.groupby('Product Name',observed=False).agg({'Sales Amount USD':'sum','Profit':'sum'}).reset_index()

    Top10_Selling_Products = Product_Sales.sort_values('Sales Amount USD', ascending=False).head(10)
    Bottom10_Selling_Products = Product_Sales.sort_values('Sales Amount USD', ascending=True).head(10)

    fig1, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,6))
    bars1 = ax1.barh(Top10_Selling_Products['Product Name'], Top10_Selling_Products['Sales Amount USD'])
    ax1.set_title('Top 10 Products by Revenue')
    ax1.set_xlabel('Revenue (USD)')
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x*1e-6:.2f}M'))
    ax1.tick_params(axis='y', labelsize=6)  # reduced font size
    for bar in bars1:
        width = bar.get_width()
        label = f"${width * 1e-6:.2f}M"
        ax1.text(width + Top10_Selling_Products['Sales Amount USD'].max() * 0.01, bar.get_y() + bar.get_height() / 2, label, va='center')

    bars2 = ax2.barh(Bottom10_Selling_Products['Product Name'], Bottom10_Selling_Products['Sales Amount USD'])
    ax2.set_title('Bottom 10 Products by Revenue')
    ax2.set_xlabel('Revenue (USD)')
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x*1e-3:.2f}K'))
    ax2.tick_params(axis='y', labelsize=6)  # reduced font size
    for bar in bars2:
        width = bar.get_width()
        label = f"${width * 1e-3:.2f}K"
        ax2.text(width + Bottom10_Selling_Products['Sales Amount USD'].max() * 0.01, bar.get_y() + bar.get_height() / 2, label, va='center')

    plt.tight_layout()
    figs.append(fig1)
    plt.close(fig1)

    # 2. Sales Distribution by Product Category (Pie Chart)
    Product_Category_Sales = Full_Data.groupby('Category')['Sales Amount USD'].sum()
    fig2, ax3 = plt.subplots(figsize=(8,8))
    ax3.pie(Product_Category_Sales, labels=Product_Category_Sales.index, autopct='%1.2f%%')
    ax3.set_title('Sales Distribution by Product Category')
    figs.append(fig2)
    plt.close(fig2)

    # 3. Sales by Category and Sub-Category (Horizontal Bar)
    grouped = Full_Data.groupby(['Category', 'Subcategory'],observed=False)['Sales Amount USD'].sum().reset_index()
    category_totals = grouped.groupby('Category')['Sales Amount USD'].sum().sort_values()
    grouped['Label'] = grouped['Category'] + ' - ' + grouped['Subcategory']
    category_order = category_totals.index.tolist()
    grouped['Category'] = pd.Categorical(grouped['Category'], categories=category_order, ordered=True)
    grouped = grouped.sort_values(['Category', 'Sales Amount USD'], ascending=[True, True])
    palette = sns.color_palette("Spectral", n_colors=len(category_order))
    category_colors = dict(zip(category_order, palette))
    grouped['Color'] = grouped['Category'].astype(str).map(category_colors)

    fig3, ax4 = plt.subplots(figsize=(12,8))
    bars = ax4.barh(grouped['Label'], grouped['Sales Amount USD'], color=grouped['Color'])
    ax4.set_title('Sales by Category and Sub-Category')
    ax4.set_xlabel('Total Sales (USD)')
    ax4.set_ylabel('Category - Sub-Category')
    ax4.xaxis.set_major_formatter(formatter)
    ax4.tick_params(axis='y', labelsize=8)
    for bar in bars:
        width = bar.get_width()
        label = f"${width * 1e-6:.2f}M"
        ax4.text(width + category_totals.max() * 0.01, bar.get_y() + bar.get_height() / 2, label, va='center')
    plt.tight_layout()
    figs.append(fig3)
    plt.close(fig3)

    # 4. Top 3 Brands in each Store Country
    Brand_Preference = Full_Data.groupby(['Store Country', 'Brand'],observed=False)['Sales Amount USD'].sum().reset_index()
    Top3_Brands = Brand_Preference.sort_values(['Store Country', 'Sales Amount USD'], ascending=[True, False])
    Top3_Brands = Top3_Brands.groupby('Store Country').head(3)

    fig4, ax5 = plt.subplots(figsize=(12,6))
    bars = sns.barplot(data=Top3_Brands, x='Store Country', y='Sales Amount USD', hue='Brand', ax=ax5)
    ax5.yaxis.set_major_formatter(formatter)
    for bar in bars.patches:
        height = bar.get_height()
        label = f'${height * 1e-6:.2f}M'
        if label != '$0.00':
            ax5.text(bar.get_x() + bar.get_width() / 2, height, label, ha='center', va='bottom', rotation=90)
    ax5.set_title('Top 3 Brands in each Store Country')
    ax5.set_xlabel('Store Country')
    ax5.set_ylabel('Sales Amount (USD)')
    figs.append(fig4)
    plt.close(fig4)

    return figs

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Analyze_Delivery_Time(Full_Data=None, input_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/combined_data.pkl', return_figs=True):
    if Full_Data is None:
        Full_Data = pd.read_pickle(input_path)

    # 1. Filter out rows with missing Delivery Date
    Delivered_Orders = Full_Data[Full_Data['Delivery Date'].notnull()].copy()

    # 2. Calculate delivery time (in days)
    Delivered_Orders['Delivery_Time_Days'] = (Delivered_Orders['Delivery Date'] - Delivered_Orders['Order Date']).dt.days

    # 3. Create plot and return figure
    fig, ax = plt.subplots(figsize=(8,5))
    sns.histplot(Delivered_Orders['Delivery_Time_Days'], bins=30, kde=True, ax=ax)
    ax.set_title('Distribution of Order Delivery Time (Days) for Online Orders')
    ax.set_xlabel('Delivery Time (Days)')
    ax.set_ylabel('Number of Orders')

    return [fig]

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Analyze_Customers(Full_Data=None, Customers=None, input_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/combined_data.pkl', customers_path='ETL/tmp/customers_cleaned.pkl', return_figs=True):
    if Full_Data is None:
        Full_Data = pd.read_pickle(input_path)
    if Customers is None:
        Customers = pd.read_pickle(customers_path)

    figs = []
    
    # 1. Gender Distribution (Donut)
    gender_counts = Full_Data['Gender'].value_counts()
    fig2, ax2 = plt.subplots(figsize=(4,4))
    wedges, texts, autotexts = ax2.pie(gender_counts.values, labels=gender_counts.index, autopct='%1.1f%%', startangle=45, colors=['skyblue','lightpink'], wedgeprops={'width':0.4})
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    ax2.add_artist(centre_circle)
    ax2.set_title('Gender Distribution')
    figs.append(fig2)
    plt.close(fig2)

    # 2. Sales by Age Group and Gender
    # Calculate Age and Age Group for further analysis
    today = pd.Timestamp.today()
    Full_Data['Age'] = Full_Data['Birthday'].apply(lambda x: (today - x).days // 365)
    bins = [0, 20, 30, 40, 50, 60, 100]
    labels = ['<20', '20-29', '30-39', '40-49', '50-59', '60+']
    Full_Data['Age_Group'] = pd.cut(Full_Data['Age'], bins=bins, labels=labels)

    Age_Gender_Sales = Full_Data.groupby(['Age_Group', 'Gender'],observed=False)['Sales Amount USD'].mean().reset_index()
    fig3, ax3 = plt.subplots(figsize=(12,6))
    bars = sns.barplot(data=Age_Gender_Sales, x='Age_Group', y='Sales Amount USD', hue='Gender', ax=ax3)
    ax3.set_title('Sales by Age Group and Gender')
    ax3.set_xlabel('Age Group')
    ax3.set_ylabel("Avg Sales")
    for bar in bars.patches:
        height = bar.get_height()
        label = f'${height:.2f}'
        if label != '$0.00':
            ax3.text(bar.get_x() + bar.get_width() / 2, height, label, ha='center', va='bottom', rotation=5)
    figs.append(fig3)
    plt.close(fig3)
    
    # 3. Top 10 Repeat Customers
    # Step 1: Count number of purchases and total sales per customer
    Repeat_Customers = Full_Data.groupby('CustomerKey').agg({
        'Order Date': 'nunique',        # Purchase count (based on unique order dates)
        'Sales Amount USD': 'sum'       # Total sales
    }).reset_index()
    Repeat_Customers.rename(columns={'Order Date':'Purchase_Count','Sales Amount USD':'Total_Sales'}, inplace=True)

    # Step 2: Calculate repeat purchase rate
    repeat_purchase_rate = Repeat_Customers[Repeat_Customers['Purchase_Count'] > 1]['CustomerKey'].nunique() / Repeat_Customers['CustomerKey'].nunique()
    #print(f"Repeat Purchase Rate: {repeat_purchase_rate:.2%}")
    Total_Customers = Customers['CustomerKey'].nunique()
    # Create separate figure with repeat purchase rate text
    fig_rate, ax_rate = plt.subplots(figsize=(6, 2))
    ax_rate.text(0.5, 0.6, f"Total Customers:\n{Total_Customers:,}", fontsize=16, ha='center', va='center')
    ax_rate.text(0.5, 0.4, f"Repeat Purchase Rate:\n{repeat_purchase_rate:.2%}", fontsize=16, ha='center', va='center')
    ax_rate.axis('off')
    figs.append(fig_rate)
    plt.close(fig_rate)

    Repeat = Repeat_Customers[Repeat_Customers['Purchase_Count'] > 1]
    Repeat_Customer_Detail = pd.merge(Customers, Repeat, on='CustomerKey', how='inner')
    Repeat_Customer_Detail = Repeat_Customer_Detail.sort_values(by=['Purchase_Count','Total_Sales'], ascending=[False,False])
    Top10_Repeat_Customers = Repeat_Customer_Detail.head(10)
    # If purchase count is same then sort by Total sales
    Top10_Repeat_Customers = Top10_Repeat_Customers.sort_values(by=['Purchase_Count','Total_Sales'], ascending=[True,True])

    # Plot: Top 10 Repeat Customers 
    fig1, ax1 = plt.subplots(figsize=(12,6))
    barh = ax1.barh(Top10_Repeat_Customers['Customer Name'], Top10_Repeat_Customers['Purchase_Count'])
    ax1.set_title('Top 10 Repeat Customers')
    ax1.set_xlabel('Repeat Frequency')
    ax1.set_ylabel('Customer Name')
    for bar in barh:
        width = bar.get_width()
        label = width
        ax1.text(width + Top10_Repeat_Customers['Purchase_Count'].max() * 0.01, bar.get_y() + bar.get_height() / 2, label, va='center')
    figs.append(fig1)
    plt.close(fig1)

    return figs

def create_cover_page():
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.axis('off')  # Hide axes

    today = datetime.date.today().strftime("%B %d, %Y")

    # Add Title
    ax.text(0.5, 0.8, "Global Electronics Retailer Report", fontsize=25, ha='center', va='center', weight='bold')

    # Add Subtitle
    ax.text(0.5, 0.65, "Comprehensive Sales, Store, Product, and Customer Analysis", fontsize=16, ha='center', va='center')

    # Add Metadata
    ax.text(0.5, 0.45, f"Prepared by: Drishti Patel\nDate: {today}", fontsize=12, ha='center', va='center')

    return fig

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def save_figures_to_pdf(pdf_path, figures_dict):
    """
    Save matplotlib figures from your analysis functions into a single PDF file.

    Args:
        pdf_path (str): Path to save the PDF file.
        figures_dict (dict): Dictionary where keys are section titles (str)
                             and values are lists of matplotlib figures.
    """
    with PdfPages(pdf_path) as pdf:
        # Cover page
        cover_fig = create_cover_page()
        pdf.savefig(cover_fig)
        plt.close(cover_fig)
        
        for section, figs in figures_dict.items():
            figsize=(12, 8)
            # title banner at top
            title_fig = plt.figure(figsize=(figsize[0],figsize[1])) 
            plt.text(0.5, 0.5, section, fontsize=25, ha='center', va='center')
            plt.axis('off')
            pdf.savefig(title_fig)
            plt.close(title_fig)

            # Save all figures in this section
            for fig in figs:
                fig.set_size_inches(figsize)  # Force resize
                pdf.savefig(fig)
                plt.close(fig)

def analyze_and_report(pdf_path='/opt/airflow/Global_Electronics_Retailer/Output/Global_Electronics_Retailer_Report.pdf'):
    figures_dict = {}

    # Load combined data once inside this function
    combined_data = pd.read_pickle('/opt/airflow/Global_Electronics_Retailer/ETL/tmp/combined_data.pkl')
    customers_data = pd.read_pickle('/opt/airflow/Global_Electronics_Retailer/ETL/tmp/customers_cleaned.pkl')

    figures_dict['Sales Analysis'] = Analyze_Sales(Full_Data=combined_data)
    figures_dict['Store Analysis'] = Analyze_Stores(Full_Data=combined_data)
    figures_dict['Product Performance'] = Analyze_Product_Performance(Full_Data=combined_data)
    figures_dict['Delivery Time'] = Analyze_Delivery_Time(Full_Data=combined_data)
    figures_dict['Customer Analysis'] = Analyze_Customers(Full_Data=combined_data, Customers=customers_data)

    save_figures_to_pdf(pdf_path, figures_dict)