import os
from flask import Flask, render_template, request
import pymysql
import plotly.graph_objs as go
import json
import plotly
from decimal import Decimal

app = Flask(__name__, static_url_path='')


db_config = {
    'host': os.environ.get('DB_HOST', 'mysql.clarksonmsda.org'),
    'user': os.environ.get('DB_USER', 'anpatil'),
    'passwd': os.environ.get('DB_PASS', 'H3ADSHOT'),  
    'db': os.environ.get('DB_NAME', 'anpatil_zagimore_datawarehouse'),
    'autocommit': True
}

def db_connect():
    return pymysql.connect(**db_config)

@app.route('/')
def index():
    with db_connect() as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT StoreKey, StoreID FROM Store_Dimension ORDER BY StoreID;")
            stores = cur.fetchall()

            cur.execute("SELECT CustomerName FROM Customer_Dimension ORDER BY CustomerName;")
            customers = cur.fetchall()

            cur.execute("""
                SELECT pcd.CategoryName, SUM(pcaf.RevenueGenerated) AS TotalRevenue 
                FROM ProductCategoryAggregateFact pcaf
                JOIN ProductCategoryDimension pcd ON pcaf.CategoryKey = pcd.CategoryKey 
                GROUP BY pcd.CategoryName;
            """)
            category_data = cur.fetchall()

            cur.execute("""
                SELECT sd.StoreID, SUM(cf.RevenueGenerated) AS TotalRevenue 
                FROM Store_Dimension sd
                JOIN CoreFact cf ON sd.StoreKey = cf.StoreKey 
                GROUP BY sd.StoreID;
            """)
            store_performance_data = cur.fetchall()

            cur.execute("""
                SELECT sd.RegionName, sd.StoreID, SUM(cf.RevenueGenerated) AS TotalRevenue 
                FROM Store_Dimension sd
                JOIN CoreFact cf ON sd.StoreKey = cf.StoreKey 
                GROUP BY sd.RegionName, sd.StoreID;
            """)
            heatmap_data = cur.fetchall()

    # Custom JSON encoder to handle Decimal objects
    class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, Decimal):
                return str(o)  # Convert Decimal to string
            return super().default(o)

    # Serialize data to JSON using custom encoder
    category_json = json.dumps(category_data, cls=DecimalEncoder)
    store_performance_json = json.dumps(store_performance_data, cls=DecimalEncoder)
    heatmap_json = json.dumps(heatmap_data, cls=DecimalEncoder)

    return render_template('index.html', stores=stores, customers=customers, 
                           category_data=category_json, store_performance_data=store_performance_json,
                           heatmap_data=heatmap_json)



@app.route('/store_revenue', methods=['POST'])
def store_revenue():
    store_key = request.form.get('store_key')
    conn = db_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    # Query to fetch revenue data by product name
    revenue_query = """
        SELECT p.ProductName, SUM(f.RevenueGenerated) AS RevenueGenerated
        FROM CoreFact AS f
        JOIN Product_Dimension AS p ON f.ProductKey = p.ProductKey
        WHERE f.StoreKey = %s
        GROUP BY p.ProductName
        ORDER BY p.ProductName;
    """

    cur.execute(revenue_query, (store_key,))
    revenue_results = cur.fetchall()

    # If no revenue data is found, prepare empty data for the graph
    if not revenue_results:
        revenue_data = {'x': [], 'y': [], 'type': 'scatter', 'mode': 'lines+markers'}
    else:
        revenue_data = {
            'x': [result['ProductName'] for result in revenue_results],
            'y': [float(result['RevenueGenerated']) for result in revenue_results],
            'type': 'scatter',
            'mode': 'lines+markers'
        }

    # Query to fetch units sold data by product category
    units_query = """
        SELECT p.CategoryName, SUM(f.UnitsSold) AS UnitsSold
        FROM CoreFact AS f
        JOIN Product_Dimension AS p ON f.ProductKey = p.ProductKey
        WHERE f.StoreKey = %s
        GROUP BY p.CategoryName
        ORDER BY p.CategoryName;
    """

    cur.execute(units_query, (store_key,))
    units_results = cur.fetchall()

    # If no units sold data is found, prepare empty data for the graph
    if not units_results:
        units_data = {'x': [], 'y': [], 'type': 'scatter', 'mode': 'lines+markers'}
    else:
        units_data = {
            'x': [result['CategoryName'] for result in units_results],
            'y': [float(result['UnitsSold']) for result in units_results],
            'type': 'scatter',
            'mode': 'lines+markers'
        }

    # Convert data to JSON format for Plotly
    revenue_graph_data = json.dumps([revenue_data], cls=plotly.utils.PlotlyJSONEncoder)
    units_graph_data = json.dumps([units_data], cls=plotly.utils.PlotlyJSONEncoder)

    cur.close()
    conn.close()

    return render_template('store_revenue.html', revenue_graph_data=revenue_graph_data, units_graph_data=units_graph_data)


@app.route('/customer_revenue', methods=['POST'])
def customer_revenue():
    customer_name = request.form.get('customer_name')
    conn = db_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)

    # Query to fetch revenue by store ID
    store_revenue_query = """
        SELECT d.StoreID, SUM(f.RevenueGenerated) AS RevenueGenerated
        FROM CoreFact f
        JOIN Customer_Dimension c ON f.CustomerKey = c.CustomerKey
        JOIN Store_Dimension d ON f.StoreKey = d.StoreKey
        WHERE c.CustomerName = %s
        GROUP BY d.StoreID
        ORDER BY d.StoreID;
    """

    cur.execute(store_revenue_query, (customer_name,))
    store_results = cur.fetchall()
    
    # If no data is found for store revenue, set empty data
    if not store_results:
        store_data = {'x': [], 'y': [], 'type': 'scatter', 'mode': 'lines+markers'}
    else:
        store_data = {
            'x': [result['StoreID'] for result in store_results],
            'y': [float(result['RevenueGenerated']) for result in store_results],
            'type': 'scatter',
            'mode': 'lines+markers'
        }

    # Query to fetch revenue by category name and revenue source type
    category_revenue_query = """
        SELECT p.CategoryName, f.RevenueSourceType, SUM(f.RevenueGenerated) AS RevenueGenerated
        FROM ProductCategoryAggregateFact f
        JOIN ProductCategoryDimension p ON f.CategoryKey = p.CategoryKey
        JOIN Customer_Dimension c ON f.CustomerKey = c.CustomerKey
        WHERE c.CustomerName = %s
        GROUP BY p.CategoryName, f.RevenueSourceType
        ORDER BY p.CategoryName;
    """

    cur.execute(category_revenue_query, (customer_name,))
    category_results = cur.fetchall()

    # If no data is found for category revenue, set empty data
    if not category_results:
        category_data = []
    else:
        # Prepare data for the plot
        category_data = {}
        for result in category_results:
            category_name = result['CategoryName']
            revenue_source = result['RevenueSourceType']
            revenue_generated = float(result['RevenueGenerated'])

            if category_name not in category_data:
                category_data[category_name] = {
                    'x': [],
                    'y': [],
                    'name': category_name,
                    'type': 'bar',
                    'text': [],
                    'textposition': 'auto'
                }

            category_data[category_name]['x'].append(revenue_source)
            category_data[category_name]['y'].append(revenue_generated)
            category_data[category_name]['text'].append(f'{category_name} - {revenue_source}')

    # Convert category data to a list for Plotly
    category_graph_data = list(category_data.values())

    cur.close()
    conn.close()

    # Render the template with both graph data
    return render_template('customer_revenue.html', store_data=json.dumps([store_data], cls=plotly.utils.PlotlyJSONEncoder), category_data=category_graph_data)





if __name__ == "__main__":
    app.run(host=os.getenv('HOSTIP', '127.0.0.1'), debug=os.getenv('FLASKDEBUG', 'True') == 'True', port=int(os.getenv('PORT', '5000')))
