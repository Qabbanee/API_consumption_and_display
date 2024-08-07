import matplotlib.pyplot as plt
import pandas as pd
import sqlite3


# Connect to the database
conn = sqlite3.connect('database.db')

#average price per asset where direction is 'UP'
query = ''' SELECT assetId , AVG(price) as avg_price
            FROM series
            WHERE direction = 'Up'
            GROUP BY assetId
          '''

df = pd.read_sql_query(query, conn)
ax=df.plot(kind='bar', x='assetId', y='avg_price', figsize=(5, 11))
plt.title('Average Price per Asset where Direction is UP')
plt.ylabel('Average Price')
plt.xlabel('Asset ID')

for p in ax.patches:
    ax.annotate(str(p.get_height()), (p.get_x() * 1.005, p.get_height() * 1.005))
plt.savefig('plot1.png')
plt.show()


#total quantities traded for each asset based on the data
query = ''' SELECT  s.assetId,
    SUM(p.quantity) AS total_quantity
    FROM positions p
    JOIN series s ON p.series_id = s.id
    GROUP BY s.assetId
'''

df = pd.read_sql_query(query, conn)
ax = df.plot(kind='bar', x='assetId', y='total_quantity', color='skyblue', figsize=(5, 11))
plt.title('Total Quantities Traded for Each Asset')
plt.ylabel('Total Quantity')
plt.xlabel('Asset ID')
# Annotate each bar with its height value
for p in ax.patches:
    ax.annotate(str(p.get_height()), (p.get_x() * 1.005, p.get_height() * 1.005))
plt.savefig('plot2.png')
plt.show()


#time series of prices for a specific asset= 'TestAssetSE1'
query = ''' SELECT price, startInterval
            FROM series
            WHERE assetId = 'TestAssetSE1'
          '''

time_series_data = pd.read_sql_query(query, conn)
time_series_data['startInterval'] = pd.to_datetime(time_series_data['startInterval'])
# Plotting the time series of prices for "TestAssetSE1"
plt.figure(figsize=(10, 5))
plt.plot(time_series_data['startInterval'], time_series_data['price'], marker='o')
plt.title('Time Series of Prices for TestAssetSE1')
plt.xlabel('Time Interval')
plt.ylabel('Price (€)')
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('plot3.png')
plt.show()


