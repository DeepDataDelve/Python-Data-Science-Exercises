# import python's number crunchers
from pandas_datareader import data as web
import pandas as pd
import numpy as np

assets =  ['AAPL', 'GM', 'GE', 'FB', 'WMT'] 

df = pd.DataFrame()  

for stock in assets:
    df[stock] = web.DataReader(stock, data_source='yahoo',
                               start='2023-1-1' , end='2024-1-1')['Adj Close']

d_returns = df.pct_change()  

# returns covariance for data set, then multiply by 252 to annualize
cov_matrix_d = d_returns.cov()
cov_matrix_a = cov_matrix_d * 250

weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])  # assign equal weights

# calculate the variance and risk of the portfolo
port_variance = np.dot(weights.T, np.dot(cov_matrix_a, weights))
port_volatility = np.sqrt(np.dot(weights.T, np.dot(cov_matrix_a, weights)))

percent_var = str(round(port_variance, 4) * 100) + '%'
percent_vols = str(round(port_volatility, 4) * 100) + '%'

print('Variance of Portfolio is {}, Portfolio Risk is {}'
      .format(percent_var, percent_vols))