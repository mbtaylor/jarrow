import feather
import pandas as pd
import numpy as np
arr = np.random.randn(10000000) # 10% nulls
arr[::10] = np.nan
df = pd.DataFrame({'column_{0}'.format(i): arr for i in range(10)})
feather.write_dataframe(df, 'big.fea')
