# Cumulative Production Calculations
This program will take in oil/gas/water production values from a data source, a .csv in this case,
and calculate cumulative production, by well, for a series of timeframes given (here, those will be found in a list called "dayslist"). This
timeframe metric is used by many oil and gas companies as a comparison for well performance.  You can edit the timeframes to anything you like, but the ones I have
used here are ones that have been common in my career. Depending on where you pull your production data or what state site you have used, you will probably 
have to change the ProdData column names.  Feel free to drop the line 'ProdData = ProdData.drop(["Unnamed: 0"], axis=1)', as that was something 
I had to do for the specific .csv with which I was working.
