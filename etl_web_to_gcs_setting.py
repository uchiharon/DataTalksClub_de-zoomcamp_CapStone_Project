from etl_web_to_gcs import todo




sheet_names = ['Page 1 Generation and Fuel Data',
 'Page 1 Puerto Rico',
 'Page 1 Energy Storage',
 'Page 2 Stocks Data',
 'Page 2 Oil Stocks Data',
 'Page 3 Boiler Fuel Data',
 'Page 4 Generator Data',
 'Page 5 Fuel Receipts and Costs',
 'Page 6 Plant Frame',
 'Page 6 Plant Frame Puerto Rico',
 'Page 7 File Layout']



check = {'Page 1 Generation and Fuel Data': todo('Page 1 Generation and Fuel Data'),
 'Page 1 Puerto Rico': todo('Page 1 Puerto Rico'),
 'Page 1 Energy Storage': todo('Page 1 Energy Storage'),
 'Page 2 Stocks Data': todo('Page 2 Stocks Data'),
 'Page 2 Oil Stocks Data': todo('Page 2 Oil Stocks Data'),
 'Page 3 Boiler Fuel Data': todo('Page 3 Boiler Fuel Data'),
 'Page 4 Generator Data': todo('Page 4 Generator Data'),
 'Page 5 Fuel Receipts and Costs': todo('Page 5 Fuel Receipts and Costs'),
 'Page 6 Plant Frame': todo('Page 6 Plant Frame'),
 'Page 6 Plant Frame Puerto Rico': todo('Page 6 Plant Frame Puerto Rico'),
 'Page 7 File Layout': todo('Page 7 File Layout')}
