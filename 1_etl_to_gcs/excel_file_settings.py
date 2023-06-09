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

old_sheet_names = ['Page 1 Generation and Fuel Data',
 'Page 1 Energy Storage',
 'Page 2 Stocks Data',
 'Page 2 Oil Stocks Data',
 'Page 3 Boiler Fuel Data',
 'Page 4 Generator Data',
 'Page 5 Fuel Receipts and Costs',
 'Page 6 Plant Frame',
 'Page 7 File Layout']




####################################################################################################################
"""
Desired Columns
"""
####################################################################################################################

sheet_0_columns = ['Plant Id',
 'Combined Heat And\nPower Plant',
 'Nuclear Unit Id',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'EIA Sector Number',
 'Sector Name',
 'Reported\nPrime Mover',
 'Reported\nFuel Type Code',
 'AER\nFuel Type Code',
 'Physical\nUnit Label',
 'Quantity\nJanuary',
 'Quantity\nFebruary',
 'Quantity\nMarch',
 'Quantity\nApril',
 'Quantity\nMay',
 'Quantity\nJune',
 'Quantity\nJuly',
 'Quantity\nAugust',
 'Quantity\nSeptember',
 'Quantity\nOctober',
 'Quantity\nNovember',
 'Quantity\nDecember',
 'Elec_Quantity\nJanuary',
 'Elec_Quantity\nFebruary',
 'Elec_Quantity\nMarch',
 'Elec_Quantity\nApril',
 'Elec_Quantity\nMay',
 'Elec_Quantity\nJune',
 'Elec_Quantity\nJuly',
 'Elec_Quantity\nAugust',
 'Elec_Quantity\nSeptember',
 'Elec_Quantity\nOctober',
 'Elec_Quantity\nNovember',
 'Elec_Quantity\nDecember',
 'MMBtuPer_Unit\nJanuary',
 'MMBtuPer_Unit\nFebruary',
 'MMBtuPer_Unit\nMarch',
 'MMBtuPer_Unit\nApril',
 'MMBtuPer_Unit\nMay',
 'MMBtuPer_Unit\nJune',
 'MMBtuPer_Unit\nJuly',
 'MMBtuPer_Unit\nAugust',
 'MMBtuPer_Unit\nSeptember',
 'MMBtuPer_Unit\nOctober',
 'MMBtuPer_Unit\nNovember',
 'MMBtuPer_Unit\nDecember',
 'Tot_MMBtu\nJanuary',
 'Tot_MMBtu\nFebruary',
 'Tot_MMBtu\nMarch',
 'Tot_MMBtu\nApril',
 'Tot_MMBtu\nMay',
 'Tot_MMBtu\nJune',
 'Tot_MMBtu\nJuly',
 'Tot_MMBtu\nAugust',
 'Tot_MMBtu\nSeptember',
 'Tot_MMBtu\nOctober',
 'Tot_MMBtu\nNovember',
 'Tot_MMBtu\nDecember',
 'Elec_MMBtu\nJanuary',
 'Elec_MMBtu\nFebruary',
 'Elec_MMBtu\nMarch',
 'Elec_MMBtu\nApril',
 'Elec_MMBtu\nMay',
 'Elec_MMBtu\nJune',
 'Elec_MMBtu\nJuly',
 'Elec_MMBtu\nAugust',
 'Elec_MMBtu\nSeptember',
 'Elec_MMBtu\nOctober',
 'Elec_MMBtu\nNovember',
 'Elec_MMBtu\nDecember',
 'Netgen\nJanuary',
 'Netgen\nFebruary',
 'Netgen\nMarch',
 'Netgen\nApril',
 'Netgen\nMay',
 'Netgen\nJune',
 'Netgen\nJuly',
 'Netgen\nAugust',
 'Netgen\nSeptember',
 'Netgen\nOctober',
 'Netgen\nNovember',
 'Netgen\nDecember',
 'YEAR']






sheet_1_columns = ['Plant Id',
 'Combined Heat And\nPower Plant',
 'Nuclear Unit Id',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'EIA Sector Number',
 'Sector Name',
 'Reported\nPrime Mover',
 'Reported\nFuel Type Code',
 'AER\nFuel Type Code',
 'Physical\nUnit Label',
 'Quantity\nJanuary',
 'Quantity\nFebruary',
 'Quantity\nMarch',
 'Quantity\nApril',
 'Quantity\nMay',
 'Quantity\nJune',
 'Quantity\nJuly',
 'Quantity\nAugust',
 'Quantity\nSeptember',
 'Quantity\nOctober',
 'Quantity\nNovember',
 'Quantity\nDecember',
 'Elec_Quantity\nJanuary',
 'Elec_Quantity\nFebruary',
 'Elec_Quantity\nMarch',
 'Elec_Quantity\nApril',
 'Elec_Quantity\nMay',
 'Elec_Quantity\nJune',
 'Elec_Quantity\nJuly',
 'Elec_Quantity\nAugust',
 'Elec_Quantity\nSeptember',
 'Elec_Quantity\nOctober',
 'Elec_Quantity\nNovember',
 'Elec_Quantity\nDecember',
 'MMBtuPer_Unit\nJanuary',
 'MMBtuPer_Unit\nFebruary',
 'MMBtuPer_Unit\nMarch',
 'MMBtuPer_Unit\nApril',
 'MMBtuPer_Unit\nMay',
 'MMBtuPer_Unit\nJune',
 'MMBtuPer_Unit\nJuly',
 'MMBtuPer_Unit\nAugust',
 'MMBtuPer_Unit\nSeptember',
 'MMBtuPer_Unit\nOctober',
 'MMBtuPer_Unit\nNovember',
 'MMBtuPer_Unit\nDecember',
 'Tot_MMBtu\nJanuary',
 'Tot_MMBtu\nFebruary',
 'Tot_MMBtu\nMarch',
 'Tot_MMBtu\nApril',
 'Tot_MMBtu\nMay',
 'Tot_MMBtu\nJune',
 'Tot_MMBtu\nJuly',
 'Tot_MMBtu\nAugust',
 'Tot_MMBtu\nSeptember',
 'Tot_MMBtu\nOctober',
 'Tot_MMBtu\nNovember',
 'Tot_MMBtu\nDecember',
 'Elec_MMBtu\nJanuary',
 'Elec_MMBtu\nFebruary',
 'Elec_MMBtu\nMarch',
 'Elec_MMBtu\nApril',
 'Elec_MMBtu\nMay',
 'Elec_MMBtu\nJune',
 'Elec_MMBtu\nJuly',
 'Elec_MMBtu\nAugust',
 'Elec_MMBtu\nSeptember',
 'Elec_MMBtu\nOctober',
 'Elec_MMBtu\nNovember',
 'Elec_MMBtu\nDecember',
 'Netgen\nJanuary',
 'Netgen\nFebruary',
 'Netgen\nMarch',
 'Netgen\nApril',
 'Netgen\nMay',
 'Netgen\nJune',
 'Netgen\nJuly',
 'Netgen\nAugust',
 'Netgen\nSeptember',
 'Netgen\nOctober',
 'Netgen\nNovember',
 'Netgen\nDecember',
 'YEAR']








sheet_2_columns = ['Plant Id',
 'Combined Heat And\nPower Plant',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'EIA Sector Number',
 'Sector Name',
 'Reported\nPrime Mover',
 'Reported\nFuel Type Code',
 'AER\nFuel Type Code',
 'Physical\nUnit Label',
 'Quantity\nJanuary',
 'Quantity\nFebruary',
 'Quantity\nMarch',
 'Quantity\nApril',
 'Quantity\nMay',
 'Quantity\nJune',
 'Quantity\nJuly',
 'Quantity\nAugust',
 'Quantity\nSeptember',
 'Quantity\nOctober',
 'Quantity\nNovember',
 'Quantity\nDecember',
 'Elec_Quantity\nJanuary',
 'Elec_Quantity\nFebruary',
 'Elec_Quantity\nMarch',
 'Elec_Quantity\nApril',
 'Elec_Quantity\nMay',
 'Elec_Quantity\nJune',
 'Elec_Quantity\nJuly',
 'Elec_Quantity\nAugust',
 'Elec_Quantity\nSeptember',
 'Elec_Quantity\nOctober',
 'Elec_Quantity\nNovember',
 'Elec_Quantity\nDecember',
 'Grossgen\nJanuary',
 'Grossgen\nFebruary',
 'Grossgen\nMarch',
 'Grossgen\nApril',
 'Grossgen\nMay',
 'Grossgen\nJune',
 'Grossgen\nJuly',
 'Grossgen\nAugust',
 'Grossgen\nSeptember',
 'Grossgen\nOctober',
 'Grossgen\nNovember',
 'Grossgen\nDecember',
 'Netgen\nJanuary',
 'Netgen\nFebruary',
 'Netgen\nMarch',
 'Netgen\nApril',
 'Netgen\nMay',
 'Netgen\nJune',
 'Netgen\nJuly',
 'Netgen\nAugust',
 'Netgen\nSeptember',
 'Netgen\nOctober',
 'Netgen\nNovember',
 'Netgen\nDecember',
 'YEAR']



sheet_3_columns = ['Census Division\nand State',
 'Coal\nJanuary',
 'Coal\nFebruary',
 'Coal\nMarch',
 'Coal\nApril',
 'Coal\nMay',
 'Coal\nJune',
 'Coal\nJuly',
 'Coal\nAugust',
 'Coal\nSeptember',
 'Coal\nOctober',
 'Coal\nNovember',
 'Coal\nDecember',
 'Oil\nJanuary',
 'Oil\nFebruary',
 'Oil\nMarch',
 'Oil\nApril',
 'Oil\nMay',
 'Oil\nJune',
 'Oil\nJuly',
 'Oil\nAugust',
 'Oil\nSeptember',
 'Oil\nOctober',
 'Oil\nNovember',
 'Oil\nDecember',
 'PetCoke\nJanuary',
 'PetCoke\nFebruary',
 'PetCoke\nMarch',
 'PetCoke\nApril',
 'PetCoke\nMay',
 'PetCoke\nJune',
 'PetCoke\nJuly',
 'PetCoke\nAugust',
 'PetCoke\nSeptember',
 'PetCoke\nOctober',
 'PetCoke\nNovember',
 'PetCoke\nDecember']



sheet_4_columns = ['Plant Id',
 'Combined Heat And\nPower Plant',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'EIA Sector Number',
 'Sector Name',
 'Reported\nFuel Type Code',
 'AER\nFuel Type Code',
 'Physical\nUnit Label',
 'Quantity\nJanuary',
 'Quantity\nFebruary',
 'Quantity\nMarch',
 'Quantity\nApril',
 'Quantity\nMay',
 'Quantity\nJune',
 'Quantity\nJuly',
 'Quantity\nAugust',
 'Quantity\nSeptember',
 'Quantity\nOctober',
 'Quantity\nNovember',
 'Quantity\nDecember',
 'YEAR']





sheet_5_columns = ['Plant Id',
 'Combined Heat And\nPower Plant',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'EIA Sector Number',
 'Sector Name',
 'Reported\nFuel Type Code',
 'AER\nFuel Type Code',
 'Physical\nUnit Label',
 'Quantity\nJanuary',
 'Quantity\nFebruary',
 'Quantity\nMarch',
 'Quantity\nApril',
 'Quantity\nMay',
 'Quantity\nJune',
 'Quantity\nJuly',
 'Quantity\nAugust',
 'Quantity\nSeptember',
 'Quantity\nOctober',
 'Quantity\nNovember',
 'Quantity\nDecember',
 'YEAR']





sheet_6_columns = ['Plant Id',
 'Combined Heat And\nPower Plant',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'EIA Sector Number',
 'Sector Name',
 'Reported\nFuel Type Code',
 'AER\nFuel Type Code',
 'Physical\nUnit Label',
 'Quantity\nJanuary',
 'Quantity\nFebruary',
 'Quantity\nMarch',
 'Quantity\nApril',
 'Quantity\nMay',
 'Quantity\nJune',
 'Quantity\nJuly',
 'Quantity\nAugust',
 'Quantity\nSeptember',
 'Quantity\nOctober',
 'Quantity\nNovember',
 'Quantity\nDecember',
 'YEAR']






sheet_7_columns = ['Plant Id',
 'Combined Heat And\n Power Plant',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'Sector Number',
 'Sector Name',
 'Boiler Id',
 'Reported\nPrime Mover',
 'Reported\nFuel Type Code',
 'Physical Unit Label',
 'Quantity Of Fuel Consumed\nJanuary',
 'Quantity Of Fuel Consumed\nFebruary',
 'Quantity Of Fuel Consumed\nMarch',
 'Quantity Of Fuel Consumed\nApril',
 'Quantity Of Fuel Consumed\nMay',
 'Quantity Of Fuel Consumed\nJune',
 'Quantity Of Fuel Consumed\nJuly',
 'Quantity Of Fuel Consumed\nAugust',
 'Quantity Of Fuel Consumed\nSeptember',
 'Quantity Of Fuel Consumed\nOctober',
 'Quantity Of Fuel Consumed\nNovember',
 'Quantity Of Fuel Consumed\nDecember',
 'MMbtu Per Unit\nJanuary',
 'MMbtu Per Unit\nFebruary',
 'MMbtu Per Unit\nMarch',
 'MMbtu Per Unit\nApril',
 'MMbtu Per Unit\nMay',
 'MMbtu Per Unit\nJune',
 'MMbtu Per Unit\nJuly',
 'MMbtu Per Unit\nAugust',
 'MMbtu Per Unit\nSeptember',
 'MMbtu Per Unit\nOctober',
 'MMbtu Per Unit\nNovember',
 'MMbtu Per Unit\nDecember',
 'Sulfur Content\nJanuary',
 'Sulfur Content\nFebruary',
 'Sulfur Content\nMarch',
 'Sulfur Content\nApril',
 'Sulfur Content\nMay',
 'Sulfur Content\nJune',
 'Sulfur Content\nJuly',
 'Sulfur Content\nAugust',
 'Sulfur Content\nSeptember',
 'Sulfur Content\nOctober',
 'Sulfur Content\nNovember',
 'Sulfur Content\nDecember',
 'Ash Content\nJanuary',
 'Ash Content\nFebruary',
 'Ash Content\nMarch',
 'Ash Content\nApril',
 'Ash Content\nMay',
 'Ash Content\nJune',
 'Ash Content\nJuly',
 'Ash Content\nAugust',
 'Ash Content\nSeptember',
 'Ash Content\nOctober',
 'Ash Content\nNovember',
 'Ash Content\nDecember',
 'YEAR']





sheet_8_columns = ['Plant Id',
 'Combined Heat And\n Power Plant',
 'Plant Name',
 'Operator Name',
 'Operator Id',
 'Plant State',
 'Census Region',
 'NERC Region',
 'NAICS Code',
 'Sector Number',
 'Sector Name',
 'Generator Id',
 'Reported\nPrime Mover',
 'Net Generation\nJanuary',
 'Net Generation\nFebruary',
 'Net Generation\nMarch',
 'Net Generation\nApril',
 'Net Generation\nMay',
 'Net Generation\nJune',
 'Net Generation\nJuly',
 'Net Generation\nAugust',
 'Net Generation\nSeptember',
 'Net Generation\nOctober',
 'Net Generation\nNovember',
 'Net Generation\nDecember',
 'Net Generation\nYear To Date',
 'YEAR']






sheet_9_columns = ['YEAR',
 'MONTH',
 'Plant Id',
 'Plant Name',
 'Plant State',
 'Purchase Type',
 'Contract\nExpiration Date',
 'ENERGY_SOURCE',
 'FUEL_GROUP',
 'Coalmine\nType',
 'Coalmine\nState',
 'Coalmine\nCounty',
 'Coalmine\nMsha Id',
 'Coalmine\nName',
 'SUPPLIER',
 'QUANTITY',
 'Average Heat\nContent',
 'Average Sulfur\nContent',
 'Average Ash\nContent',
 'Average Mercury\nContent',
 'FUEL_COST',
 'Regulated',
 'Operator Name',
 'Operator Id',
 'Primary Transportation Mode',
 'Secondary Transportation Mode',
 'Natural Gas Supply Contract Type',
 'Natural Gas Delivery Contract Type',
 'Moisture\nContent',
 'Chlorine\nContent']






# Drop from balancing authority ID to Name
sheet_10_columns = ['YEAR',
 'Plant Id',
 'Plant State',
 'Sector Number',
 'NAICS Code',
 'Plant Name',
 'Combined Heat And\nPower Status']





sheet_11_columns = ['YEAR',
 'Plant Id',
 'Plant State',
 'Sector Number',
 'NAICS Code',
 'Plant Name',
 'Combined Heat And\nPower Status']