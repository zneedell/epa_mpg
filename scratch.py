vin_original.loc[vin_original['model'].str.contains('s10'), 'model_mod']
epa_original.loc[epa_original['model'].str.contains('t10'), 'model_mod']

= 's10 pickup'

l = list(vin.fuelType1.unique())
l.sort()
l


l = list(epa_original.fuelType1.unique())
l.sort()
l








vin_original.loc[vin_original.model.str.contains(pattern1), 'model']

epa_original.loc[epa_original.model.str.contains(pattern1), 
'model'].drop_duplicates().to_csv('models_to_split_epa.csv')


vin.loc[(vin.make == 'chevrolet') & (vin.model.str.contains('g')), ['model', 'model_mod']].drop_duplicates()
vin.loc[vin.VIN_ID == 36534, ['model_mod', 'model']]



vin.model_mod[(vin.make == 'saturn')]
list(vin_original.model[(vin_original.model.str.contains('100'))])

 
t = epa[(epa.make == 'saturn') & (epa.model.str.contains('100'))]

t[['model', 'model_mod']]

dict(vin[(vin.model.str.contains('-'))][['model', 'model_mod', 'make']])
dict(epa[(epa.make == 'mercedes-benz') & (epa.model.str.contains('c'))][['model', 'model_mod', 'make']])


vin_original[vin_original.make == 'freightliner']

epa.loc[(epa.make == 'ford') & (epa.model.str.contains('ltd')), 'model']

epa.loc[epa.model.str.contains('\d\.\d'), ['model', 'model_mod']]

epa.ix[epa.make == 'mercedes-benz', ['model_mod', 'model']].drop_duplicates().to_csv('mercedes_models_epa.csv')

epa_original.loc[epa_original.model.str.contains(separator) , ['model_mod', 'model']].drop_duplicates().to_csv('separator.csv')

vin_mercedes_models_index = vin.ix[(vin.make == 'mercedes-benz')].index
vin.ix[vin_mercedes_models_index, ['model_mod', 'Series']].drop_duplicates().to_csv('mercedes_models_vin.csv')


	# pattern = re.compile(r'[\w, ,-]*?([^\d\W]+[ ,-]*\d+).*')
	vin_mercedes_models_index = vin.ix[(vin.make == 'mercedes-benz')].index
	vin.ix[vin_mercedes_models_index, ['model_mod', 'Series']] = vin.ix[vin_mercedes_models_index, 'Series'].apply(
		lambda x: pattern.match(x).groups()[0].replace(' ', '') if pattern.match(x) else x)
	epa.ix[epa.make == 'mercedes-benz', 'model'] = epa.ix[epa.make == 'mercedes-benz', 'model_mod'].apply(
		lambda x: pattern.match(x).groups()[0].replace(' ', '') if pattern.match(x) else x)

	
	## Apply pattern modifications. 
	## Find models like this: 
	## epa.loc[epa.model.str.contains(r'.*[0-9]\.[0-9](.+)')][['make', 'model', 'model_mod']].drop_duplicates()
	## epa_original.model_mod[epa_original.model == '190 d 2.2/190 e 2.3']
	pattern_list = [
		re.compile(r'(.*) [0-9]\.[0-9]$'), 	# Drop the second part if it's like 'model 3.2'
		re.compile(r'.*[0-9]\.[0-9](.+)')]	# Drop the first part if it's like '3.2cl'
	for pattern in pattern_list:
		epa['model_mod'] = epa['model_mod'].apply(
			lambda x: pattern.match(x).groups()[0] if(pattern.match(x)) else x)
	## Get rid of spaces and dashes. 
	# def try_no_sep(s):
	# 	try:
	# 		return s.replace(' ', '').replace('-', '')
	# 	except:
	# 		return s
	# for df in (epa, vin):
	# 	df['model_mod'] = df['model_mod'].apply(try_no_sep)

		## Keep only first word of the model. 
	for df in (epa, vin):
		df['model_mod'] = df['model_mod'].apply(lambda x: x.strip().split(' ')[0].split('-')[0])
	### Replace '^(\w+?)[0-9]+' with `groups()[0]`. 
	vin.ix[(vin.make == 'saturn') & (vin.model_mod.str.contains('^(\w+?)[0-9]+')), 'model_mod'] = \
		vin.ix[(vin.make == 'saturn') & (vin.model_mod.str.contains('^(\w+?)[0-9]+')), 'model_mod'
			].apply(lambda x: re.match('^(\w+?)[0-9]+', x).groups()[0])

	# For certain makes, only keep the string before the number. 
	# pattern = re.compile(r'(\D+)(\d+)')

	# Drop make international. 
	vin = vin.loc[vin['make'] != 'international']
	# vin.drop(vin[vin['make'] == 'international'].index, inplace=True)
	
	# Equivalent to:
# not_matched_vins = vin.loc[[not(x in vins_matched) for x in vin.VIN_ID]]
# not_matched_epas = epa.loc[[not(x in epas_matched) for x in epa.EPA_ID]]


vin.loc[vin.VIN == 'yv1ax472xe2242024'][['make', 'model', 'year', 'fuelType1']]
matched_vins_simple
matched_vins_simple.loc[matched_vins_simple.VIN == 'yv1ax472xe2242024']
epa.loc[epa.EPA_ID == 18497]

[['make', 'model_mod', 'year', 'fuelType1']]

# Duplicates characterization.
## Create the ranges of values for each VIN. 
matched_vins_ranges = matched_vins_simple.groupby('VIN')
matched_vins_ranges['highway08', 'comb08', 'city08'].describe(percentiles=[]).unstack().reset_index().to_csv('duplicate_ranges.csv')
## Max number of duplicates:
max(map(len, matched_vins_ranges.groups.values()))






















on_cols = [
	'make',
	'model_mod',
	'year',
	'fuelType1_mod',
	'drive_mod',
	'displ_mod',
	'cylinders',
	'transmission_speeds_mod',
	'transmission_type_mod',
	]

matched_vins_sub = matched_vins[on_cols + ['VIN_ID', 'EPA_ID', 'VIN', 'counts',]]
other_vin_cols = list(set(vin.columns) - set(on_cols + ['VIN', 'counts', 'VIN_ID']))
other_epa_cols = list(set(epa.columns) - set(on_cols + ['EPA_ID']))







# Check if all VIN numbers are in matched or not_matched.
nm = set(not_matched_vins.VIN)
m = set(matched_vins_simple.VIN)
a = set(vin.VIN)
a == m | nm # True: yes all the VINs are either in not_matched or matched

# Find duplicate VIN numbers.
def find_duplicates(vin):
	all_vins = list(vin.VIN)
	vins_counts = {}
	for x in all_vins:
		vins_counts[x] = vins_counts.setdefault(x, 0) + 1
	# Other way:
	# from collections import Counter
	# vins_counts2 = Counter(all_vins)
	duplicate_vins = map(lambda x: x[0], filter(lambda (k, v): v > 1, vins_counts.items()))
	# Other method: duplicate_vins_2 = [k for k, v in vins_counts.items() if v > 1]
	print('{0:20.2%}'.format(float(len(duplicate_vins))/len(all_vins)))
	return duplicate_vins

duplicate_vins = find_duplicates(not_matched_vins)
duplicate_original_vins = find_duplicates(vin_original)
duplicate_erg = find_duplicates(erg)

shared_items = set(vins_counts.items()) & set(vins_counts2.items())

list(vin.VIN) == list(len(vin_original.VIN))

with open('duplicates_vins.csv', 'wb') as csv_file:
	csv_writer = csv.writer(csv_file)
	for d in duplicate_vins:
		csv_writer.writerow([d, vins_counts[d]])

vin.to_csv('vin_data_processed.csv', encoding='utf8')






list(vin[vin.VIN_ID == 44010][on_cols[:6]]) == list(epa[epa.EPA_ID == 2674][on_cols[:6]])


matched_all_vins = drop_pattern(pd.merge(vin, matched_vins_no_dupes, how='left', on='VIN_ID'), re.compile(r'.*_y'))
matched_all_vins.to_csv('matched_vins.csv')

outer_join = pd.merge(matched_all_vins, epa, how='outer', on='EPA_ID')
outer_join = outer_join[sorted(outer_join.columns)]
outer_join.to_csv('outer_join.csv')

found = left_join_1['counts'][[not(x) for x in left_join_1['EPA_ID'].isnull()]].sum()
print('Merge fraction weighted: {:.2%}'.format(found/vin['counts'].sum()))




other_cols = list(set(inner_join_1.columns) - set(on_cols))
ord_dict = dict(zip(on_cols, range(len(on_cols))) + zip(other_cols, [len(on_cols)]*len(other_cols)))
inner_join_1 = inner_join_1[sorted(inner_join_1.columns, key= lambda x: ord_dict[x])]


matched_vins.loc[matched_vins['VIN_ID'] == 81191].to_csv('duplicate_example.csv')

# Export.
## Define columns to output.
mod_cols_sub = [x for x in mod_cols  if x not in ['transmission_speeds', 'transmission_type',]]
out_cols = on_cols + append_to_list(mod_cols_sub, '_x') \
	+ append_to_list(mod_cols_sub, '_y') + ['VIN_ID', 'EPA_ID', 'counts']
## Push to database. 
conn = sqlite3.connect('X:\EPA_MPG\VIN_EPA_DB2.db')
inner_join[out_cols].to_sql(
	name='model_no_mod', con=conn, flavor='sqlite', if_exists='replace', index=False)
## To .csv file.
inner_join.to_csv(
	'outer_join_{}_{}.csv'.format(
		datetime.now().date().isoformat(), datetime.now().strftime("%H-%M-%S")), 
	encoding='utf8')
outer_join[out_cols].to_csv(
	'outer_join_{}_{}.csv'.format(
		datetime.now().date().isoformat(), datetime.now().strftime("%H-%M-%S")), 
	encoding='utf8')




# Find what's not matching. 
vin_sub = pd.DataFrame(vin[on_cols].groupby(
						on_cols).groups.keys(), columns=on_cols)
epa_sub = pd.DataFrame(epa[on_cols].groupby(
						on_cols).groups.keys(), columns=on_cols)
## Add IDs
vin_sub['VIN_ID'] = range(1, len(vin_sub) + 1)
epa_sub['EPA_ID'] = range(1, len(epa_sub) + 1)
pd.merge(vin_sub, epa_sub, how='outer', on=on_cols).to_csv(
	'model_merge_sub_{}_{}.csv'.format(
		datetime.now().date().isoformat(), datetime.now().strftime("%H-%M-%S")), 
	encoding='utf8')


# Drop all columns which names match `pattern`.
def drop_pattern(df, pattern):
	return df.drop([x for x in df.columns if pattern.match(x)], axis=1)

# Rename columns with the first group that matches `pattern`. 
def rename_pattern(df, pattern):
	return df.rename(columns=
		dict(zip(df.columns, [pattern.match(x).groups()[0] for x in df.columns])))

pattern = re.compile(r'.*_[x, y]')


matched_vins_simple = drop_pattern(matched_vins, pattern)
matched_vins_no_dupes = matched_vins_simple.drop_duplicates(subset='VIN')
vins_no_dupes = vin.drop_duplicates(subset='VIN')
print('Merge fraction weighted: {:.2%}'.format(float(matched_vins_no_dupes['counts'].sum())/vins_no_dupes['counts'].sum()))

matched_vins_simple.to_csv('matched_vins.csv', encoding = 'utf8')
# Duplicates characterization.
## Create the ranges of values for each VIN. 
matched_vins_ranges = matched_vins_simple.groupby('VIN')
matched_vins_ranges['highway08', 'comb08', 'city08'].describe(percentiles=[]).unstack().reset_index().to_csv('duplicate_ranges.csv')
## Max number of duplicates:
max(map(len, matched_vins_ranges.groups.values()))

vins_matched = matched_vins_no_dupes.VIN_ID
epas_matched = matched_vins_no_dupes.EPA_ID
not_matched_vins = vin.loc[~vin.VIN_ID.isin(vins_matched)]
not_matched_epas = epa.loc[~epa.EPA_ID.isin(epas_matched)]
# Equivalent to:
# not_matched_vins = vin.loc[[not(x in vins_matched) for x in vin.VIN_ID]]
# not_matched_epas = epa.loc[[not(x in epas_matched) for x in epa.EPA_ID]]

not_matched = pd.concat([not_matched_epas, not_matched_vins])
not_matched_out = not_matched[[
	'make',
	'model_mod',
	'model',
	'year',
	'fuelType1_mod',
	'drive_mod',
	'displ_mod',
	'cylinders',
	'transmission_speeds_mod',
	'transmission_type_mod',
	'EPA_ID',
	'VIN_ID',
	'VIN',
	'counts',
	'BodyClass',
	'VehicleType',
	'Series'
		]]

def clear_use_range(wb, sheet_name):
	wb.sheets(sheet_name).activate()
	active_sheet = xw.sheets.active
	used_range_rows = (active_sheet.api.UsedRange.Row, 
		active_sheet.api.UsedRange.Row + active_sheet.api.UsedRange.Rows.Count)
	used_range_cols = (active_sheet.api.UsedRange.Column, 
		active_sheet.api.UsedRange.Column + active_sheet.api.UsedRange.Columns.Count)
	used_range = xw.Range(*zip(used_range_rows, used_range_cols))
	used_range.clear()