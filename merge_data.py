#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 30 13:54:42 2019

@author: zack
"""

import pandas as pd
import re
import numpy as np
from datetime import datetime
from itertools import combinations
from collections import OrderedDict
import operator

VIN_path = 'vehicles.csv'
EPA_path = 'vehicles_epa.csv'
count_path = 'counts_for_ZN.txt'

def load(VIN_path,EPA_path):
    vin = pd.read_csv(VIN_path, dtype=str, encoding='utf8')
    #df.columns.values[:] = [col.split('_')[-1] for col in df.columns]
    epa = pd.read_csv(EPA_path, dtype=str, encoding='utf8')
    counts = pd.read_csv(count_path,names=['first','second','counts'], encoding='utf8')
    return vin, epa, counts

def mod_makes(vin, epa):
	# Modify makes. 
	## Ram.
	vin.loc[(vin.make == 'ram'), 'make'] = 'dodge'
	epa.loc[(epa.make == 'ram'), 'make'] = 'dodge'
	## Ford. 
	epa.loc[(epa.make == 'ford') & (epa.model == 'escort zx2'), 'model_mod'] = 'zx2'
	## Chevrolet.
	vin.loc[(vin.make == 'geo'), 'make'] = 'chevrolet'
	epa.loc[(epa.make == 'geo'), 'make'] = 'chevrolet'
	epa.loc[epa.make == 'gmc', 'make'] = 'chevrolet'
	vin.loc[vin.make == 'gmc', 'make'] = 'chevrolet'
	## Toyota. 
	vin.loc[vin.model.str.contains('scion'), 'make'] = 'scion'
	## Chrysler.
	### Replace Chrysler with Dodge for model Caravan in VIN.
	vin.loc[(vin.make == 'chrysler') & (vin.model_mod == 'caravan'), 'make'] = 'dodge'
	## Sprinter
	vin.loc[(vin.make == 'sprinter (dodge or freightliner)'), 'make'] = 'dodge'

	return vin, epa

def mod_models(vin, epa):
	# Modify models. 
    # Explorer Sport Trac
    # Taurus
	vin.loc[(vin.model == 'explorer sport trac'), 'model_mod'] = 'explorersport'
	vin.loc[(vin.model == 'taurus x'), 'model_mod'] = 'taurusx'
	# Cadillac. 
	vin.loc[(vin.make == 'cadillac') & (vin.model.str.contains('xts')) & vin.Series.str.contains('livery'), 'model_mod'] = 'xtslimo'
	epa.loc[(epa.make == 'cadillac') & (epa.model.str.contains('(?=.*xts.*)(?=.*(limo|hearse).*)')), 
		'model_mod'] = 'xtslimo'
	# For Lexus models, drop the numbers. 
	epa.loc[(epa.make == 'lexus') & (epa.model_mod.str.contains(r'(?:gs|sc)\d+')), 'model_mod'] = \
		epa.loc[(epa.make == 'lexus') & (epa.model_mod.str.contains(r'(?:gs|sc)\d+')), 
			'model_mod'].str.extract(r'(\D+)\d+').values
	# Get rid of 'new' from all volkswagen models. 
	vin.loc[(vin.make == 'volkswagen') & (vin.model_mod.str.contains('new')), 'model_mod'] = \
		vin.loc[(vin.make == 'volkswagen') & (vin.model_mod.str.contains('new')), 'model_mod'].str.extract('new(.*)').values
	epa.loc[(epa.make == 'volkswagen') & (epa.model_mod.str.contains('new')), 'model_mod'] = \
		epa.loc[(epa.make == 'volkswagen') & (epa.model_mod.str.contains('new')), 'model_mod'].str.extract('new(.*)').values
	# In EPA, change the model name to crosstour when the model name contains that string. 
	epa.loc[(epa.make == 'honda') & (epa.model.str.contains('crosstour')), 'model_mod'] = 'crosstour'
	# EPA has a separate model called accord wagon; in VIN, BodyClass identifies the wagons; 
	# so we create a separate model in VIN called accord-wagon.
	vin.loc[(vin.make == 'honda') & (vin.model == 'accord') & (vin.BodyClass == 'wagon'), 'model_mod'] = 'accord-wagon'
	epa.loc[(epa.make == 'honda') & (epa.model == 'accord wagon'), 'model_mod'] = 'accord-wagon'
	# Honda HX. 
	epa.loc[(epa.model.str.contains('(?=(?:.*civic.*))(?=(?:.*hx.*))')) & (epa.make == 'honda'), 
		'model_mod'] = 'civichx'
	vin.loc[(vin.Series.str.contains('hx')) & (vin.model.str.contains('civic')) & (vin.make == 'honda'), 
		'model_mod'] = 'civichx'
	# EPA has a separate model called matrix, whereas VIN has a model corolla matrix; 
	# so we create a separate model in VIN called matrix.
	vin.loc[(vin.make == 'toyota') & (vin.model == 'corolla matrix'), 'model_mod'] = 'matrix'
	## Modify acura models. 
	epa.loc[(epa.make == 'acura') & epa.model.str.contains(r'\d\.\d.*'), 'model_mod'] = \
		epa.loc[(epa.make == 'acura') & epa.model.str.contains(r'\d\.\d.*'), 'model_mod'].apply(
			lambda s: re.sub(r'\d\.\d(.*)', r'\1', s))
	## Infiniti. 
	epa.loc[(epa.make == 'infiniti') & epa.model.str.contains(r'(?=.*?)x(?=$|\s)'), 'model_mod'] = \
		epa.loc[(epa.make == 'infiniti') & epa.model.str.contains(r'(?=.*?)x(?=$|\s)'), 'model_mod'].apply(
			lambda s: re.sub(r'(.*?)x(?=$|\s).*', r'\1', s))
	## Nissan. 
	epa.loc[(epa.make == 'nissan'), 'model_mod'] = \
        epa.loc[(epa.make == 'nissan'), 'model_mod'].replace('truck', 'pickup', regex=True)
	epa.loc[epa.model.str.contains('pathfinder armada'), 'model_mod']	= 'armada'
	## BMW. 
	epa.loc[(epa.make == 'bmw'), 'model_mod'] = epa.loc[(epa.make == 'bmw'), 'model_mod'].apply(lambda s: s[0])
	vin.loc[(vin.make == 'bmw'), 'model_mod'] = vin.loc[(vin.make == 'bmw'), 'model_mod'].apply(lambda s: s[0])
	## Delete from EPA all models that contain chassis in model name. 
	epa = epa.loc[~epa.model.str.contains('chassis')]
	## All models with displacements in the model name, e.g. '190e 2.3-16'
	def mod_models_w_displ(s):
		pattern = re.compile(r'(.*)\d\.\d(.*)')
		groups = re.search(pattern, s).groups()
		return groups[0].strip() or groups[1].strip()
	## Lincoln.
	### Replace 'zephyr' with 'mkz' for VIN for make 'lincoln'
	vin.loc[(vin.make == 'lincoln') & (vin.model.str.contains('zephyr')), 'model_mod'] = 'mkz'
	## Ford.
	### Replace 'ltd crown victoria' with 'crown victoria' in EPA for make 'ford'
	epa.loc[(epa.make == 'ford') & (epa.model.str.contains('ltd crown victoria')), 'model_mod'] = 'crown victoria'
	### Replace 'crown victoria' with 'crown victoria police' in VIN for make 'ford' if `series = 'police interceptor'`
	vin.loc[(vin.make == 'ford') & (vin.model.str.contains('crown victoria')) & (vin.Series == 'police interceptor'),
		'model_mod'] = 'crown victoria police'
	## Chrysler. 
	### Replace '300c' and '300c/srt' with '300' in EPA
	epa.loc[(epa.make == 'chrysler') & (epa.model.str.contains('300')), 'model_mod'] = '300'
	## Saturn.
	### Replace L100/200 with LS1 in EPA when year is larger than 2001 (inclusive)
	epa.loc[(epa.make == 'saturn') & (epa.model == 'l100/200') & (epa.year >= 2001), 'model_mod'] = 'ls1'
	vin.loc[(vin.make == 'saturn') & (vin.model_mod != 'ls1'), 'model_mod'] = \
		vin.loc[(vin.make == 'saturn') & (vin.model_mod != 'ls1'), 'model_mod'].apply(
			lambda s: re.search(r'([^\d]+)[\d]', s).groups()[0] if re.search(r'([^\d]+)[\d]', s) else s)
	## Mercedes. 
	class_index = vin.loc[(vin.make == 'mercedes-benz') & (vin.model.str.contains(r'(?:\D+)-class'))].index
	vin.loc[class_index, 'model_mod'] = vin.loc[class_index, 'model'].str.extract(r'(\D+)-class').values
	digit_class_index = vin.loc[(vin.make == 'mercedes-benz') & (vin.model.str.contains(r'(?:\d+)'))].index
	vin.loc[digit_class_index, 'model_mod'] = \
		vin.loc[digit_class_index, 'Series'].str.split(' ').apply(lambda xs: xs[0]).str.extract(r'.*?(\D+)').values
	vin.loc[(vin.make == 'mercedes-benz') & (vin.model_mod == 'm'), 'model_mod'] = 'ml'
	### AMG models.
	vin.loc[(vin.make == 'mercedes-benz'), 'model_mod'], epa.loc[(epa.make == 'mercedes-benz'), 'model_mod'] = \
		[df.loc[(df.make == 'mercedes-benz'), 'model_mod'].str.replace('amg', '').str.strip() for df in (vin, epa)]
	### Get the models that start with numbers, e.g. 190 sl and 200sel, and keep only the numbers.
	index = epa.loc[(epa.make == 'mercedes-benz') & (epa.model.str.contains(r'^\d+')), 'model_mod'].index
	epa.loc[index, 'model_mod'] = epa.loc[index, 'model_mod'].str.extract(r'^(\d+)').values
	### Get the models that start with letters, e.g. sl190 and sel 190, and keep only the letters.
	index = epa.loc[(epa.make == 'mercedes-benz') & (epa.model.str.contains(r'^\D+')), 'model_mod'].index
	epa.loc[index, 'model_mod'] = epa.loc[index, 'model_mod'].str.extract(r'^(\D+)').values
	## Toyota. 
	### Prius Eco
	vin.loc[(vin.VIN.apply(lambda s: s[3:8]) == 'karfu') & (vin.model_mod.str.contains('prius')), 'model_mod'] = 'priuseco'
	epa.loc[epa.model.str.contains('prius plug-in'), 'model_mod'] = 'prius plug-in'
	pattern = r'c|v|prime|plug-in|eco'
	vin.loc[vin.model.str.contains(r'prius (?:{})'.format(pattern)), 'model_mod'] = \
		vin.loc[vin.model.str.contains(r'prius (?:{})'.format(pattern)), 'model_mod'].apply(lambda s: s.replace(' ', ''))
	epa.loc[epa.model.str.contains(r'prius (?:{})'.format(pattern)), 'model_mod'] = \
		epa.loc[epa.model.str.contains(r'prius (?:{})'.format(pattern)), 'model_mod'].apply(lambda s: s.replace(' ', ''))
	vin.loc[vin.make == 'scion', 'model_mod'] = \
		vin.loc[vin.make == 'scion', 'model_mod'].apply(lambda x: x.split(' ')[1] if len(x.split(' '))>1 else x)
	epa.loc[epa.model.str.contains('solara'), 'model_mod'] = 'solara'
	vin.loc[vin.model.str.contains('4-runner'), 'model_mod'] = '4runner'
	epa.loc[(epa.make == 'toyota'), 'model_mod'] = \
		epa.loc[(epa.make == 'toyota'), 'model_mod'].replace('truck', 'pickup', regex=True)
	vin.loc[(vin.make == 'toyota') & (vin.model == 'camry') & (vin.year == 2002) & (vin.displ_mod == '2.2'),
		'displ_mod'] = '2.4'
	## Mazda.
	def delete_mazda(s):
		match = re.search('mazda(.*)', s)
		if match:
			return match.groups()[0]
		else:
			return s
	vin.loc[vin['make'] == 'mazda', 'model_mod'] = \
		vin.loc[vin['make'] == 'mazda', 'model_mod'].apply(delete_mazda)
	epa.loc[(epa.make == 'mazda') & (epa.model_mod.str.contains(r'^b\d')), 'model_mod'] = \
		epa.loc[(epa.make == 'mazda') & (epa.model_mod.str.contains(r'^b\d')), 'model_mod'].apply(lambda x: x[0])
	### Add displacement and cylinder where it's missing.
	vin.loc[(vin['make'] == 'mazda') & vin.model.str.contains('626') & 
		vin.VIN.apply(lambda x: x[7]).isin('c,e'.split(',')), 'displ_mod'] = 2.0
	vin.loc[(vin['make'] == 'mazda') & vin.model.str.contains('626') & 
		vin.VIN.apply(lambda x: x[7]).isin('d,f'.split(',')), 'displ_mod'] = 2.5
	vin.loc[(vin['make'] == 'mazda') & vin.model.str.contains('protege') & 
		vin.VIN.apply(lambda x: x[6:8]).isin('41'.split(',')), 'displ_mod'] = 1.5
	vin.loc[(vin['make'] == 'mazda') & vin.model.str.contains('protege') & 
		vin.VIN.apply(lambda x: x[6:8]).isin('42,21,23'.split(',')), 'displ_mod'] = 1.8
	vin.loc[(vin['make'] == 'mazda') & vin.model.str.contains('protege') & 
		vin.VIN.apply(lambda x: x[6:8]).isin('22,24'.split(',')), 'displ_mod'] = 1.6
	## Mini. 
	### John Cooper Works. 
	jcw_index = epa.loc[epa.model_mod.str.contains('john cooper works')].index
	epa.loc[jcw_index, 'model_mod'] = \
		epa.loc[jcw_index, 'model_mod'].str.extract('john cooper works(.*)').apply(lambda s: 'jcw'+s).values
	### Others. 
	def take_out(s):
		replace_list = r'gp-2, \\bgp\\b, coupe, kit, \(.*\), all4, \\b2\\b, \\b4\\b, door, hardtop'.split(', ')
		default_str = ''
		replace_dict = OrderedDict(list(zip(replace_list, [default_str]*len(replace_list))))
		for k, v in list(replace_dict.items()): 
			s = re.sub(k, v, s).strip()
		return s
	mapping = {
		'clubman s': 'cooper s clubman',
		'clubman': 'cooper clubman',
		'cooper clubvan': 'cooper clubman',
	}
	vin.loc[vin.make == 'mini', 'model_mod'], epa.loc[epa.make == 'mini', 'model_mod'] = \
		[df.loc[df.make == 'mini', 'model_mod'].apply(take_out).replace(mapping).apply(
			lambda s: s.replace(' ', '')) for df in (vin, epa)]
	## Chevrolet.
	### s10 models. 
	epa.loc[(epa.make == 'chevrolet') & (epa.model.str.contains(r'(^|\s)blazer($|\s)')), 'model_mod'] = 'blazer'
	vin.loc[(vin.make == 'chevrolet') & (vin.model.str.contains(r'(^|\s)blazer($|\s)')), 'model_mod'] = 'blazer'
	epa.loc[(epa.make == 'chevrolet') & (epa.model.str.contains('suburban')), 'model_mod'] = 'suburban'
	vin.loc[(vin.make == 'chevrolet') & (vin.model.str.contains('suburban')), 'model_mod'] = 'suburban'
	epa.loc[(epa.make == 'chevrolet') & (epa.model.str.contains('s10|s-10')), 'model_mod'] = 's'
	vin.loc[(vin.make == 'chevrolet') & (vin.model.str.contains('s10|s-10')), 'model_mod'] = 's'
	### Geo Metro model. 
	vin.loc[(vin.make == 'chevrolet') & (vin.model.str.contains('geo prizm')), 'model_mod'] = 'prizm'
	vin.loc[(vin.make == 'chevrolet') & (vin.model.str.contains('geo metro')), 'model_mod'] = 'metro'
	### Replace gmt-400 with c when drive_mod = 2 and with k otherwise. 
	vin.loc[(vin.make == 'chevrolet') & (vin.model == 'gmt-400') & (vin.drive_mod == 'two'), 'model_mod'] = 'c'
	vin.loc[(vin.make == 'chevrolet') & (vin.model == 'gmt-400') & (vin.drive_mod == 'all'), 'model_mod'] = 'k'
	### Replace '^\D\d' with the first letter. 
	pattern = r'^(\D+)\s*\d+'
	ind = epa.loc[(epa.make.isin(['chevrolet', 'dodge'])) & (epa.model_mod.str.contains(pattern)), 'model_mod'].index
	epa.loc[ind, 'model_mod'] = epa.loc[ind, 'model_mod'].str.extract(pattern).values
	ind = vin.loc[(vin.make.isin(['chevrolet', 'dodge'])) & (vin.model_mod.str.contains(pattern)), 'model_mod'].index
	vin.loc[ind, 'model_mod'] = vin.loc[ind, 'model_mod'].str.extract(pattern).values

	# Keep only first word of the model. 
	epa['model_mod'], vin['model_mod'] = [
		df['model_mod'].apply(lambda x: x.strip().split(' ')[0].split('-')[0]) for df in (epa, vin)]

	return vin, epa



def fix(vin, epa, counts, init_yr=1991, last_yr=np.inf):
	
	vin_original, epa_original = vin.copy(), epa.copy()

	# Fix errors in the datasets.
	vin_original.loc[(vin_original.ModelYear == 1998) & (vin_original.Make == 'FORD') & 
		(vin_original.Model == 'Expedition') & (vin_original.DisplacementL == 14.6), 
		['DisplacementL', 'FuelTypePrimary']] = [4.6, 'gasoline']
	vin_original.loc[(vin_original.ModelYear == '1998') & (vin_original.Make == 'FORD') & 
		(vin_original.Model == 'Explorer')  & (
			(vin_original.VIN.apply(lambda x: x[7]) == 'E') | (vin_original.VIN.apply(lambda x: x[7]) == 'X')), 
		['DisplacementL', 'FuelTypePrimary']] = ['4.0', 'gasoline']
	vin_original.loc[(vin_original.ModelYear == '1998') & (vin_original.Make == 'FORD') & 
		(vin_original.Model == 'Explorer') & (vin_original.VIN.apply(lambda x: x[7]) == 'P'), 
		['EngineCylinders', 'DisplacementL', 'FuelTypePrimary']] = ['8', '5.0', 'gasoline']
	vin_original.loc[vin_original.Model.str.lower() == 'solistice', 'Model'] = 'solstice'
	vin_original.loc[vin_original.Model.str.lower() == 's/v 70 series', 'Model'] = 's70/v70'

	# Define integer id based on error code from VIN database. 
	vin_original['error_id'] = vin_original.ErrorCode.apply(
		lambda x: re.search('([0-9]+).*', x).groups()[0])

	# Define columns on which the merge will be performed.
	epa_cols = [
		'make',
		'model',
		'year',
		'fuelType1',
		'fuelType2',
		'drive',
		'transmission_type',
		'transmission_speeds',
		'cylinders',
		'displ',
		]
	vin_cols = [
		'Make',
		'Model',
		'ModelYear',
		'FuelTypePrimary',
		'FuelTypeSecondary',
		'DriveType',
		'TransmissionStyle', 
		'TransmissionSpeeds',
		'EngineCylinders',
		'DisplacementL',
		]

	# Get rid of undesirable columns.
	vin_keep_cols = [
		'VIN', 'VehicleType', 'BodyClass', 'error_id', 'Series', 'vtyp3', 'counts', 'Trim', 'Trim2', 'GVWR', 'BodyCabType', 'Doors'
		] + vin_cols 
	vin_original.drop([x for x in vin_original.columns if x not in vin_keep_cols], axis=1, inplace=True)
	epa_keep_cols = [
		'trany', 'city08', 'city08U', 'comb08', 'comb08U', 'highway08', 'highway08U', 'VClass', 'atvType', 'eng_dscr'
		] + epa_cols
	epa_original.drop([x for x in epa_original.columns if x not in epa_keep_cols], axis=1, inplace=True)

	# Rename the VIN dataframe columns to be the same as the EPA dataframe columns.
	vin_original = vin_original.rename(columns=dict(list(zip(vin_cols, epa_cols))))

	# Get rid of rows where certain info is missing.
	essential_cols = 'make, model, year'.split(', ')
	for col in essential_cols:
		vin_original = vin_original.loc[~vin_original[col].isnull()]

	# Replace missing fuelType1 (nan) with u'gasoline'.
	vin_original.fuelType1, epa_original.fuelType1 = [
		df.fuelType1.fillna('gasoline') for df in (vin_original, epa_original)]

	# Replace missing values (nan) with u'-1'.
	vin_original, epa_original = [
		df.apply(lambda x: pd.Series.fillna(x, '-1')) for df in (vin_original, epa_original)]

	# Make everything lower case and trim white spaces.
	vin_original, epa_original = [
		df.applymap(lambda s: s.lower().strip()) for df in (vin_original, epa_original)]

	# Get rid of undesirable vehicles.
	filter_out_strs = 'incomplete, trailer, motorcycle, bus, low speed vehicle (lsv)'.split(', ')
	vin_original = vin_original.loc[~vin_original['VehicleType'].isin(filter_out_strs)]
	vin_original = vin_original.loc[~vin_original['BodyClass'].str.contains('incomplete')]

	# Get rid of duplicates in fields (e.g. 'gasoline, gasoline', or 'audi, audi').
	def del_duplicate_in_str(s):
		def _del_duplicate_in_str(s):
			found = pattern.search(s)
			if found:
				s0, s1 = [x.strip() for x in found.groups()]
				if pattern.search(s0):
					return _del_duplicate_in_str(s0)
				elif s0 == s1:
					return s0
			return s

		if not isinstance(s, str):
			return s
		pattern = re.compile('(.*), (.*)')
		return _del_duplicate_in_str(s)	
	vin_original = vin_original.applymap(del_duplicate_in_str)

	# Drop certain makes. 
	drop_makes = \
		'''volvo truck
		western star
		whitegmc
		winnebago
		winnebago industries, inc.
		workhorse
		ai-springfield
		autocar industries
		capacity of texas
		caterpillar
		e-one
		freightliner
		kenworth
		mack
		navistar
		peterbilt
		pierce manufacturing
		spartan motors chassis
		terex advance mixer
		the vehicle production group
		utilimaster motor corporation
		international'''.split('\n')
	drop_makes = list(map(str.strip, drop_makes))
	vin_original = vin_original.loc[~vin_original.make.isin(drop_makes)]

	## Modify fuel type, drive type for epa and vin, and transmission type for vin. 
	mapping = {
		'vin': {
			'fuelType1':	{
				'compressed natural gas (cng)':									'natural gas',
				'liquefied petroleum gas (propane or lpg)':						'natural gas',
				'liquefied natural gas (lng)':									'natural gas',
				'gasoline, diesel':												'gasoline',
				'diesel, gasoline':												'gasoline',
				'ethanol (e85)':												'ethanol',
				'compressed natural gas (cng), gasoline':						'gasoline',
				'gasoline, compressed natural gas (cng)':						'gasoline',
				'compressed hydrogen / hydrogen':								'hydrogen',
				'fuel cell':													'hydrogen',
				},
			'drive':	{
				'4x2':																'two',
				'6x6':																'all',
				'6x2':																'two',
				'8x2':																'two',
				'rwd/ rear wheel drive':											'two',
				'fwd/front wheel drive':											'two',
				'4x2, rwd/ rear wheel drive':										'two',
				'4x2, fwd/front wheel drive':										'two',
				'rwd/ rear wheel drive, 4x2':										'two',
				'fwd/front wheel drive, 4x2':										'two',
				'4wd/4-wheel drive/4x4':											'all',
				'awd/all wheel drive':												'all',
				},
			'transmission_type':	{
				'manual/standard': 												'manu',
				'automated manual transmission (amt)': 							'manu',
				'manual/standard, manual/standard': 							'manu',
				'dual-clutch transmission (dct)': 								'manu',
				'continuously variable transmission (cvt)': 					'auto',
				'automatic': 													'auto',
				'automatic, continuously variable transmission (cvt)': 			'auto',
				}
			},
		'epa': {
			'fuelType1':	{
				'regular gasoline':			'gasoline',
				'premium gasoline':			'gasoline',
				'midgrade gasoline':		'gasoline',
				},
			'drive':	{
				'rear-wheel drive':				'two',
				'front-wheel drive':			'two',
				'2-wheel drive':				'two',
				'all-wheel drive':				'all',
				'4-wheel drive':				'all',
				'4-wheel or all-wheel drive':	'all',
				'part-time 4-wheel drive':		'all',
				},
			}
		}
	for (df, df_name) in ((epa_original, 'epa'), (vin_original, 'vin')):
		for item in mapping[df_name]:
			df[item + '_mod'] = df[item]
			df[item + '_mod'] = df[item].replace(mapping[df_name][item])

	# Flag flexible fuel vehicles. 
	flex_str = 'ffv|flexible|ethanol|e85|natural gas'
	vin_index, epa_index = [df.loc[(df.fuelType1.str.contains(flex_str)) | (df.model.str.contains(flex_str)) |
		(df.fuelType2.str.contains(flex_str))].index
		for df in (vin_original, epa_original)]
	vin_original.loc[vin_index, 'fuelType1_mod'], epa_original.loc[epa_index, 'fuelType1_mod'] = 'ffv', 'ffv'
	epa_index = epa_original.loc[epa_original.atvType.str.contains('bi|ffv') | 
		epa_original.eng_dscr.str.contains('ffv')].index
	epa_original.loc[epa_index, 'fuelType1_mod'] = 'ffv'

	# Ford escape. 
	vin_original.loc[(vin_original.make == 'ford') & (vin_original.model == 'escape') & 
		(vin_original.year >= 2010) & (vin_original.year <= 2012) & 
		(vin_original.displ == '3.0') & (vin_original.cylinders == '6'), 'fuelType1'] = 'ffv'
                  
	# Modify fuel types to account for electric vehicles. 
	def mod_electric_vehicles(df):
		indexes_processed = []
		# phev.
		phev_index = df.loc[(df.model.str.contains('plug|volt')) |  
			((df.fuelType1.str.contains('electric')) & (df.fuelType2.str.contains('gasoline')))].index.tolist()
		indexes_processed += phev_index
		# Optional, because first one processed: `indexes_processed` is empty:
		# phev_index = list(set(phev_index) - set(indexes_processed))
		df.loc[phev_index, 'fuelType1_mod'] = 'phev'
		# hev.
		hev_index = df.loc[((df.fuelType1.str.contains('gasoline')) & (df.fuelType2.str.contains('electric'))) |
			(df.model.str.contains('(hev|hybrid)'))].index.tolist()
		hev_index = list(set(hev_index) - set(indexes_processed))
		df.loc[hev_index, 'fuelType1_mod'] = 'hev'
		# bev.
		bev_index = df.loc[(df.model.str.contains('bev')) | (df.model.str.contains('electric')) | 
			(df.fuelType1.str.contains('electric'))].index.tolist()
		# Optional, because last one processed.
		# bev_index = list(set(bev_index) - set(indexes_processed))
		df.loc[bev_index, 'fuelType1_mod'] = 'bev'
		return df
	vin_original = mod_electric_vehicles(vin_original)
	# Make Chevvy Volts PHEVs.
	vin_original.loc[(vin_original.make == 'chevrolet') & (vin_original.model.str.contains('volt')), 'fuelType1_mod'] = 'phev'
	ev_dict = {'hybrid': 'hev', 'plug-in hybrid': 'phev', 'ev': 'bev'}
	for k, v in list(ev_dict.items()):
		epa_original.loc[epa_original.atvType == k, 'fuelType1_mod'] = v 

	# Make years ints.
	epa_original.year = epa_original.year.apply(float).apply(int)
	vin_original.year = vin_original.year.apply(float).apply(int)

	# Fix some more errors in the datasets. 
	## MY06 Chevy Tahoe and Suburban HEVs have the incorrect engine info in VPIC; they should be 5.3 liter 8 cylinder 
	## FFV, not 2.4 liter 4 cylinder.
	vin_original.loc[(vin_original.year == 2006) & (vin_original.make.str.lower() == 'chevrolet') & 
		(vin_original.model.str.lower().str.contains('tahoe|suburban') & (vin_original.fuelType1_mod == 'hev')), 
		['cylinders', 'displ']] = ('8', '5.3')
	## MY11 Chevy Silverado diesel has the incorrect engine in VPIC; it should be 6.6 liter 8 cylinder, not 2.2 liter 4 
	## cylinder.
	vin_original.loc[(vin_original.year == 2011) & (vin_original.make.str.lower() == 'chevrolet') & 
		(vin_original.model.str.lower().str.contains('silverado') & (vin_original.fuelType1_mod == 'diesel')), 
		['cylinders', 'displ']] = ('8', '6.6')

	# Only keep years between `init_yr` and `last_yr`. 
	vin_original = vin_original.loc[(vin_original.year >= init_yr) & (vin_original.year <= last_yr)]
	epa_original = epa_original.loc[(epa_original.year >= init_yr) & (epa_original.year <= last_yr)]

	# Add an ID for EPA before the splitting occurs. Equivalent to VIN. 
	epa_original['EPA'] = list(range(1, len(epa_original) + 1))
	counts = counts.applymap(lambda s:s.lower() if type(s) == unicode else s)   

	vin_original['first'] = vin_original['VIN'].str[:8]
	vin_original['second'] = vin_original['VIN'].str[9:12]
	vin_original = pd.merge(vin_original, counts, on=['first','second'])
	vin_original.loc[np.isnan(vin_original['counts']),'counts'] = 0

	return vin_original, epa_original

def split_and_expand(vin_original, epa_original):

	separator = r'/|,'

	# Create new variables so the original ones don't get overwritten.
	vin_mod, epa_mod = vin_original.copy(), epa_original.copy()

	# Add model_mod.
	vin_mod['model_mod'] = vin_mod['model']
	epa_mod['model_mod'] = epa_mod['model']

	## First, modify the models that will be split. 
	vin_mod, epa_mod = modify_both_before_split(vin_mod, epa_mod)
	vin_mod = modify_vin_before_split(vin_mod)
	epa_mod = modify_epa_before_split(epa_mod, separator)

	# Split rows that contain separators into several rows. 
	## In VIN. 
	## Expand each row that contains a symbol that is not a dash into however many rows are needed:
	## e.g. `monte carlo/this& and this too$any symbol?is isolated - but not a dash or space`
	## becomes one row for each of: `monte carlo`, `this`, ` and this too`, `any symbol`, and
	## `is isolated - but not a dash or space`
	print('Expanding VIN data')
	vin_expanded = pd.concat(
			[pd.Series(np.append(row[[col for col in vin_mod.columns if col != 'model_mod']].values, [x]))
				for _, row in vin_mod.iterrows() 
				for x in [s.strip() for s in re.findall(r'[\w -]+', row['model_mod'])]],
			axis=1).transpose()
	vin_expanded.columns = vin_mod.columns
	## Delete all spaces from a subset of the makes. 
	vin_expanded.loc[vin_expanded.make == 'buick', 'model_mod'] = \
		vin_expanded.loc[vin_expanded.make == 'buick', 'model_mod'].replace(' ', '', regex=True)
	vin_expanded.loc[(vin_expanded.make == 'buick') & (vin_expanded.model_mod == 'parkavenue'), 'model_mod'] = 'park'
	# Replace all instances of pick-up with pickup.
	vin_expanded['model_mod'] = vin_expanded.model_mod.str.replace('pick-up', 'pickup')
	# Replace monte carlo with montecarlo
	vin_expanded['model_mod'] = vin_expanded.model_mod.str.replace('monte carlo', 'montecarlo')

	## In EPA. 
	## Expand each row that contains `separator` into however many rows are needed:
	print('Expanding EPA data')
	epa_expanded = pd.concat(
			[pd.Series(np.append(row[[col for col in epa_mod.columns if col != 'model_mod']].values, [x]))
				for _, row in epa_mod.iterrows() 
				for x in split_row(row['model_mod'], separator)],
			axis=1).transpose()
	epa_expanded.columns = epa_mod.columns
	
	print('Making last minute changes')

	# Change grandvoy. to grandvoyager.
	index_mod = epa_expanded.loc[(epa_expanded.make == 'chrysler') & (epa_expanded.model_mod == 'grandvoy.')].index
	epa_expanded.loc[index_mod, 'model_mod'] = 'grandvoyager'

	## Get rid of spaces after 'grand' and 'new'. 
	pattern = re.compile(r'(grand|new)[\W]+([\w]+)')
	### In EPA. 
	index_mod = epa_expanded.loc[epa_expanded.model_mod.str.contains(pattern)].index
	epa_expanded.loc[index_mod, 'model_mod'] = \
		epa_expanded.loc[index_mod, 'model_mod'].str.extract(pattern).apply(''.join, axis=1)
	### In VIN. 
	index_mod = vin_expanded.loc[vin_expanded.model_mod.str.contains(pattern)].index
	vin_expanded.loc[index_mod, 'model_mod'] = \
		vin_expanded.loc[index_mod, 'model_mod'].str.extract(pattern).apply(''.join, axis=1)

	# Add IDs
	vin_expanded['VIN_ID'] = list(range(1, len(vin_expanded) + 1))
	epa_expanded['EPA_ID'] = list(range(1, len(epa_expanded) + 1))

	# Turn anything that looks like this: texttext-123123 (\w+-\d+) into texttext123123 (drop the dash)
	# e.g. f-350
	# Do this using a for-loop with df. 
	vin_expanded.loc[vin_expanded.model_mod.str.contains(r'[^\d]+-\d+.*'), 'model_mod'], \
	epa_expanded.loc[epa_expanded.model_mod.str.contains(r'[^\d]+-\d+.*'), 'model_mod'] = [
		df.loc[df.model_mod.str.contains(r'[^\d]+-\d+.*'), 'model_mod'].str.replace('-', '') 
		for df in (vin_expanded, epa_expanded)]

	# Get rid of transmission type in model for epa_expanded data. 
	pattern = r'(.+)[24a]wd.*'
	epa_expanded['model_mod'] = epa_expanded['model_mod'].apply(
		lambda x: re.search(pattern, x).groups()[0].strip() if re.search(pattern, x) else x)

	# Remove class and series from model names. 
	index_mod = vin_expanded.loc[vin_expanded.model_mod.str.contains(r'\w+\s*-\s*(?:class|series)')].index
	vin_expanded.loc[index_mod, 'model_mod'] = \
		vin_expanded.loc[index_mod, 'model_mod'].str.extract(r'(\w+)\s*-\s*(?:class|series)').values

	# Reset index.
	vin_expanded = vin_expanded.reset_index(drop=True)
	epa_expanded = epa_expanded.reset_index(drop=True)

	return vin_expanded, epa_expanded

def modify(vin_original, epa_original):

	# Create new variables so the original ones don't get mutated.
	vin, epa = vin_original.copy(), epa_original.copy()

	# Make the counts integers. 
	vin['counts'] = vin['counts'].astype(int)
	vin.loc[vin.counts <= 0, 'counts'] = 1

	# Modify transmission information
	## In vin DB: turn transmission speeds into integers then strings.
	vin['transmission_speeds_mod'] = vin['transmission_speeds'].apply(lambda s: str(try_int(s)))
	## In epa DB: transform info in epa database to get trammission speeds and types.
	## Transmission speeds.
	def get_transmission_speeds(s):
		try:
			return re.search(r'\d+', s).group()
		except:
			return None
	## Transmission type.
	def get_transmission_type(s):
		if isinstance(s, basestring):
			if bool(re.search(r'auto', s)):
				return "auto"
			else:
				return "manu"
	## Apply to epa.
	epa['transmission_speeds_mod'] = epa['transmission_speeds'] = epa.trany.apply(get_transmission_speeds)
	epa['transmission_type_mod'] = epa['transmission_type'] = epa.trany.apply(get_transmission_type)

	# Round displacement in both databases.
	def convert_displacement(s):
		if re.findall(',', s):
			return str(round(float(s.split(',')[0]), 1))
		elif s == '-1':
			return s
		else:
			return str(round(float(s), 1))
	for df in (epa, vin):
		df['displ_mod'] = df['displ'].apply(convert_displacement)

	# Change type of mpg values to be floats. 
	mpg_list = 'highway08, highway08U, comb08, comb08U, city08, city08U'.split(', ')
	epa[mpg_list] = epa[mpg_list].astype(float)

	# Add vtyp3 for epa. 
	epa['vtyp3'] = '-1'
	epa.loc[epa.VClass.str.contains('pickup') & epa.model.str.contains('15|10'), 'vtyp3'] = '3'
	epa.loc[epa.VClass.str.contains('pickup') & epa.model.str.contains('25|35'), 'vtyp3'] = '4'
	
	# Add weight variable. 
	weight_df = vin.GVWR.str.extract(r'([\d,]*) lb').fillna('0')
	if pd.__version__ <= '0.22.0':
		weight_series = weight_df
	else:
		weight_series = weight_df[0]
	vin['weight'] = weight_series.str.replace(',', '').astype(int)

	# Drop heavy vehicles. 
	weight_limit = 900000
	vin = vin.loc[vin.weight < weight_limit]

	# Modify makes.
	vin, epa = mod_makes(vin, epa)

	# Modify models. 
	vin, epa = mod_models(vin, epa)

	# Add type and type_from_vin variables. 
	vin, epa = add_type(vin, epa)
	vin = add_type_from_vin(vin)
	vin.type = pd.Series(
		[type_from_vin if type_from_vin else v_type for (v_type, type_from_vin) in zip(vin.type, vin.type_from_vin)]
		)

	# Make some more changes based on Tom's SAS code. 

	# Add vtyp for epa. 
	epa.loc[epa.VClass.str.contains('seaters|cars|wagons'), 'vtyp'] = 'car'
	epa.loc[epa.VClass.str.contains('small pickup'), 'vtyp'] = 'lt1'
	epa.loc[epa.VClass.str.contains('standard pickup'), 'vtyp'] = 'lt2'
	epa.loc[epa.VClass.str.contains('utility'), 'vtyp'] = 'suv'
	epa.loc[epa.VClass.str.contains('minivan'), 'vtyp'] = 'min'
	epa.loc[epa.VClass.str.contains('vans'), 'vtyp'] = 'van'
	epa.loc[epa.VClass.str.contains('purpose'), 'vtyp'] = 'pur'

	model_str = 'crv|macan|pt|hhr|escape|pacifica|equinox|vue|santa|magnum|edge|captiva|trax|ecosport|rav4|highlander|'\
	'rendezvous|freestyle|srx|flex|mkx|mkc|mkt|xt5|mariner|enclave|compass|traverse|torrent|aztek|acadia|terrain|'\
	'patriot|encore|envision|outlook|tiguan|touareg|allroad|q3|q5|q7|x6|x3|x5|rogue|cube|juke|murano|crosstour|x|'\
	'pilot|element|tribute|r350|r500|gl320|gl420|glk350|cayenne|94x|freelander|baja|forester|b9|venza|xc70|xc40|'\
	'xl7|zdx|rdx|mdx|tucson|sportage|veracruz|modelx|journey|sq5|'\
	'xc60|xc90|outlander|endeavor|outback|mdx|cx3|cx4|cx5|cx7|cx9'

	vin.loc[(vin.model_mod.str.contains(model_str)) | ((2004 <= vin.year) & (vin.year < 2008) & (vin.model_mod == 'pacifica')) |
	((vin.make == 'honda') & (vin.model_mod.str.contains('cr')) & (vin.VIN.apply(lambda s: s[0:3] != 'jhm'))) |
	((vin.make =='honda') & (vin.model_mod == 'hr')) | ((vin.make == 'bmw') & (vin.model_mod == 'x')) |
	((vin.make =='mercedes-benz') & (vin.model_mod == 'r')) | ((vin.make == 'infiniti') & 
		(vin.model_mod.apply(lambda s: s[0:2]).str.contains(r'^ex$|^fx$|^jx$|^qx$'))) | 
	((vin.make == 'lexus') & vin.model_mod.str.contains(r'^nx$|^rx$')) | ((2010 <= vin.year) & (vin.model_mod == 'sorento')) |
	((vin.year >= 2006 ) & (vin.model_mod == 'ml')) | ((vin.model_mod >= 2019) & (vin.model_mod == 'blazer')) |
	((vin.year >= 2011) & (vin.model_mod == 'explorer')) | ((vin.year >= 2014) & (vin.model_mod == 'cherokee')) |
	(((1996 <= vin.year) & (vin.year <= 2004)) | (vin.year >= 2013) & (vin.model_mod == 'pathfinder')), 'vtyp'] = 'cuv'

	# Divide epa purpose vehicles into minivans and suvs;
	epa['vtyp2'] = epa['vtyp']
	epa.loc[epa.vtyp == 'pur', 'vtyp2'] = 'suv'
	epa.loc[(epa.vtyp == 'pur') & (epa.model_mod.isin(('grandcaravan','grandvoyager','quest','sienna','silhouette',\
		'townandcountry','venture','villager','voyager','windstar'))), 'vtyp2'] = 'min'

	# Reset index.
	vin = vin.reset_index(drop=True)
	epa = epa.reset_index(drop=True)
	
	return vin, epa

def add_type(vin, epa, default_type='0'):
	# Create tonnage (type) variable for VIN and EPA. 
	# In vin.
	# Only for 3 manufacturers.
	manufacturers = 'ford, dodge, chevrolet'.split(', ')
	vin['type'] = np.nan
    # NOTE: THIS IS CHANGED TO BE LESS RESTRICTIVE BECAUSE THERES NO VEHTYPE
	#vin.loc[(vin.vtyp3.str.contains(r'1|2')) | (vin.VehicleType.str.contains('car')), 'type'] = default_type
	vin.loc[(vin.VehicleType.str.contains('car')), 'type'] = default_type
	ton_dict = {'1/4': '15', '1/2': '15', '3/4': '25', '1': '35'}
	tonnages = '15|25|35|45|55'
	for var in 'model_mod, Series, Trim'.split(', '):
		vin.loc[vin.make.isin(manufacturers) & (vin.type != default_type) & 
			(vin[var].str.contains(r'(\D|^)({})'.format(tonnages))), 'type'] = \
				vin[var].str.extract(r'(\D|^)({})'.format(tonnages))[1]
		vin.loc[vin.make.isin(manufacturers) & (vin.type != default_type) & 
			(vin[var].str.contains(r'([^\(\s]*)\ston')), 'type'] = (
				vin[var].str.extract(r'([^\(\s]*)\ston').replace(ton_dict))
	# Replace nans with the default type string.
	vin.loc[(vin.type.isnull()) | (~vin.type.isin([default_type] + list(ton_dict.values()))), 'type'] = default_type
	# Add certain exceptions. 
	vin.loc[(vin.make == 'chevrolet') & (vin.model.str.contains(
		's10|colorado|canyon|sonoma|blazer')), 'type'] = default_type
	vin.loc[(vin.make == 'dodge') & (vin.model_mod == 'dakota'), 'type'] = default_type
	vin.loc[(vin.make == 'ford') & (vin.model_mod == 'ranger'), 'type'] = default_type
	# In epa.
	# In model name.
	epa['type'] = np.nan
	epa.loc[(epa.VClass.str.contains(r'pickup|sport|van')) & (epa.model_mod.str.contains('{}'.format(tonnages))), 
		'type'] = epa.model_mod.str.extract('({})'.format(tonnages)).replace({'10': '15'}).replace({'20': '25'})
	# Replace nans with default type. 
	epa.loc[epa.type.isnull(), 'type'] = default_type

	return vin, epa

def add_type_from_vin(vin, default_type='0'):
	# Add variable type_from_vin in vin.
	vin['type_from_vin'] = default_type
	# Ford. 
	model_str = '^f\d|^e\d'
	vin.loc[(vin.make == 'ford') & (vin.model_mod.str.contains(model_str)) & (vin.VIN.apply(lambda x: x[5]) == '1'), 
		'type_from_vin'] = '15'
	vin.loc[(vin.make == 'ford') & (vin.model_mod.str.contains(model_str)) & (vin.VIN.apply(lambda x: x[5]) == '2'), 
		'type_from_vin'] = '25'
	vin.loc[(vin.make == 'ford') & (vin.model_mod.str.contains(model_str)) & (vin.VIN.apply(lambda x: x[5]) == '3'), 
		'type_from_vin'] = '35'
	# Dodge.
	dodge_index = vin.loc[(vin.make == 'dodge') &
		(vin.year >= 1989) & (vin.year <= 1996) & vin.VIN.apply(lambda x: x[2]).isin('5,6'.split(',')) &
		vin.VIN.apply(lambda x: x[4]).isin('B,C,E,F,L,M'.lower().split(','))].index
	vin.loc[vin.index.isin(dodge_index) & (vin.VIN.apply(lambda x: x[5]) == '1'), 'type_from_vin'] = '15'
	vin.loc[vin.index.isin(dodge_index) & (vin.VIN.apply(lambda x: x[6]) == '2'), 'type_from_vin'] = '25'
	vin.loc[vin.index.isin(dodge_index) & (vin.VIN.apply(lambda x: x[5]) == '3'), 'type_from_vin'] = '35'
	dodge_index = vin.loc[(vin.make == 'dodge') &
		(vin.year >= 1989) & (vin.year <= 1996) & vin.VIN.apply(lambda x: x[2]).isin('7'.split(',')) &
		vin.VIN.apply(lambda x: x[4]).isin('B,C,E,F'.lower().split(','))].index
	vin.loc[vin.index.isin(dodge_index) & (vin.VIN.apply(lambda x: x[5]) == '1'), 'type_from_vin'] = '15'
	vin.loc[vin.index.isin(dodge_index) & (vin.VIN.apply(lambda x: x[5]) == '2'), 'type_from_vin'] = '25'
	vin.loc[vin.index.isin(dodge_index) & (vin.VIN.apply(lambda x: x[5]) == '3'), 'type_from_vin'] = '35'
	## Through 2011.
	model_str = 'ram|1500|2500|^b$|^d$|^w$'
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) &
		(vin.year <= 2011) & (vin.VIN.apply(lambda x: x[5]) == '1'), 'type_from_vin'] = '15'
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) &
		(vin.year <= 2011) & (vin.VIN.apply(lambda x: x[5]) == '2'), 'type_from_vin'] = '25'
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) &
		(vin.year <= 2011) & vin.VIN.apply(lambda x: x[5]).isin('3, 4'.split(', ')), 'type_from_vin'] = '35'
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) &
		(vin.year <= 2011) & vin.VIN.apply(lambda x: x[5]).isin('5, 6'.split(', ')), 'type_from_vin'] = '45'
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) &
		(vin.year <= 2011) & (vin.VIN.apply(lambda x: x[5]) == '7'), 'type_from_vin'] = '55'
	# Through 2018.
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) & (vin.year > 2011) & 
		((vin.VIN.apply(lambda x: x[5]).isin('A, 6, B, 7'.split(', '))) | 
			(vin.VIN.apply(lambda x: x[5:6]).isin('VA, VB, VN'.split(', ')))),
		'type_from_vin'] = '15'
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) & (vin.year > 2011) & 
		((vin.VIN.apply(lambda x: x[5]).isin('4, 5'.split(', '))) | 
			(vin.VIN.apply(lambda x: x[5:6]).isin('VC, VD, VP, VS, VT'.split(', ')))),
		'type_from_vin'] = '25'
	vin.loc[(vin.make == 'dodge') & (vin.model_mod.str.contains(model_str)) & (vin.year > 2011) & 
		((vin.VIN.apply(lambda x: x[5]).isin('C, P, S, 2, 8, D, R, T, 3, 9'.split(', '))) | 
			(vin.VIN.apply(lambda x: x[5:6]).isin('VE, VF, VG, VH, VI, VJ, VK, VL, VM, VR'.split(', ')))),
		'type_from_vin'] = '35'
	# Chevrolet.
	## Through 2008, chevy and gmc. 
	chevy_model_str = 'van|express|suburban|tahoe|^c|^k|silverado|avalanche|s10|colorado'
	gmc_model_str = 'vandura|savanna|rally|yukon|sierra|sonoma|canyon'
	# 'sierra|silverado|^c|^k|^g|express|vandura|yukon|suburban|avalanche|tahoe'
	vin.loc[(vin.make == 'chevrolet') & (vin.year <= 2008) & vin.model_mod.str.contains('|'.join([chevy_model_str, 
		gmc_model_str])) & vin.VIN.apply(lambda x: x[5]).isin('1, 6'.split(', ')), 'type_from_vin'] = '15'
	vin.loc[(vin.make == 'chevrolet') & (vin.year <= 2008) & vin.model_mod.str.contains('|'.join([chevy_model_str, 
		gmc_model_str])) &	vin.VIN.apply(lambda x: x[5]).isin('2, 7'.split(', ')), 'type_from_vin'] = '25'
	vin.loc[(vin.make == 'chevrolet') & (vin.year <= 2008) & vin.model_mod.str.contains('|'.join([chevy_model_str, 
		gmc_model_str])) &	vin.VIN.apply(lambda x: x[5]).isin('3, 8'.split(', ')), 'type_from_vin'] = '35'
	## 2009.
	vin.loc[(vin.make == 'chevrolet') & (vin.year == 2009) & vin.model_mod.str.contains('|'.join([chevy_model_str, 
		gmc_model_str])) &	vin.VIN.apply(lambda x: x[5]).isin('1, 2, 3'.split(', ')), 'type_from_vin'] = '15'
	vin.loc[(vin.make == 'chevrolet') & (vin.year == 2009) & vin.model_mod.str.contains('|'.join([chevy_model_str, 
		gmc_model_str])) &	vin.VIN.apply(lambda x: x[5]).isin('4, 5, 6'.split(', ')), 'type_from_vin'] = '25'
	vin.loc[(vin.make == 'chevrolet') & (vin.year == 2009) & vin.model_mod.str.contains('|'.join([chevy_model_str, 
		gmc_model_str])) & vin.VIN.apply(lambda x: x[5]).isin('7, 8, 9'.split(', ')), 'type_from_vin'] = '35'
	## For chevy. 
	## 2010 to 2014.
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2010) & (vin.year <= 2014) & 
		vin.model_mod.str.contains(chevy_model_str) & vin.VIN.apply(lambda x: x[5]).isin('P,R,S,T,U'.lower().split(',')),
		'type_from_vin'] = '15'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2010) & (vin.year <= 2014) & 
		vin.model_mod.str.contains(chevy_model_str) & vin.VIN.apply(lambda x: x[5]).isin('V,X,Y'.lower().split(',')),
		'type_from_vin'] = '25'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2010) & (vin.year <= 2014) & 
		vin.model_mod.str.contains(chevy_model_str) & vin.VIN.apply(lambda x: x[5]).isin('Z,0,1'.lower().split(',')),
		'type_from_vin'] = '35'	
	## 2015.
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2015) & vin.model_mod.str.contains(chevy_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('P,R,S,T,N,9'.lower().split(',')), 'type_from_vin'] = '15'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2015) & vin.model_mod.str.contains(chevy_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('U,V,W,X'.lower().split(',')), 'type_from_vin'] = '25'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2015) & vin.model_mod.str.contains(chevy_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('Y,Z,0,1'.lower().split(',')), 'type_from_vin'] = '35'	
	## For gmc. 
	## 2010 to 2014.
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2010) & (vin.year <= 2014) & 
		vin.model_mod.str.contains(gmc_model_str) & vin.VIN.apply(lambda x: x[5]).isin('T,U,V,W,X,Y'.lower().split(',')),
		'type_from_vin'] = '15'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2010) & (vin.year <= 2014) & 
		vin.model_mod.str.contains(gmc_model_str) & vin.VIN.apply(lambda x: x[5]).isin('Z,0,1,5'.lower().split(',')),
		'type_from_vin'] = '25'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year >= 2010) & (vin.year <= 2014) & 
		vin.model_mod.str.contains(gmc_model_str) & vin.VIN.apply(lambda x: x[5]).isin('2,3,4,6'.split(',')),
		'type_from_vin'] = '35'	
	## 2015.
	vin.loc[(vin.make == 'chevrolet') & (vin.year == 2015) & vin.model_mod.str.contains(gmc_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('T,U,V,W'.lower().split(',')), 'type_from_vin'] = '15'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year == 2015) & vin.model_mod.str.contains(gmc_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('X,Y,Z,0'.lower().split(',')), 'type_from_vin'] = '25'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year == 2015) & vin.model_mod.str.contains(gmc_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('1,2,3,4'.split(',')), 'type_from_vin'] = '35'
	## 2016 - 2018.
	vin.loc[(vin.make == 'chevrolet') & (vin.year > 2015) & vin.model_mod.str.contains(gmc_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('L,M,N,P,9'.lower().split(',')), 'type_from_vin'] = '15'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year > 2015) & vin.model_mod.str.contains(gmc_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('R,S,T,U'.lower().split(',')), 'type_from_vin'] = '25'	
	vin.loc[(vin.make == 'chevrolet') & (vin.year > 2015) & vin.model_mod.str.contains(gmc_model_str) &
		vin.VIN.apply(lambda x: x[5]).isin('V,W,X,Y'.lower().split(',')), 'type_from_vin'] = '35'
	return vin

def comb_list(l):
	out = []
	for n in range(len(l), 0, -1):
		combs = combinations(l, n)
		out += list(map(list, combs))
	return out

def merge(vin, epa, n_var_left=4, keep_epa_models=False, no_engine_restriction=True):
	"""
	Args:
		n_var_left: number of variables to keep at the top of the list for the merge (variables that won't be dropped). 
	"""
	no_missing_val_cols = [
		'make',
		'model_mod',
		'year',
		'fuelType1_mod',
		]

	compulsory_cols = [
		]

	missing_val_cols = [
		'type',
		'drive_mod',
		'displ_mod',
		'cylinders',
		'transmission_type_mod',
		'transmission_speeds_mod',
		]

	at_least_one_of = [
		'displ_mod',
		'cylinders',
		]

	match_col_list = comb_list(missing_val_cols)
	matched_vins = pd.DataFrame(columns=list(set(vin.columns.tolist() + epa.columns.tolist())) + ['matched_on'])
	remaining_epas = epa if keep_epa_models else None
	# Merge on all columns first, then try without the fuel type. 
	col_list = [no_missing_val_cols + compulsory_cols, no_missing_val_cols[:-1] + compulsory_cols]
	for cols in col_list:
		print(('*'*50))
		print(('Merging using:', cols))
		for match_cols in match_col_list:
			# We want at least one piece of info about the engine size to be considered in the merge. 
			if (set(match_cols) & set(at_least_one_of)) or no_engine_restriction:
				on_cols = cols + match_cols
				print(('Matching on ', on_cols))
				remaining_vins = vin.loc[~vin.VIN_ID.isin(matched_vins.VIN_ID)]
				# Make sure we don't match on the missing values. 
				remaining_vins = remaining_vins.loc[reduce(operator.and_, [(remaining_vins[x] != '-1') for x in on_cols])]
				if not keep_epa_models:
					remaining_epas = epa.loc[~epa.EPA_ID.isin(matched_vins.EPA_ID)]
					remaining_epas = remaining_epas.loc[reduce(operator.and_, [(remaining_epas[x] != '-1') for x in on_cols])]
				inner_join = pd.merge(remaining_vins.reset_index(), remaining_epas, how='inner', on=on_cols).set_index('index')
				inner_join['n_matched_on'] = len(on_cols)
				inner_join['matched_on'] = ', '.join(on_cols)
				matched_vins = pd.concat([inner_join, matched_vins])
				# Calculate and print match fraction. 
				match_fraction = (float(matched_vins['VIN, counts'.split(', ')].drop_duplicates().counts.sum())/
					vin['VIN, counts'.split(', ')].drop_duplicates().counts.sum())
				print(('Weighted match fraction: {:.2%}'.format(match_fraction)))

	on_cols = no_missing_val_cols
	print(('*'*50))
	for _ in range(len(no_missing_val_cols) - n_var_left + 1):
		print(('Matching on', on_cols))
		remaining_vins = vin.loc[~vin.VIN_ID.isin(matched_vins.VIN_ID)]
		if not keep_epa_models:
			remaining_epas = epa.loc[~epa.EPA_ID.isin(matched_vins.EPA_ID)]
		inner_join = pd.merge(remaining_vins.reset_index(), remaining_epas, how='inner', on=on_cols).set_index('index')
		inner_join['n_matched_on'] = len(on_cols)
		inner_join['matched_on'] = ', '.join(on_cols)
		matched_vins = pd.concat([inner_join, matched_vins])
		# Calculate and print match fraction. 
		match_fraction = (float(matched_vins['VIN, counts'.split(', ')].drop_duplicates().counts.sum())/
			vin['VIN, counts'.split(', ')].drop_duplicates().counts.sum())
		print(('Weighted match fraction: {:.2%}'.format(match_fraction)))
		on_cols = on_cols[:-1]

	print(('*'*50))
	# Calculate and print match fraction. 
	match_fraction = (float(matched_vins['VIN, counts'.split(', ')].drop_duplicates().counts.sum())/
		vin['VIN, counts'.split(', ')].drop_duplicates().counts.sum())
	print(('Final weighted match fraction: {:.2%}'.format(match_fraction)))

	return matched_vins, vin 




def run_merge(vin, epa):	
	####################################################################################################################
	# Merge datasets. 
	####################################################################################################################
	print('Merging datasets')
	for var in 'year'.split(', '):
		vin[var], epa[var] = vin[var].astype(str), epa[var].astype(str)		
	matched_vins, vin = merge(vin, epa, n_var_left=4, keep_epa_models=True, no_engine_restriction=False)
	return matched_vins, vin


def output(vin, epa, matched_vins, export=False, export_all=False):
	####################################################################################################################
	# Post-process datasets. 
	####################################################################################################################
	matched_vins_ids = matched_vins[['VIN_ID', 'EPA_ID', 'matched_on']].applymap(try_int)
	# Rename columns so the result of the merge is more comprehensible. 
	vin_vin = vin.rename(columns = dict(list(zip(vin.columns, vin.columns + '_vin'))))
	epa_epa = epa.rename(columns = dict(list(zip(epa.columns, epa.columns + '_epa'))))
	# Create a simplifed version of the merged dataset.
	matched_vins_simple = pd.merge(pd.merge(matched_vins_ids, vin_vin, left_on='VIN_ID', right_on='VIN_ID_vin', 
		how='left'), epa_epa, left_on='EPA_ID', right_on='EPA_ID_epa', how='left')
	matched_vins_simple.drop(['EPA_ID', 'VIN_ID'], axis=1, inplace=True)
	matched_vins_no_dupes = matched_vins_simple.sort_values(['transmission_type_mod_epa', 'comb08_epa'], ascending=True
		).drop_duplicates(subset='VIN_vin')
	vins_no_dupes = vin_vin.drop_duplicates(subset='VIN_vin')
	print(('Merge fraction weighted: {:.2%}'.format(float(matched_vins_no_dupes['counts_vin'].sum())/
		vins_no_dupes['counts_vin'].sum())))
	print(('Duplication rate: {:.4}'.format(float(len(matched_vins_simple))/float(len(matched_vins_no_dupes)))))

	# Find the VINs that weren't matched. 
	vins_matched = matched_vins_simple.VIN_vin
	epas_matched = matched_vins_simple.EPA_epa
	not_matched_vins = vin_vin.loc[~vin_vin.VIN_vin.isin(vins_matched)]
	not_matched_epas = epa_epa.loc[~epa_epa.EPA_epa.isin(epas_matched)]
	not_matched = pd.concat([not_matched_epas, not_matched_vins])

	# Create a simple version, where there aren't any suffixes (data from vin and epa are stored in the same columns).
	not_matched_vins_simple = vin.loc[~vin.VIN.isin(vins_matched)] #pd.concat([vins_matched, ignore_vins]))]
	not_matched_epas_simple = epa.loc[~epa.EPA.isin(epas_matched)]
	not_matched_simple = pd.concat([epa, not_matched_vins_simple])
	
	####################################################################################################################
	# MPG range distribution.
	####################################################################################################################
	# Create the ranges of values for each VIN. 
	group_by_vars = ['VIN_vin', 'transmission_type_mod_epa']
	matched_vins_groups = matched_vins_simple.groupby(group_by_vars)
	mpgs = 'highway08, comb08, city08'.split(', ')
	def add_end(l, s):
		return list(map(''.join, list(zip(l, [s]*len(l)))))
	mpgs_epa = add_end(mpgs, '_epa')
	_matched_vins_ranges = \
		pd.concat([matched_vins_groups[mpgs_epa].max().add_suffix('_max'),
			matched_vins_groups[mpgs_epa].min().add_suffix('_min'),	
			matched_vins_groups[mpgs_epa].count().add_suffix('_count')], axis=1).reset_index()
	# Add counts. 
	keep_cols = [
		'VIN_vin', 'year_vin', 'make_vin', 'model_vin', 'model_epa', 'fuelType1_vin', 'fuelType1_epa',
		'displ_mod_vin', 'displ_mod_epa', 'cylinders_vin', 'cylinders_epa', 'transmission_type_mod_vin', 
		'transmission_type_mod_epa', 'counts_vin'] #, 'vtyp3_vin']
	matched_vins_ranges = pd.merge(_matched_vins_ranges, matched_vins_no_dupes[keep_cols], 
		on=group_by_vars, how='right')
	matched_vins_ranges_full = pd.merge(_matched_vins_ranges, matched_vins_no_dupes, 
		on=group_by_vars, how='right')
	
	####################################################################################################################
	# Spread distribution.
	####################################################################################################################
	mpgs_epa_max = add_end(mpgs_epa, '_max')
	mpgs_epa_min = add_end(mpgs_epa, '_min')
	spread = pd.DataFrame()
	for mpg, mx, mn in zip(mpgs, mpgs_epa_max, mpgs_epa_min):
		matched_vins_ranges[mpg + '_spread'] = matched_vins_ranges[mx] - matched_vins_ranges[mn]
		print(matched_vins_ranges[mpg + '_spread'].value_counts().sort_index())
		print(matched_vins_ranges[mpg + '_spread'].value_counts().sort_index()/len(matched_vins_ranges) * 100)
		spread[mpg + '_spread_no_wgt'] = matched_vins_ranges[mpg + '_spread'].value_counts().sort_index()	
		spread[mpg + '_spread'] = matched_vins_ranges[[mpg + '_spread', 'counts_vin']].groupby(mpg + '_spread').sum()
		spread[mpg + '_spread%'] = spread[mpg + '_spread']/spread[mpg + '_spread'].sum()
		spread[mpg + '_spread%_cum'] = spread[mpg + '_spread%'].cumsum()
	spread.to_csv(r'EPA_MPG/spread.csv', encoding = 'utf8') 
	matched_vins_ranges.groupby('make_vin')['make_vin, comb08_spread'.split(', ')].describe().unstack().to_csv(
		r'EPA_MPG/spread_by_make.csv', encoding = 'utf8')

	# VINs to ignore. 
	ignore_vins = get_ignore(vin, vins_matched)
	####################################################################################################################
	# Generate output files. 
	####################################################################################################################
	# Exports.
	if export:
		matched_vins_ranges['comb08_max-min%'] = \
			matched_vins_ranges['comb08_epa_max'] / matched_vins_ranges['comb08_epa_min']
		matched_vins_ranges.loc[(matched_vins_ranges['comb08_max-min%'] >= 1.20) & 
			(matched_vins_ranges['counts_vin'] >= 500)].to_csv(r'EPA_MPG/large_spreads_and_counts_2.csv', 
				encoding = 'utf8')
		matched_vins_ranges.to_csv(r'EPA_MPG/duplicate_ranges.csv', encoding = 'utf8')

		# Export simple matched file. 
		matched_vins_simple.to_csv(r'EPA_MPG/matched_vins.csv', encoding = 'utf8')
		matched_vins_no_dupes.to_csv(r'EPA_MPG/matched_vins_no_dupes.csv', encoding = 'utf8')
		
		# Create a dataset with all records and export it.
		if export_all:
			all_records = pd.concat([not_matched, matched_vins_simple])
			all_records_no_dupes = pd.concat([not_matched, matched_vins_ranges_full])
			ordered_cols = [
				'matched_on',
				'make_epa',
				'make_vin',
				'model_epa',
				'model_mod_epa',
				'model_vin',
				'model_mod_vin',
				'year_epa',
				'year_vin',
				'fuelType1_epa',
				'fuelType1_mod_epa',
				'fuelType1_vin',
				'fuelType1_mod_vin',
				'fuelType2_vin',
				'drive_epa',
				'drive_mod_epa',
				'drive_vin',
				'drive_mod_vin',
				'displ_epa',
				'displ_mod_epa',
				'displ_vin',
				'displ_mod_vin',
				'cylinders_epa',
				'cylinders_vin',
				'transmission_speeds_epa',
				'transmission_speeds_mod_epa',
				'transmission_speeds_vin',
				'transmission_speeds_mod_vin',
				'transmission_type_epa',
				'transmission_type_mod_epa',
				'transmission_type_vin',
				'transmission_type_mod_vin',
				'type_epa',
				'VIN_vin',
				'VIN_ID_vin',
				'type_vin',
				'type_from_vin_vin',
#				'vtyp3_vin',
				'counts_vin',
				'BodyClass_vin',
				'GVWR_vin',
				'weight_vin',
				'Series_vin',
				'VehicleType_vin',
				'error_id_vin',
				'EPA_ID_epa',
				'city08_epa',
				'comb08_epa',
				'highway08_epa',
				'VClass_epa',
				'trany_epa',
				'BodyCabType_vin',
				'Doors_vin'
				]
			# Create csv files. 
			all_records[ordered_cols].to_csv(r'EPA_MPG/all_records.csv', encoding = 'utf8')
			mpg_range_vars = [
				'highway08_epa_max',
				'comb08_epa_max',
				'city08_epa_max',
				'highway08_epa_min',
				'comb08_epa_min',
				'city08_epa_min',
				'highway08_epa_count',
				'comb08_epa_count',
				]
			today_date = datetime.now().date().isoformat()
			all_records_no_dupes[ordered_cols + mpg_range_vars].to_csv(
				r'EPA_MPG/all_records_no_dupes_{}.csv'.format(today_date), encoding = 'utf8')

		# Select subset of columns for export in comparator file. 
		keep_cols = [
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
			'Trim',
			'VehicleType',
			'Series', 
			'type',
			'weight', 
#			'vtyp3',
				]

		# Update comparator file.
		#out_wb = xw.Book(r"EPA_MPG/not_matched_comparator.xlsm")
		#clear_used_range(out_wb, 'not_matched')
		#out_wb.sheets('not_matched').range(1, 1).value = not_matched_simple[keep_cols]
		#clear_used_range(out_wb, 'vin_only')

		# Generate data for records missing fuel efficiency data. 
		gen_missing_mpgs(vin, epa, ignore_vins)

		# Create a summary.
		vin_out = matched_vins_no_dupes[vin_vin.columns.tolist() + ['matched_on']]
		keep_cols = ['VIN_vin', 'highway08_epa_min', 'comb08_epa_min', 'city08_epa_min',
			'city08_epa_count', 'transmission_type_mod_epa']
		summary = pd.merge(vin_out, matched_vins_ranges[keep_cols], on='VIN_vin')
		summary.rename(columns={'city08_epa_count': 'duplicate_counts'}, inplace=True)
		summary.to_csv(r'EPA_MPG/summary.csv', encoding = 'utf8')
		print(('Unique matches: {:.2%}'.format(
			float(sum(summary.loc[summary.duplicate_counts == 1, 'counts_vin']))/sum(summary.counts_vin))))

		freq_dist = summary.duplicate_counts.value_counts(normalize=True).sort_index()

	all_dfs = {
		'epa': epa, 
		'vin': vin, 
		'matched_vins_simple': matched_vins_simple,
		'matched_vins_no_dupes': matched_vins_no_dupes,
		'ignore_vins': ignore_vins,
		'matched_vins_ranges': matched_vins_ranges, 
		}
	not_matched.to_csv('EPA_MPG.not_matched.csv', encoding = 'utf8')  
    
	return all_dfs

def gen_missing_mpgs(vin, epa, ignore_vins):
	# Missing fuel efficiency data. 
	# * MISSING IN EPA FEG?;
	# IF ERG_MODEL='Sebring' AND EPA_CYL=6 AND CITY08=. THEN DO;
	# IF ERG_MY=2001 THEN DO; CITY08=17; HIGHWAY08=25; COMB08=20; END;
	# IF ERG_MY=2003 THEN DO; CITY08=19; HIGHWAY08=25; COMB08=21; END;
	# IF 2004 LE ERG_MY LE 2005 THEN DO; CITY08=18; HIGHWAY08=25; COMB08=21; END;
	# IF 2007 LE ERG_MY LE 2011 THEN DO; CITY08=16; HIGHWAY08=27; COMB08=20; END;
	# END;

	# IF ERG_MODEL='Impala' THEN DO;
	# IF ERG_MY=2006 THEN DO; EPA_CYL=6; EPA_DISP=3.5; CITY08=17; HIGHWAY08=25; COMB08=20; END;
	# IF ERG_MY=2011 AND EPA_CYL=6 AND EPA_DISP=3.5 THEN DO; CITY08=18; HIGHWAY08=29; COMB08=22; END;
	# IF ERG_MY=2011 AND EPA_CYL=6 AND EPA_DISP=3.9 THEN DO; CITY08=17; HIGHWAY08=25; COMB08=20; END;
	# END;

	# add the mpg of models that are missing based on the ones that exist already for the heavy ones. 
	# Based on tonnage, year, van vs. truck. 
	heavy_vins = vin.loc[vin.VIN.isin(ignore_vins)]
	heavy_mpgs = pd.merge(heavy_vins, epa, on='make, model'.split(', ')).drop_duplicates(subset='VIN')

	keep_cols = [x for x in epa.columns if re.search('.*08.*', x)]
	on_vars = 'type, year'.split(', ')
	min_lookup = epa.groupby(on_vars)[keep_cols].min().reset_index()

	for _ in range(3):
		heavy_left = heavy_vins.loc[~heavy_vins.VIN.isin(heavy_mpgs.VIN)]
        try:
            mpgs = pd.concat([heavy_mpgs, pd.merge(heavy_left, min_lookup, on=on_vars)])
        except:
		on_vars = on_vars[:-1]

	heavy_mpgs = heavy_mpgs.sort_values('comb08', ascending=True).drop_duplicates(subset='VIN')

	# heavy_vins.loc[~heavy_vins.VIN.isin(heavy_mpgs.VIN)].make.unique()
	heavy_mpgs.to_csv(r'EPA_MPG/heavy.csv', encoding = 'utf8')

def modify_both_before_split(vin_mod, epa_mod):
	## Lumina model.
	pattern = r'(?=.*lumina.*)(?=.*(apv|minivan).*)'
	epa_mod.loc[epa_mod.model.astype(str).str.contains(pattern), 'model_mod'], 
	vin_mod.loc[vin_mod.model.astype(str).str.contains(pattern), 'model_mod'] = 'luminaapv'
	# For Saab and Honda models, drop the dash. Note that this could be done after the splitting is done also. 
	for df in vin_mod, epa_mod:
		df.loc[df.make.astype(str).str.contains('saab|honda'), 'model_mod'] = \
			df.loc[df.make.astype(str).str.contains('saab|honda'), 'model_mod'].astype(str).str.replace('-', '')

	return vin_mod, epa_mod

def modify_vin_before_split(vin_mod):
	# For chrysler models, replace town & country with townandcountry and new yorker with newyorker. 
	vin_mod.loc[(vin_mod.make == 'chrysler') & (vin_mod.model_mod == 'town & country'), 'model_mod'] = \
		'townandcountry'
	vin_mod.loc[(vin_mod.make == 'chrysler') & (vin_mod.model_mod == 'new yorker'), 'model_mod'] = \
		'newyorker'
	# Pontiac model formula & convertible should be firebird.
	vin_mod.loc[vin_mod.model_mod.str.contains('formula'), 'model_mod'] = 'firebird'
	return vin_mod

def modify_epa_before_split(epa_original, separator):
	"Modify EPA models before splitting them where there is a separator in separate rows."
	index_mod = epa_original.loc[epa_original.model.str.contains(separator)].index
	epa_original.loc[index_mod, 'model_mod'] = \
		epa_original.loc[index_mod, 'model_mod'].apply(trim_slashes, args=(separator,))
	## Delete all spaces from a subset of the makes. 
	makes_no_spaces = 'bmw, buick, cadillac, lexus, subaru, rolls-royce'.split(', ')
	index_mod = epa_original.loc[(epa_original.make.isin(makes_no_spaces)) & 
		(epa_original.model.str.contains(separator))].index
	epa_original.loc[index_mod, 'model_mod'] = \
		epa_original.loc[index_mod, 'model_mod'].replace(' ', '', regex=True)
	## Dodge models.
	epa_original.loc[(epa_original.make == 'dodge') & epa_original.model.str.contains(r'caravan c/v/grand caravan'),
		'model_mod'] = 'caravan/grandcaravan'	
	epa_original.model_mod = epa_original.model_mod.str.replace('grand caravan', 'grandcaravan')
	## Monte-carlo model.
	pattern = re.compile('monte carlo')
	index_mod = epa_original.loc[(epa_original.make == 'chevrolet')].index
	epa_original.loc[index_mod, 'model_mod'] = \
		epa_original.loc[index_mod, 'model_mod'].apply(lambda x: replace_spaces(x, pattern, ''))
	## Chrysler models. 
	models_w_spaces = r'new yorker, town and country, fifth avenue, grand \S*'.split(', ')
	pattern = re.compile('(?=(' + '|'.join('{}'.format(x) for x in models_w_spaces) + '))')
	index_mod = epa_original.loc[(epa_original.make == 'chrysler') & epa_original.model_mod.str.contains(pattern)].index
	epa_original.loc[index_mod, 'model_mod'] = \
		epa_original.loc[index_mod, 'model_mod'].apply(lambda x: replace_spaces(x, pattern, ''))
	## Ferrari models. 
	pattern = re.compile(r'\S* f1')
	index_mod = epa_original.loc[(epa_original.make == 'ferrari') & epa_original.model_mod.str.contains(pattern)].index
	epa_original.loc[index_mod, 'model_mod'] = \
		epa_original.loc[index_mod, 'model_mod'].apply(lambda x: replace_spaces(x, pattern, ''))
	epa_original.loc[epa_original.make == 'ferrari', 'model_mod'] = \
		epa_original.loc[epa_original.make == 'ferrari', 'model_mod'].apply(lambda s: s.replace('ferrari', '').strip())
	## Mercedes models. 
	epa_original.loc[epa_original.make == 'mercedes-benz', 'model_mod'] = \
		epa_original.loc[epa_original.make == 'mercedes-benz', 'model_mod'].apply(lambda s: s.replace('600sel', '600 sel'))
	## Pontiac models. 
	## Turn [u'trans sport 2wd', u'firebird/trans am', u'trans sport/montana 2wd'] into
	## [u'trans 2wd', u'firebird/trans', u'trans/montana 2wd']
	pattern = re.compile(r'(.*)(trans [^\s/]*)(.*)')
	index_mod = epa_original.loc[(epa_original.make == 'pontiac') & epa_original.model_mod.str.contains(pattern)].index
	epa_original.loc[index_mod, 'model_mod'] = \
		epa_original.loc[index_mod, 'model_mod'].apply(lambda s: re.sub(pattern, r'\1trans\3', s))
	epa_original.loc[epa_original.model_mod.str.contains('firebird'), 'model_mod'] = 'firebird'

	return epa_original

def split_row(s, separator):
	pattern1 = re.compile(r'(.*?)(?=\S*(?:{}) *\S*?)(\S*)(.*)'.format(separator))
	if pattern1.search(s):
		groups = pattern1.search(s).groups()
		parts = [ss.strip() for ss in re.split(separator, groups[1])]
		# For cases like: srt-8/9
		pattern2 = re.compile(r'([\w\W]*?)(\d+)$') # e.g. srt-9
		subparts = []
		for part in parts:
			if re.search(pattern2, part):
				subparts.append(re.search(pattern2, part).groups())
		# Check that we're in a case like srt-8/9 and not, 636/mrx-8.
		if len(subparts) == len(parts) and subparts[0][0] != '':
			def find_non_blank(t):
				try: return reduce(lambda x, xs: x if x != '' else xs[0], t)
				except: return ''
			default = list(map(find_non_blank, list(zip(*subparts))))
			parts = [(subpart1 or default[0]) + (subpart2 or default[1]) for (subpart1, subpart2) in subparts]
		return list(map(''.join, [[groups[0], x, groups[2]] for x in parts]))
	else:
		return [s]

def try_int(a):
	try:
		return int(a)
	except:
		return a

def trim_slashes(s, separator):
	for sep in separator.split('|'):
		s = re.sub(' *{} *'.format(sep).encode('string-escape'), sep.encode('string-escape'), s)
	return s

def get_ignore(vin, vins_matched):
	# Create an ignore list for the vin dataset. 
	## Turn type into int. 
	not_matched_vins = vin.loc[~vin.VIN.isin(vins_matched)]
	not_matched_vins.type = not_matched_vins.type.apply(try_int)
	ignore_vins = pd.Series()
	ignore_vins = pd.concat([ignore_vins, 
		not_matched_vins.loc[
			(not_matched_vins.make == 'ford') & 
			(not_matched_vins.model_mod.str.contains('f250|f350|f450|e150|e250|e350|excursion|expedition')), 'VIN']])
	ignore_vins = pd.concat([ignore_vins, 
		not_matched_vins.loc[(not_matched_vins.make == 'dodge') & (not_matched_vins.type >= 15), 'VIN']])
	ignore_vins = pd.concat([ignore_vins, 
		not_matched_vins.loc[(not_matched_vins.make == 'chevrolet') & (not_matched_vins.type >= 25), 'VIN']])

	return ignore_vins



## Replace spaces. 
def replace_spaces(s, pattern, replace_with='-'):
	replace_with_ = lambda s: s.replace(' ', replace_with)
	found_strs = re.findall(pattern, s)
	for _s in found_strs:
		s = re.sub(_s, replace_with_(_s), s)
	return s


def main():
    vin_original, epa_original, counts = load(VIN_path,EPA_path)
    vin_original_fixed, epa_original_fixed = fix(vin_original, epa_original, counts, init_yr=0)
    #run merge
    vin_expanded, epa_expanded = split_and_expand(vin_original_fixed, epa_original_fixed)
    vin, epa = modify(vin_expanded, epa_expanded)
    t0 = datetime.now()
    matched_vins, vin_merge = run_merge(vin, epa)    
    t1 = datetime.now()
    print '\nRuntime: {}'.format(t1-t0)
    dfs = output(vin, epa, matched_vins, export=True, export_all=True)
    
    
    
    