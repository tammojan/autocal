# AutoCal: modules for automatic running of Apercal
# V.A. Moss 08/10/2018 (vmoss.astro@gmail.com)

__author__ = "V.A. Moss"
__date__ = "$29-nov-2018 17:00:00$"
__version__ = "0.1"

import os
import sys
import glob
import pyrap.tables as pt

###################################################################
# Function to check ALTA for new archived datasets

def check_alta(path,done_tids):

	# Get new tids
	cmd = os.popen('ils %s' % path)
	potential_tids = []
	tids = []

	# Process new ones (only those starting with 1*)
	for x in cmd:
		folder = x.split('/')[-1].strip()
		if folder.startswith('1') and float(folder) not in done_tids:
			potential_tids.append(folder)

	# Check the status (e.g. is it archived?)
	for tid in potential_tids:

		# Check the archive status
		cmd = os.popen('imeta ls -C %s%s' % (path,tid)).read()
		status = cmd.split('attribute: ALTA_State')[1].split('value: ')[1].split('\n')[0]
		if status == 'ARCHIVED':
			tids.append(tid)

	# Return the final list of new ones
	return tids

###################################################################
# Function to check if data already exists

def check_happili(path,tid):

	# Check for folder
	try:
		total_happili = float(os.popen('du -sc /data/apertif/%s' % tid).read().split('\t')[0])/1e6
	except:
		total_happili = 0.0
	
	# Check the size in ALTA
	cmd_text = (""" iquest "%%s: %%s" "SELECT COLL_NAME, sum(DATA_SIZE) WHERE COLL_NAME like '%s%s/%%.MS'" """ % (path,tid))
	cmd = os.popen(cmd_text)
	alta_sizes = []
	for line in cmd:
		col = line.split()
		alta_size = float(col[-1])
		alta_sizes.append(alta_size)

	# Total ALTA size in GB
	total_alta = (sum(alta_sizes)/1e9)	

	# Assume ALTA is always bigger and within 10%
	if total_happili > 0:
		if (total_alta/total_happili) >= 1.0:
			data_exists = 'Y'
		else:
			data_exists = 'N'
	else:
		data_exists = 'N'

	return data_exists

###################################################################