# AutoCal: modules for automatic running of Apercal
# V.A. Moss 08/10/2018 (vmoss.astro@gmail.com)

__author__ = "V.A. Moss"
__date__ = "$29-nov-2018 17:00:00$"
__version__ = "0.1"

import os
import sys
import glob
import pyrap.tables as pt
import requests
import json
import datetime

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
		if folder.startswith('1') and '_INSP' not in folder and float(folder) not in done_tids:
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
# Function to identify whether a dataset is a target
# Based on a call to ATDB for now, tbd in future

def identify_target(tid):

	calibrators = ['3C48',
				   '3C138',
				   '3C147',
				   '3C196',
				   '3C286',
				   '3C295',
				   'CTD93']

	# Get information from json
	sdict = get_json_info(tid)

	# Return info dictionary
	tdict = {}

	# Decide based on length of observation
	# Targets will always be longer than 11 hours right?
	if sdict['duration'] >= 11:

		# Identify as a target
		tdict['target'] = tid
		tdict['type'] = 'target'
		tdict['target_name'] = sdict['name']

		# Find calibrator 1 (Flux Calibrator)
		# Hack to make shakedown work... 
		cdict1 = get_json_info(tid+2)
		if cdict1['duration'] <= 0.5 and cdict1['name'] in calibrators:
			tdict['cal1'] = tid+2
			tdict['cal1_name'] = cdict1['name']

		# Find calibrator 2 (Polarisation Calibrator)
		# Hack to make shakedown work... 
		cdict2 = get_json_info(tid+1)
		if cdict2['duration'] <= 0.5 and cdict2['name'] in calibrators:
			tdict['cal2'] = tid+1
			tdict['cal2_name'] = cdict2['name']

	else:

		# Identify as a non-target
		tdict['target'] = tid
		tdict['type'] = 'non-target'
		tdict['target_name'] = sdict['name']

	return tdict


###################################################################
# Function to get the info JSON
def get_json_info(tid):

	# Check for observation on ATDB
	try:
		response = requests.get('http://atdb.astron.nl/atdb/observations/?taskID=%s' % tid)
	except:
		print("Could not locate observation information!")
		return {'name' : None, 'start' : None, 'end' : None, 'duration' : None}

	# If it reads, check whether it is a target
	json_data = json.loads(response.text)

	# Get the information
	name = json_data['results'][0]['field_name']
	start = json_data['results'][0]['starttime']
	end = json_data['results'][0]['endtime']

	# Calculate the duration
	start_dt = datetime.datetime.strptime(start,'%Y-%m-%dT%H:%M:%SZ')
	end_dt = datetime.datetime.strptime(end,'%Y-%m-%dT%H:%M:%SZ')
	duration_dt = end_dt - start_dt
	duration = (duration_dt.days*24 + duration_dt.seconds / 3600.) # hours

	return {'name' : name, 'start' : start, 'end' : end, 'duration' : duration}

###################################################################
# Slack hook function
def send_to_slack(msg_color, msg_text):

	# Construct the full message
	full_msg = """{
	"attachments": [
		{
			"color": "%s",
			"author_name": "AutoCalBot",
			"title": "AutoCal Status Report",
			"title_link": "http://ganglia.astron.nl/?c=happili",
			"text": "%s"
	   }
	]
}""" % (msg_color, msg_text)
	
	# Send the command
	cmd = """curl -X POST --data-urlencode 'payload=%s' https://hooks.slack.com/services/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""" % (full_msg)
	print(cmd)
	os.system(cmd)



