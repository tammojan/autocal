# AutoCal: automatically trigger Apercal pipeline
# V.A. Moss 08/10/2018 (vmoss.astro@gmail.com)

__author__ = "V.A. Moss"
__date__ = "$29-nov-2018 17:00:00$"
__version__ = "0.1"

import os
import sys
from numpy import *
from argparse import ArgumentParser, RawTextHelpFormatter
from astropy.io import ascii
import datetime
import time
from modules.functions import *
import glob

#from apercal.subs import getdata_alta
from apercal.pipeline import start_apercal_pipeline

def main():
	"""
	The main program to be run.
	:return:
	"""

	# Time the total process length
	start = time.time()

	# Parse the relevant arguments
	parser = ArgumentParser(formatter_class=RawTextHelpFormatter)
	parser.add_argument('-a', '--alta_path',
			default='/altaZone/archive/apertif_main/visibilities_default/',
			help='Specify path location on ALTA (default: %(default)s)')


	# Parse the arguments above
	args = parser.parse_args()

	# Identify which host we are running on
	hostname = os.popen('hostname').read().strip()

	# Text file to keep track of processed datasets
	processed = ascii.read('processed.txt')
	done_tids = processed['tid']

	# # Text file to keep track of processed measurement sets specifically
	# processed_ms = ascii.read('processed_ms.txt')
	# done_ms = processed_ms['msname']

	# Call ALTA to find new datasets
	path = args.alta_path
	tids = check_alta(path,done_tids)

	# Check if the script is already running
	status = glob.glob('RUNNING*')

	# Print results
	if len(tids) > 0 and 'RUNNING' not in status:
		print('New data found!\n%s' % ('\n'.join(tids)))

		# Make a running file
		os.system('touch RUNNING')

		# Loop through and make a function call, writing to a new results folder
		for tid in tids:

			# Reset the success term
			success = False

			# Let's make it an integer
			tid = int(tid)

			# try: 

			# 	# First check if the data already exists, and if so, skip this step
			# 	#data_exists = check_happili(path,tid)
			# 	data_exists = 'N'

			# 	# Decide which beams based on node, in groups of 10
			# 	start_beam = (int(hostname[-1])-1)*10
			# 	end_beam = start_beam + 9
			# 	beamlist = arange(start_beam,end_beam+1)

			# 	# Otherwise, download the data
			# 	if data_exists == 'N':
			# 		try:
			# 			os.mkdir('/data/apertif/%s' % tid)
			# 		except:
			# 			print('Directory already exists!')
			# 		os.chdir('/data/apertif/%s' % tid)

			# 		# Get data with altadata library
			# 		getdata_alta(tid[0:6], int(tid[6:]), list(range(start_beam, end_beam+1)))
			# 		#cmd = ('python ~/altadata/getdata_alta.py %s %s-%s %.2d-%.2d N' % (tid[0:6],tid[6:],tid[6:],start_beam,end_beam))
			# 		print(cmd)
			# 		os.system(cmd)

			# 	else: 
			# 		print("Data already exists for %s! Continuing to pipeline..." % tid)

			# except:
			# 	print('Something went wrong during download for %s...' % tid)
			# 	sys.exit()

			# Call the pipeline script (we are downloading within that now)
			#try:
			if True:

				# Do the Apercal thing
				
				# Check if we have identified an actual target 
				# If it is a target, it will also bring over the calibrators
				tdict = identify_target(tid)

				if tdict['type'] == 'target':

					target_name = tdict['target_name']

					print('Processing target observations: %s (%s)...' % (target_name,tid))

					# Determine which beams we want to process
					start_beam = (int(hostname[-1])-1)*10
					end_beam = start_beam + 9
					beamlist = arange(start_beam,end_beam+1)
					cal_beamlist = [0]

					# print info from tdict
					print('Flux calibrator: %s (%s)' % (tdict['cal1_name'],tdict['cal1']))
					print('Polarisation calibrator: %s (%s)' % (tdict['cal2_name'],tdict['cal2']))

					msg_color = 'good'
					msg_text = "Apercal pipeline triggered for %s: %s" % (tid, target_name)
					send_to_slack(msg_color, msg_text)

					# flux_status,flux_caltable = start_fluxcal_pipeline(tdict['calibrator1'][0:6],tdict['calibrator1'][6:],cal_beamlist)
					# pol_status,pol_caltable = start_polcal_pipeline(tdict['calibrator2'][0:6],tdict['calibrator2'][6:],cal_beamlist)
					# status,results_path = start_target_pipeline(tdict['target'][0:6],tdict['target'][6:],beamlist,flux_caltable,pol_caltable)

					#try: 
						# New format for pipeline call (all in one)
					#success = start_apercal_pipeline((tdict['cal1'],tdict['cal1_name'],cal_beamlist),
																	 # (tdict['cal2'],tdict['cal2_name'],cal_beamlist),
																	 # (tdict['target'],tdict['target_name'],beamlist))
					
					success = True
					# except Exception:
					# 	success = False

					# If all success, write to the processed file
					if success:
						print("Success! Pipeline has been triggered for %s... finalising..." % tid)
						os.chdir('/home/moss/autocal/%s/' % hostname)
						out = open('processed.txt','a')
						out.write('%s %s\n' % (tid,str(datetime.datetime.now())))
						out.flush()

						msg_color = 'good'
						msg_text = "Apercal pipeline finished successfully for %s: %s" % (tid, target_name)
						send_to_slack(msg_color, msg_text)

					else:
						msg_color = 'danger'
						msg_text = "Apercal pipeline triggering *FAILED* for %s: %s" % (tid, target_name)
						send_to_slack(msg_color, msg_text)

				else:
					print('%s (%s) is not a target... Continuing!' % (tdict['target_name'],tid))
					out = open('processed.txt','a')
					out.write('%s %s\n' % (tid,str(datetime.datetime.now())))
					out.flush()

			# except IOError:
			# 	print('Something went wrong during triggering Apercal for %s...' % tid)
			# 	sys.exit()


		# Print the finish time
		end = time.time()
		total = end-start
		print('Total time to process all new datasets: %.2f min' % (total/60.))

		# Set to no longer running
		os.system('rm -rf RUNNING')
	
		# Also move the log
		os.system('mv autocal*.log logs/')


	elif 'RUNNING' in status:
		print ("Code is currently running... exiting!")

	else:
		print ("No new datasets as of %s... exiting!" % str(datetime.datetime.now()))
		os.system('mv autocal*.log logs/')


if __name__ == '__main__':
	main()
