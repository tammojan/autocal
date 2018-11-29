# AutoCal: automatically trigger Apercal pipeline
# V.A. Moss 08/10/2018 (vmoss.astro@gmail.com)

__author__ = "V.A. Moss"
__date__ = "$29-nov-2018 17:00:00$"
__version__ = "0.1"

import os
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from astropy.io import ascii
import datetime
import time
from modules.functions import *
import glob

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

	# Text file to keep track of processed datasets
	processed = ascii.read('processed.txt')
	done_tids = processed['tid']

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

			try: 

				# First check if the data already exists, and if so, skip this step
				#data_exists = check_happili(path,tid)
				data_exists = 'N'

				# Otherwise, download the data
				if data_exists == 'N':
					try:
						os.mkdir('/data/apertif/%s' % tid)
					except:
						print('Directory already exists!')
					os.chdir('/data/apertif/%s' % tid)

					# Decide which beams based on node, in groups of 10
					hostname = os.popen('hostname').read().strip()
					start_beam = (int(hostname[-1])-1)*10
					end_beam = start_beam + 9

					# Get data with altadata library
					cmd = ('python ~/altadata/getdata_alta.py %s %s-%s %.2d-%.2d N' % (tid[0:6],tid[6:],tid[6:],start_beam,end_beam))
					print(cmd)
					os.system(cmd)

				else: 
					print("Data already exists for %s! Continuing to pipeline..." % tid)

			except:
				print('Something went wrong during download for %s...' % tid)
				sys.exit()

			# Call the pipeline script
			try:

				# Do the Apercal thing
				something = True
				if something == True:
					print('APERCAL PIPELINE STUFF WOULD GO HERE!')

				else:
					print('Some error message for %s... Continuing!' % tid)
			except:
				print('Something went wrong during running Apercal for %s...' % tid)
				sys.exit()

			# If all success, write to the processed file
			success = True
			print("Success! Pipeline has been run for %s... finalising..." % tid)
			if success == True:
				os.chdir('/home/moss/autocal')
				out = open('processed.txt','a')
				out.write('%s %s\n' % (tid,str(datetime.datetime.now())))
				out.flush()

				# Remove the data from happili (it won't try to generate again if in text file)
				os.system('rm -rf /data/apertif/%s' % tid)

				# Send a slack hook (TBD)
				# cmd = """curl -X POST --data-urlencode 'payload={"text":"Inspection plots for %s are ready! See: http://apertifsurveys.wordpress.com/%s"}' https://hooks.slack.com/services/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""" % (tid,tid)
				# print(cmd)
				# os.system(cmd)

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
		#os.system('mv autoplot*.log logs/')

	else:
		print ("No new datasets as of %s... exiting!" % str(datetime.datetime.now()))
		os.system('mv autocal*.log logs/')


if __name__ == '__main__':
    main()



