import os
import glob
import re
import tarfile
import gzip
from datetime import datetime

# This script allows for bash execution in python
def sh(script):
    os.system("bash -c \"%s\"" % script)

# Write out file as gzip - currently not using this
def write_gzip(input_file):
    try:
        output_file = input_file + '.gz'
        print "Starting time: {0}".format(datetime.now())
        f_in = open(input_file, 'rb')
        f_out = gzip.open(output_file, 'wb')
        f_out.writelines(f_in)
        f_out.close()
        f_in.close()
        print "ending time: {0}".format(datetime.now())
    except Exception as e:
        print e
        raise
def extractTar():
    home = '/data/adobe-data'
    s3_adobe_landing = "s3://aarp-landing/adobe/"
    s3_adobe_archive = "s3://aarp-archive/adobe/"

    print "Starting Adobe Untar-Reload at {0}".format(datetime.now())
    print "Read in tar.gz files from S3 Landing"

    S3_copy_str = "aws s3 cp {0} {1}  --recursive --exclude '*' --include '*.tar.gz' --profile=adobe".format(
        s3_adobe_landing, home)
    print S3_copy_str
    sh(S3_copy_str)

    print "File Ingest Completed at {0}".format(datetime.now())

    # get all files in directory
    files = glob.glob('{0}/*.tar.gz'.format(home))
    files_extracted = []

    for file in files:
        # Get directory name from file name
        temp = re.findall(r"(\d{4}-\d{2}-\d{2})", file)
        year_date = temp[0]
        print("Extracted {0} from {1}".format(year_date, file))

        try:
            base_file = os.path.basename(file)
            print("Starting extract of {0} at {1}".format(base_file, datetime.now()))

            output_dir = '{0}/{1}'.format(home, year_date)
            # os.mkdir('outdir')
            t = tarfile.open(file, 'r:gz')
            t.extractall(output_dir)
            # print os.listdir('outdir')
            print("Extract of {0} is complete at {1}".format(base_file, datetime.now()))

            files_extracted.append(base_file)

        except Exception as e:
            print e
            raise

        # File successfully unzipped, now Send to S3
        try:
            print("Starting to Write {0} to {1} at {2}".format(output_dir, s3_adobe_landing, datetime.now()))
            # using AWS CLI through Bash
            sh("aws s3 cp {0} {1}{2}  --recursive --profile=adobe".format(output_dir, s3_adobe_landing, year_date))

            print("Write Complete at {0}".format(datetime.now()))

            # once write to S3 is succcessful - remove directory - it too big to keep there
            print("Removing directory {0} from local".format(output_dir))
            sh("rm -r {0}".format(output_dir))

            # Get year_month from year date - just remove last three characters
            year_month = year_date[:-3]

            # now move tar.gz into archive
            print("Moving {0} to archive".format(base_file))
            sh("aws s3 mv {0}{1} {2}{3}/{1} --profile=adobe".format(s3_adobe_landing, base_file, s3_adobe_archive,
                                                                    year_month))
            base_file
            # delete tar files from ingest server once all extractions successfully completed
            print("Removing {0} from local".format(base_file))
            sh("rm {0}/{1}".format(home, base_file))
        except Exception as e:
            print e
            raise

