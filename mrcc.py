import gzip
import os.path as Path
import sys

from tempfile import TemporaryFile

import boto3
import botocore
import warc

from mrjob.job import MRJob
from gzipstream import GzipStreamFile


class CCJob(MRJob):
    def configure_options(self):
        super(CCJob, self).configure_options()
        self.pass_through_option('--runner')
        self.pass_through_option('-r')
        self.add_passthrough_option('--s3_local_temp_dir',
                                    help='local temporary directory to buffer content from S3',
                                    default=None)

    def process_record(self, record):
        """
        Override process_record with your mapper
        """
        raise NotImplementedError('Process record needs to be customized')

    def mapper(self, _, line):
        # If we're on EC2 or running on a Hadoop cluster, pull files via S3
        if self.options.runner in ['emr', 'hadoop']:
            # Connect to Amazon S3 using anonymous credentials
            boto_config = botocore.client.Config(
                signature_version=botocore.UNSIGNED,
                read_timeout=180,
                retries={'max_attempts' : 20})
            s3client = boto3.client('s3', config=boto_config)
            # Verify bucket
            try:
                s3client.head_bucket(Bucket='commoncrawl')
            except botocore.exceptions.ClientError as e:
                sys.stderr.write('Failed to access bucket "commoncrawl": {}\n'.format(e))
                return
            # Check whether WARC/WAT/WET input exists
            try:
                s3client.head_object(Bucket='commoncrawl',
                                     Key=line)
            except botocore.client.ClientError as exception:
                sys.stderr.write('Input not found: {}\n'.format(line))
                return
            sys.stderr.write('Loading s3://commoncrawl/{}\n'.format(line))
            # Start a connection to one of the WARC/WAT/WET files
            try:
                temp = TemporaryFile(mode='w+b',
                                     dir=self.options.s3_local_temp_dir)
                s3client.download_fileobj('commoncrawl', line, temp)
            except botocore.client.ClientError as exception:
                sys.stderr.write('Failed to download {}: {}\n'.format(line, exception))
                return
            temp.seek(0)
            ccfile = warc.WARCFile(fileobj=(GzipStreamFile(temp)))
        # If we're local, use files on the local file system
        else:
            line = Path.join(Path.abspath(Path.dirname(__file__)), line)
            sys.stderr.write('Loading local file {}\n'.format(line))
            ccfile = warc.WARCFile(fileobj=gzip.open(line))

        for i, record in enumerate(ccfile):
            for key, value in self.process_record(record):
                yield key, value
            self.increment_counter('commoncrawl', 'processed_records', 1)

    def combiner(self, key, value):
        # use the reducer by default
        for key_val in self.reducer(key, value):
            yield key_val

    def reducer(self, key, value):
        yield key, sum(value)
