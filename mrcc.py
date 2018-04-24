import gzip
import logging
import os.path as Path

from tempfile import TemporaryFile

import boto3
import botocore
import warc

from mrjob.job import MRJob
from mrjob.util import log_to_stream

from gzipstream import GzipStreamFile


# Set up logging - must ensure that log_to_stream(...) is called only once
# to avoid duplicate log messages (see https://github.com/Yelp/mrjob/issues/1551).
LOG = logging.getLogger('CCJob')
log_to_stream(format="%(asctime)s %(levelname)s %(name)s: %(message)s", name='CCJob')


class CCJob(MRJob):
    """
    A simple way to run MRJob jobs on CommonCrawl Data
    """
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
        """
        The Map of MapReduce
        If you're using Hadoop or EMR, it pulls the CommonCrawl files from S3,
        otherwise it pulls from the local filesystem. Dispatches each file to
        `process_record`.
        """
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
            except botocore.exceptions.ClientError as exception:
                LOG.error('Failed to access bucket "commoncrawl": %s', exception)
                return
            # Check whether WARC/WAT/WET input exists
            try:
                s3client.head_object(Bucket='commoncrawl',
                                     Key=line)
            except botocore.client.ClientError as exception:
                LOG.error('Input not found: %s', line)
                return
            # Start a connection to one of the WARC/WAT/WET files
            LOG.info('Loading s3://commoncrawl/%s', line)
            try:
                temp = TemporaryFile(mode='w+b',
                                     dir=self.options.s3_local_temp_dir)
                s3client.download_fileobj('commoncrawl', line, temp)
            except botocore.client.ClientError as exception:
                LOG.error('Failed to download %s: %s', line, exception)
                return
            temp.seek(0)
            try:
                #ccfile = warc.WARCFile(fileobj=(GzipStreamFile(temp)))
                ccfile = warc.WARCFile(fileobj=(gzip.open(temp)))
            except Exception as exception:
                LOG.error('Failed to open %s at %s: %s', temp, line, exception)
                return
        # If we're local, use files on the local file system
        else:
            line = Path.join(Path.abspath(Path.dirname(__file__)), line)
            LOG.info('Loading local file %s', line)
            try:
                ccfile = warc.WARCFile(fileobj=gzip.open(line))
            except Exception as exception:
                LOG.error('Failed to open %s: %s', line, exception)
                return

        for _i, record in enumerate(ccfile):
            for key, value in self.process_record(record):
                yield key, value
            self.increment_counter('commoncrawl', 'processed_records', 1)

    def combiner(self, key, values):
        """
        Combiner of MapReduce
        Default implementation just calls the reducer which does not make
        it necessary to implement the combiner in a derived class. Only
        if the reducer is not "associative", the combiner needs to be
        overwritten.
        """
        for key_val in self.reducer(key, values):
            yield key_val

    def reducer(self, key, values):
        """
        The Reduce of MapReduce
        If you're trying to count stuff, this `reducer` will do. If you're
        trying to do something more, you'll likely need to override this.
        """
        yield key, sum(values)
