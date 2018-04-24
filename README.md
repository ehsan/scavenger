Scavenger: Find CryptoJacking scripts on the Web
================================================

Python MapReduce jobs for processing the Common Crawl in order to find CryptoJacking scripts on the web.

### Getting started

Make sure you have an EC2 account set up (http://aws.amazon.com). You'll need the following handy:

1. Your access key and secret to launch EC2 instances
2. A bucket to use on S3 for storing intermediate and final jobs results
3. Sign up for Elastic MapReduce (EMR). You don't need to launch any clusters as MRJob creates a temporary one on each job launch by default (although that is configurable if you want to reuse an existing cluster see: http://mrjob.readthedocs.org/en/latest/guides/emr-advanced.html).

### Basic usage of command-line jobs

This repo uses MRJob, a Python MapReduce library from Yelp which is particularly useful for spinning up temporary Elastic MapReduce (EMR) clusters on AWS, running a single job and spinning down instances on failure/completion. MRJob can run locally or on a standard (non-EMR) Hadoop cluster, but we focus on EMR and the more cost-effective spot instances by default since running a job over the full Common Crawl requires > 100 cores.

1. Download the repo and start a config:
```bash
git clone https://github.com/ehsan/scavenger
cd scavenger
virtualenv .
bin/activate
cat mrcc.py scavenger.py | sed "s/from mrcc import CCJob//" > scavenger_emr.py
```

2. Using your favorite text editor, open up mrjob.conf. There are several numbered, commented lines in the file that indicate where you need to change various settings (your EC2 access key, etc.) Feel free to drop us a line if there are any problems with this part.

3. Start the job:
```bash
python scavenger_emr.py -r emr --conf-path mrjob.conf --output-dir s3://some-bucket/path/ --mrjob-args-go-here `bin/get_latest_cc`
```

`get_latest_cc` is a shell script that pulls the warcs.path (just a manifest file with pointers to the crawl data, ~10MB) down to local disk and uses it as input to the job.

All MRJob options are valid, see the documentation (http://mrjob.readthedocs.org/en/latest/index.html) for more details.
