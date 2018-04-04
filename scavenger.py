from mrcc import CCJob

import csv
import urllib2
import hashlib

class Scavenger(CCJob):
    hashes = list()

    def configure_options(self):
        try:
            samples = "https://raw.githubusercontent.com/ehsan/scavenger/master/data/samples.txt"
            with urllib2.urlopen(samples) as file:
                reader = csv.reader([file.read()])
                for row in reader:
                    name = row[0]
                    url = row[1]

                    try:
                        response = urllib2.urlopen(url).read()
                        # Try to use the first 2048 characters
                        response = response[:2048]
                        hash = hashlib.sha1(response).hexdigest()
                        self.hashes.append((name, hash))
                    except Exception:
                        continue
        except Exception:
            return

    def process_record(self, record):
        # WARC records have three different types:
        #  ["application/warc-fields", "application/javascript; msgtype=request", "application/javascript; msgtype=response"]
        # We're only interested in the HTTP responses
        if 'javascript' in record['Content-Type'] and \
           'msgtype=response' in record['Content-Type']:
            payload = record.payload.read()
            # Try to use the first 2048 characters
            content = payload[:2048]
            hash = hashlib.sha1(content).hexdigest()
            for entry in self.hashes:
                if hash == entry[1]:
                    # Found a cryptojacker!
                    yield entry[0], record.url
                    self.increment_counter('commoncrawl', 'found_scripts', 1)


if __name__ == '__main__':
    Scavenger.run()
