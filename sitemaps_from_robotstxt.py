import logging
import re

try:
    # Python2
    from urlparse import urlparse
    from urlparse import urljoin
except ImportError:
    # Python3
    from urllib.parse import urlparse
    from urllib.parse import urljoin

from mrcc import CCJob

sitemap_regex = re.compile('^sitemap:\s*(\S+)', re.I)


class SitemapExtractor(CCJob):
    '''Extract sitemap URLs (http://www.sitemaps.org/) from robots.txt WARC files.'''

    def process_record(self, record):
        '''emit: sitemap_url => [host]'''
        if record['WARC-Type'] != 'response':
            # we're only interested in the HTTP responses
            return
        url = None
        self.increment_counter('commoncrawl', 'robots.txt processed', 1)
        for line in record.payload:
            match = sitemap_regex.match(line)
            if match:
                sitemap_url = match.group(1).strip()
                self.increment_counter('commoncrawl', 'sitemap URLs found', 1)
                try:
                    sitemap_url.decode("utf-8", "strict")
                except:
                    # invalid encoding, ignore
                    logging.warn('Invalid encoding of sitemap URL: {}'.format(sitemap_url))
                    self.increment_counter('commoncrawl', 'sitemap URL invalid encoding', 1)
                    return
                if url is None:
                    url = record['WARC-Target-URI']
                    host = None
                    try:
                        host = urlparse(url).netloc.lower()
                    except Exception as url_parse_error:
                        try:
                            logging.warn('Invalid robots.txt URL: {} - {}'.format(url, url_parse_error))
                        except UnicodeEncodeError as unicode_error:
                            logging.warn('Invalid robots.txt URL - {} - {}'.format(url_parse_error, unicode_error))
                        self.increment_counter('commoncrawl', 'invalid robots.txt URL', 1)
                        return
                if not sitemap_url.startswith('http'):
                    sitemap_url = urljoin(url, sitemap_url)
                yield sitemap_url, [host]

    def reducer(self, key, values):
        '''Map sitemap URL to cross-submit hosts:
            sitemap_url => [host_1, ..., host_n]'''
        sitemap_uri = None
        try:
            sitemap_uri = urlparse(key)
        except Exception as url_parse_error:
            try:
                logging.warn('Invalid sitemap URL: {} - {}'.format(key, url_parse_error))
            except UnicodeEncodeError as unicode_error:
                logging.warn('Invalid sitemap URL - {} - {}'.format(url_parse_error, unicode_error))
            self.increment_counter('commoncrawl', 'invalid sitemap URL', 1)
            return
        sitemap_host = sitemap_uri.netloc.lower()
        cross_submit_hosts = set()
        for robots_txt_hosts in values:
            for robots_txt_host in robots_txt_hosts:
                if robots_txt_host != sitemap_host:
                    cross_submit_hosts.add(robots_txt_host)
        yield key, list(cross_submit_hosts)


if __name__ == '__main__':
  SitemapExtractor.run()
