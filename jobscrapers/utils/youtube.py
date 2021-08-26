#!/usr/bin/env python

#script for download video from youtube

from pytube import YouTube
import os
import logging
import requests
from bs4 import BeautifulSoup
from urlparse import urlparse
from pprint import pprint
import threading
from optparse import OptionParser

parser = OptionParser()
parser.add_option('--page-url', action='store', dest='page_url', type='string', help='URL of page when links to youtube video will be look')
parser.add_option('--download-dir', action='store', dest='download_dir', type='string', help='Directory for save movies')
parser.add_option('--video-url', action='store', dest='video_url', type='string', help='Direct link to youtube video')
parser.add_option('--filename', action='store', dest='filename', type='string', help='Filename of video')

(options, args) = parser.parse_args()

logger = logging.getLogger('YOUTUBE DOWNLODER')
ch2 = logging.StreamHandler()
ch2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch2.setFormatter(formatter)
logger.addHandler(ch2)
logger.setLevel(1)

threads = []

class DownloaderThread(threading.Thread):

    def __init__(self, url, directory, logger, filename=None):
        threading.Thread.__init__(self)
        self.url = url
        self.directory = directory
        self.logger = logger
        self.filename = filename

    def run(self):
        logger.info('downloading video from "%s"' % self.url)
        self.download_video(self.url, self.directory)
        logger.info('downloading video from "%s" done' % self.url)

    def download_video(self, url, directory):
        yt = YouTube(url)
        yt = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
        yt.download(output_path=directory, filename=self.filename)

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        logger.error('Error: Creating directory. ' + directory)


def get_youtube_links(page_url):
    res = requests.get(page_url)
    if res.status_code == 200:
        html = res.text
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all("a", href=True)
        return map(lambda j: j['href'], filter(lambda i: 'youtu.be' in str(i['href']), links))


def main():
    download_dir = options.download_dir
    page_url = options.page_url
    video_url = options.video_url
    filename = options.filename
    if download_dir is not None:
        createFolder(download_dir)
    else:
        download_dir = '.'
    logger.info('Download dir: "%s"' % download_dir)
    if page_url is not None:
        logger.info('Page URL: "%s"' % page_url)
        urls = get_youtube_links(page_url)
        logger.info('Video links: %s' % str(urls))
        for url in urls:
            threads.append(DownloaderThread(url, download_dir, logger))
            threads[len(threads) - 1].start()
    elif video_url is not None:
        logger.info('Video URL: "%s"' % video_url)
        threads.append(DownloaderThread(video_url, download_dir, logger, filename))
        threads[len(threads) - 1].start()
    else:
        logger.error('At least one from options: "page-url" or "video-url" is required!')


if __name__ == '__main__':
    main()





