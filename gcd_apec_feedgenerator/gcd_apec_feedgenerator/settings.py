# -*- coding: utf-8 -*-

# Scrapy settings for gcd_apec_feedgenerator project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'gcd_apec_feedgenerator'

SPIDER_MODULES = ['gcd_apec_feedgenerator.spiders']
NEWSPIDER_MODULE = 'gcd_apec_feedgenerator.spiders'

SLACK_CHANNEL = '#incident'
SLACK_WEB_HOOK_POINT = 'https://hooks.slack.com/services/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'
NOTIFY_EMAILS = ['a.kuzovkov@myxtramile.com', 'a.egoshin@myxtramile.com']
#NOTIFY_EMAILS = ['a.kuzovkov@myxtramile.com', 'elodie_apec_email@kuzovkov12.ru']
ELODIE_EMAIL = ['e.vernette@myxtramile.com']
EMAIL_FROM = 'contact@myxtramile.com'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
SMTP_PASSWORD = 'Motown10'

JOBS_PER_ONCE = 500
# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'gcd_apec_feedgenerator (+http://www.yourdomain.com)'

APEC_ENV = 'PROD'  #DEV
#APEC_ENV = 'DEV'  #DEV
APEC_ACCOUNT = {
    'wsdl': 'https://adepsides.apec.fr/v5/positions?wsdl',
    'recruiter_id': '100061739',
    'atsId': 50,
    'salt': '17192c4f9df8648798b4cd57538207edc3908d83deb3c9d3d361db9cace767e07e93d42902e5ff141f28f121e9c61d006015b44244d6160220e1893a689acd95',
    'password': 'Motown1234+=!!',
    'numeroDossier': '100074336W'
}

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

LOG_LEVEL = 'INFO'
#LOG_LEVEL = 'DEBUG'

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'gcd_apec_feedgenerator.middlewares.GcdApecFeedgeneratorSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'gcd_apec_feedgenerator.middlewares.GcdApecFeedgeneratorDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'gcd_apec_feedgenerator.pipelines.GcdApecFeedgeneratorPipeline': 300,
#}

ITEM_PIPELINES = {
    'gcd_apec_feedgenerator.pipelines.GcdApecExporterPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
