from setuptools import setup, find_packages

setup(
    name         = 'gcdapec',
    version      = '1.0',
    packages     = find_packages(),
    entry_points = {'scrapy': ['settings = gcd_apec_feedgenerator.settings']},
    package_data = {'gcd_apec_feedgenerator': ['res/*.json', 'res/*.csv', 'res/*.html']},
    install_requires = [
        'psycopg2',
        'google-api-python-client',
        'urllib3',
        'zeep',  # SOAP Client for APEC's API
        'certify',
        'tidylib'
    ]
)
