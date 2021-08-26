import threading

class myThread(threading.Thread):

    def __init__(self, file, azs, logger):
        threading.Thread.__init__(self)
        self.file = file
        self.azs = azs
        self.logger = logger

    def run(self):
        self.logger.info('pushing file "%s" to AzureStorage...' % self.file)
        azure_url = self.azs.push_file(self.file)
        self.logger.info('...done, URL: %s' % azure_url)
