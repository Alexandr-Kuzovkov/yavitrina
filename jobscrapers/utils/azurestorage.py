import os, sys, logging
from azure.storage.blob import BlockBlobService, PublicAccess



class AzureStorage:
    account_name = None
    account_key = None
    container_name = '$root'
    enabled = False
    block_blob_service = None

    def __init__(self, account_name, account_key, container_name='$root'):
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.logger = logging.getLogger('AzureStorage')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def push_file(self, full_path_to_file):
        try:
            self.block_blob_service = BlockBlobService(account_name=self.account_name, account_key=self.account_key)
            container_exists = False
            container_list = self.block_blob_service.list_containers()
            for container in container_list:
                if container.name == self.container_name:
                    container_exists = True
                    break
            if not container_exists:
                self.block_blob_service.create_container(self.container_name)
                # Set the permission so the blobs are public.
                self.block_blob_service.set_container_acl(self.container_name, public_access=PublicAccess.Container)
        except Exception, ex:
            self.logger.error('ERROR:')
            self.logger.error(ex)
        else:
            local_file_name = os.path.basename(full_path_to_file)
            self.block_blob_service.create_blob_from_path(self.container_name, local_file_name, full_path_to_file)
            blob_list = self.block_blob_service.list_blobs(self.container_name)
            for blob in blob_list:
                if blob.name == local_file_name:
                    return ('https://%s.blob.core.windows.net/%s/%s' % (self.account_name, self.container_name, local_file_name)).replace('$root/', '')
            return False
