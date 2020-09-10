from google.cloud import storage

class Bucket:
    def __init__(self, bucket_name):
        self.__bucket = storage.Client().get_bucket(bucket_name)

    def download_blob(self, source):
        """Download a file from the bucket."""
        blob = self.__bucket.blob(source)
        with open(source, "wb") as file_obj:
            blob.download_to_file(file_obj)

    def upload_blob(self, source_file, destination_file):
        """Uploads a file to the bucket."""
        blob = self.__bucket.blob(destination_file)
        blob.upload_from_filename(source_file)

