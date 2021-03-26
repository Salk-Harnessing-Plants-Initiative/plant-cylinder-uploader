# plant-cylinder-uploader
Python-based Windows service to upload cylinder images to AWS S3.

* Files can be nested as deep as you want, and only files in folders which are leaf-level (do not have any children folders) will be processed. The folder must be named a valid `container_id` or `plant_id` (`plant_id` is PREFERRED); then it will be uploaded to the datasystem. 
* The expectation is that you have the `plant_id` as a barcode on the plant cylinder so that you can name the folder of images by scanning the barcode. 
* Images are also mostly-sorted in s3 by putting them in a subdirectory path organized as `.../{plant_or_container_id}/{date_of_image_timestamp}/yourfile-4f9zd13a42.jpg`. 
* The images are uploaded with a unique `upload_session` metadata value so that if you upload two folders that are named the same and contain images on the same day, you can still distinguish which folder it belongs to. (Query postgres to check that for multiple `upload_session`s in the same s3 subdirectory, or manually check in s3).

# Installation
Similar to https://github.com/Salk-Harnessing-Plants-Initiative/greenhouse-giraffe-uploader

* AWS IAM user who can upload to s3 and invoke the validation function
* `pip install -r requirements.txt`
* `cp example_config.json config.json`, fill it out
* validation test using `python main.py`
* run the python script as an automatic service using NSSM

# Release notes
* 1.1.0: Allows for folders which are named `{arbitrary something}.{plant_or_container_id}` so that users can encode a human-readable component to their cylinder naming/QR encoding during label printing
* 1.0.0: Validation-tested
