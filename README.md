# plant-cylinder-uploader
Python-based Windows service to upload cylinder images to AWS S3.

* Files can be nested as deep as you want, and only files in folders which are leaf-level (do not have any children folders) will be processed. The folder must be named a valid `container_id` or `plant_id` (`plant_id` is PREFERRED); then it will be uploaded to the datasystem. 
* The expectation is that you have the `plant_id` as a barcode on the plant cylinder so that you can name the folder of images by scanning the barcode. 
* Images are also mostly-sorted in s3 by putting them in a subdirectory path organized as `.../{plant_or_container_id}/{date_of_image_timestamp}/yourfile-4f9zd13a42.jpg`. 
* The images are uploaded with a unique `upload_session` metadata value so that if you upload two folders that are named the same and contain images on the same day, you can still distinguish which folder it belongs to. (Query postgres to check that for multiple `upload_session`s in the same s3 subdirectory, or manually check in s3).

# Installation
* `requirements.txt`
* `cp example_config.json config.json`
* AWS IAM user who can upload to s3 and invoke the validation function
* run the python script as an automatic service using NSSM