import main
import unittest
import os
import pathlib
import boto3
from datetime import datetime
from shutil import copyfile        

class Test(unittest.TestCase):
	def test_generate_plant_cylinder_s3_key(self):
		file_path = "/hello/there/penguin.jpg"
		s3_directory = "image/raw/cylinder/"
		plant_or_container_id = "kljasdfjkjhvkl"
		image_timestamp = datetime(2020, 5, 17)
		key = main.generate_plant_cylinder_s3_key(file_path, s3_directory, plant_or_container_id, image_timestamp)
		print(key)

	def test_get_leaf_directories(self):
		directory = "/Users/russelltran/Desktop/greenhouse_images"
		print("Leaf directories for {}:".format(directory))
		print(main.get_leaf_directories(directory))

	def test_get_files_for_leaf_directories(self):
		directory = "/Users/russelltran/Desktop/greenhouse_images"
		print("Leaf directory files for {}:".format(directory))
		print(main.get_files_for_leaf_directories(directory))

	def test_make_parallel_path(self):
		src_dir = "/hello/penguins"
		dst_dir = "/hello/apples"
		src_path = "/hello/penguins/elephants/icecream.jpg"
		output = main.make_parallel_path(src_dir, dst_dir, src_path, add_date_subdir=False)
		self.assertEqual(output, "/hello/apples/elephants/icecream.jpg")
		output = main.make_parallel_path(src_dir, dst_dir, src_path, add_date_subdir=True)
		today = datetime.now().strftime('%Y-%m-%d')
		self.assertEqual(output, "/hello/apples/{}/elephants/icecream.jpg".format(today))

	def test_delete_directory_if_empty_or_hidden(self):
		directory = "/tmp/something/bananas"
		os.makedirs(directory, exist_ok=True)
		main.delete_directory_if_empty_or_hidden(directory)
		self.assertFalse(os.path.exists(directory))
		self.assertTrue(os.path.exists("/tmp/something"))
		main.delete_directory_if_empty_or_hidden("/tmp/something")
		self.assertFalse(os.path.exists("/tmp/something"))

	def test_move(self):
		os.makedirs("/tmp/unprocessed/something/bananas/phone", exist_ok=False)
		os.makedirs("/tmp/done/something/bananas/phone", exist_ok=False)
		with open('/tmp/unprocessed/something/bananas/phone/myfile.txt', 'w') as fp: 
			pass
		with open('/tmp/unprocessed/something/bananas/phone/yourfile.txt', 'w') as fp: 
			pass
		assert(os.path.exists('/tmp/unprocessed/something/bananas/phone/myfile.txt'))
		assert(os.path.exists('/tmp/unprocessed/something/bananas/phone/yourfile.txt'))

		main.move(
			src_path='/tmp/unprocessed/something/bananas/phone/myfile.txt',
		 	dst_path='/tmp/done/something/bananas/phone/myfile.txt',
		 	src_root='/tmp/unprocessed')
		# All directories remain cause yourfile.txt is still present
		self.assertTrue(os.path.exists('/tmp/unprocessed/something/bananas/phone'))
		self.assertTrue(os.path.exists('/tmp/unprocessed/something/bananas'))
		self.assertTrue(os.path.exists('/tmp/unprocessed/something'))
		self.assertTrue(os.path.exists('/tmp/unprocessed'))

		main.move(
			src_path='/tmp/unprocessed/something/bananas/phone/yourfile.txt',
		 	dst_path='/tmp/done/something/bananas/phone/yourfile.txt',
		 	src_root='/tmp/unprocessed')
		# All children of unprocessed should be gone cause now empty
		self.assertFalse(os.path.exists('/tmp/unprocessed/something/bananas/phone'))
		self.assertFalse(os.path.exists('/tmp/unprocessed/something/bananas'))
		self.assertFalse(os.path.exists('/tmp/unprocessed/something'))
		self.assertTrue(os.path.exists('/tmp/unprocessed'))

	def test_qr_code_valid(self):
		client = boto3.client('lambda')
		result = main.qr_code_valid(client, qr_code="GI-wNbpbOPEMsMHRqKMsL", upload_device_id="testing")
		self.assertTrue(result)
		result = main.qr_code_valid(client, qr_code="snowmansnowman", upload_device_id="testing")
		self.assertFalse(result)

def something():
	src_path = '/Users/russelltran/Desktop/greenhouse_images/2021-02-04/unprocessed/what/why/shoebox.jpg' 
	dst_path = '/Users/russelltran/Desktop/greenhouse_images/2021-02-04/done/what/why/shoebox.jpg'
	src_root = '/Users/russelltran/Desktop/greenhouse_images/2021-02-04/unprocessed'
	main.move(src_path, dst_path, src_root)
	# works 16 Feb 2021

if __name__ == '__main__':
    unittest.main()

