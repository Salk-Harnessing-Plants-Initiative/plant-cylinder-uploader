import main
import unittest
import os
import pathlib
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

if __name__ == '__main__':
    unittest.main()

