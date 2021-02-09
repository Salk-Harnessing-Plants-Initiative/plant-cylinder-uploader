import subprocess

PATH_TO_PYTHON = 'C:\\Users\\Greenhouse\\AppData\\Local\\Programs\\Python\\Python36\\python.exe'
PATH_TO_MAIN = "C:\\Users\\Greenhouse\\Documents\\greenhouse-giraffe-uploader\\main.py"

with open('output.log', 'a') as f:
	try:
	    p = subprocess.run([PATH_TO_PYTHON, PATH_TO_MAIN], stdout=f, stderr=subprocess.STDOUT)
	except Exception as e:
	    f.write("ERROR: " + repr(e) + "\n")