The Following flow represents the folder structure of the project 
				SMD2022_Project
				|   
				+---code
				|       config.txt
				|       dataVault.sql
				|       InformationDelivery.py
				|       InformationMart.sql
				|       staging.py
				|       
				+---doc
				|       ER Diagram.pdf
				|       README.txt
				|		...
				|       
				+---report
				|       report.tex
				|		...
				|       
				\---results
						GUI Dashboard.pdf
						metadataExtraction.txt
						...


1. Fist step is to install the following python libraries from the pip command

pip install dash==2.7.0
pip install numpy==1.23.1
pip install pandas==1.4.3
pip install plotly==5.9.0
pip install psycopg2==2.9.5
pip install psycopg2_binary==2.9.5
pip install python_dateutil==2.8.2
pip install dash-core-components
pip install dash-html-components
pip install dash-renderer

2. Next step in execution involves creating an Enterprise Data Warehouse database.
a. The table creation script in .sql to create is found in code folder with dataVault.sql filename

b. Navigate to dataVault.sql file and copy the complete path from Local Disk
	Example: 'F:\University of Birmingham\Storing and Managing Data\Semester Project\SMD2022_Project\code\dataVault.sql'

c. Open the postres shell in command prompt and enter the credentials.

d. Enter the following command to execute the sql script for dataVault.sql, the directory would be the complete path of datavault.sql file
	\i '<file path for dataVault.sql>'
	Example : \i 'F:\\University of Birmingham\\Storing and Managing Data\\Semester Project\\SMD2022_Project\\code\\dataVault.sql'

Note: If spaces are present in parent directories, use escape character '\' to avoide errors

Executing the script would create a database called smdvault, connect to the database and create 27 Hub, Satellite and Link tables.

3. Next step is to execute staging layer that takes data for pre-autism and VMDataset
a. Navigate to code directory

b. Open config.txt in a text editor

c. Enter the VM Dataset, Pre Autism Dataset paths and postgres connection parameters in the text file without quotes in the following format.
	VMDataFolder,<Folder path to VM Dataset>
	PreAutismDataFolder,<Folder path to Pre Autism Dataset>
	USER,<username>
	PASSWORD,<password>
	HOST,<host server path>
	PORT,<port>
	DATABASE,<database name>
	
	Example:
	VMDataFolder,F:\University of Birmingham\Storing and Managing Data\Semester Project\data\VMData
	PreAutismDataFolder,F:\University of Birmingham\Storing and Managing Data\Semester Project\data\PreAutismData
	USER,postgres
	PASSWORD,postgrespassword
	HOST,localhost
	PORT,5432
	DATABASE,smdvault

Note : 	The default username is 'smd', password is 'smd2022' and connected to port 5432 for postgres credentials
		If any changes in the credentials are required to be made, navigate to config.txt file in code folder and change the respective parameters

d. Execute the python script using the following command in shell from the code folder
	python staging.py
	

e. Python script would display the folder path for both the datasets and postgres credentials, then continue the staging process.Execution time typically would take around 3-4 minutes and the following message is displayed when data gets inserted into Enterprise Data Warehouse(EDW)

Inserted data successfully in PostgreSQL
PostgreSQL connection is closed


4. Next step is to Build a Data mart for EDW, this step is executed in postgres shell.
a. Navigate to code folder 

b. Script to execute Data Mart is available in InformationMart.sql file

c. Copy the complete path to InformationMart.sql file from windows navigator
	Example : 'F:\\University of Birmingham\\Storing and Managing Data\\Semester Project\\SMD2022_Project\\code\\InformationMart.sql'
	
d. Open the postres shell in command prompt and enter the credentials.


e. Enter the following command to execute the sql script for InformationMart.sql, the directory would be the complete path of InformationMart.sql file
	\i '<file path for InformationMart.sql>'
	Example : \i 'F:\\University of Birmingham\\Storing and Managing Data\\Semester Project\\SMD2022_Project\\code\\InformationMart.sql'

Note: If spaces are present in parent directories, use escape character '\' to avoide errors

Facts and Dimensional views for data marts are created after the script is run, the script connects to smdvault database and executes CREATE VIEW DDL commands

5. Final step is to generate a GUI for data querying
a. Navigate to code folder

b. Script to generate GUI ia available in InformationDelivery.py file

c. Execute the following command to run python script from shell of the folder
	python InformationDelivery.py

Note : 	The default username is 'smd', password is 'smd2022' and connected to port 5432 for postgres credentials
		If any changes in the credentials are required to be made, navigate to config.txt file in code folder and change the respective parameters

d. plotly will provide a local URL to open in browser when the script runs
	Example : Dash is running on http://127.0.0.1:4050/

e. Copy the URL and paste it in a browser to view the GUI