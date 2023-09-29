#### This repository contains the 4 main scripts of the project, and to complete a simple data center, I took the following steps:
##### 1. Install the MySQL library on the server and build a table for storing K-lines;
##### 2. Use a python program to obtain the 1-minute K-line data of several currencies and save it to the database;
##### 3. Use a python program to regularly check whether the k-line in the database is missing. If there is any missing, obtain the missing data from the exchange through an http request and save it to the database to ensure that the k-line data in the database is complete;
##### 4. Obtain data from MySQL regularly, and ensure that the latest K line is stored in MySQL before obtaining the data.
###### [1_binance_data_to_mysql.py：Gets data from binance via websocket and store to MySQL](https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL/blob/master/1_binance_data_to_mysql.py)
###### [2_check_data.py：1. Reads data from MySQL ;2. Checks whether the removed K-line is missing or not. ;3. If there is anything missing, gets the latest K-line through http](https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL/blob/master/2_check_data.py)
###### [3_get_data_and_sample.py：Gets data from mysql and resample to desired time interval](https://github.com/JiahaoLi-creator/Build-A-Cryptocurrency-Data-Center-with-MySQL/blob/master/3_get_data_and_sample.py)
