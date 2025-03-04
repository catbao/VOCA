# VOCA-Server Setup

<!-- ## Run with Docker
https://docs.google.com/document/d/17YTot0R3aGX7sqO5I_8jTBWlBwlVFlCuSLeH9rYK01w/edit?usp=sharing
```
docker-compose build
docker-compose up -d
visit http://localhost:3000 in the browser

```
```
tips：Before you use OM3, please make sure the data has been imported into the database.You can download the demo data from this link https://jsiiv0axoo.feishu.cn/file/JBdKbxWYQo49gFxj8YUcgkErnmg?from=from_copylink -->
<!-- ```
## Run run directly -->


1、Configure your db information in the initdb/dbconfig.json file：
   ```
   For example:
   "hostname":"127.0.0.1",
   "db":"postgres1",         //update to your dbname
   "username":"postgres",    //update to your username
   "password":"******",      //update to your password
```
2、npm run start

```
tips：Before you use VOCA, please make sure the data has been imported into the database and you have encoded the data.
```
