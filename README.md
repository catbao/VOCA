# VOCA System

## Project Setup
You can run voca-client and voca-server on your computer at the same time. 


### VOCA Client
```
1. cd voca-client
2. npm install
3. npm run serve
```

### VOCA Server
```
1. cd voca-server
2. npm install
3. configure your db information in the initdb/dbconfig.json file
4. npm run start
```

### Run
After starting the client and server, you can visit http://localhost:8081 in the browser.

### Enviroment
```
nodejs: v16.20.2
npm: 8.19.4
postgres: 14.12
```

### Database Initialization

We have backed up a database file named `nycdata`, which you can download from [Google Drive](https://drive.google.com/file/d/1Ab4erGIKN9NFXi8WhFHGy6XoaTH4kNKu/view?usp=sharing). We recommend using PostgreSQL for importing the database. 

#### Step 1: Import the Database
1. Download and install PostgreSQL if you haven’t already. 
```
Windows: Visit the [official website](https://www.postgresql.org/download/) and select the version 14 to download.
MacOS: brew install postgresql@14
```

2. You can import it into your local database using graphical tools such as Navicat or DBeaver:
```
1. Create a postgres connection
2. Create a database
3. Import the csv file
```

#### Step 2: Encode the Data
Make sure you have installed nodejs. If not, Visit the [official website](https://nodejs.org/en/download) and select the version 16 to download.

1. Navigate to the /voca-server/initdb directory:
```
cd /voca-server/initdb  
```

2. Run the encode_bigarray_ave script to encode the data, this will generate the corresponding coefficient file in the database:
```
nodejs encode_bigarray_ave.js 
```

You can also complete this step with your own data. If you use your own data, make sure it is formatted similarly to the nycdata file (your data file can have a single t column, and multiple v columns).




