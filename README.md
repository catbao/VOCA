# VOCA System

### VOCA Server
```
1. cd voca-server
2. npm install
3. configure your db information in the initdb/dbconfig.json file
4. npm run start
```

### Enviroment
```
nodejs: v16.20.2
npm: 8.19.4
postgres: 14.12
```

### Database
We backed up a database, we named it nycdata, you can import it in your computer's database. You can find it in '/voca-server/initdb' directory. We use postgresql database, we also recommend you to use it.

Then, you can enter the '/voca-server/initdb' directory, and run the encode_bigarray_ave file. You can see that there are 3 files in your database at this time, namely nycdata, nycdata_om3, tablenum.

Of course you can complete this series of steps with your own data. Then, you can visit http://localhost:8081 in the browser.




