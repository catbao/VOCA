const { Client } = require('pg');
const fs = require('fs');

const dbConfig = {       //update to your own
  user: 'postgres',        
  host: 'localhost',       
  database: 'postgres',  
  password: '***',     
  port: 5432,              
};                

const sqlFilePath = '***/nycdata.sql'; //update to your own

async function importSqlFile() {
  const client = new Client(dbConfig);
  try {
    await client.connect();
    console.log('Connected to PostgreSQL database.');

    const sql = fs.readFileSync(sqlFilePath, 'utf8');
    console.log('SQL file read successfully.');

    await client.query(sql);
    console.log('SQL file imported successfully.');
  } catch (err) {
    console.error('Error importing SQL file:', err);
  } finally {
    await client.end();
  }
}

importSqlFile();