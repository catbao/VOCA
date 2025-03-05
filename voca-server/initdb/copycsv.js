const { Pool } = require("pg");
const fs = require("fs");
const copyFrom = require("pg-copy-streams").from;

const dbConfig = {       //update to your own
    user: 'postgres',        
    host: 'localhost',       
    database: 'postgres',  
    password: '******',     
    port: 5432,          
  };                
  
const CSV_FILE_PATH = "******/nycdata.csv"; // update to your own
const TABLE_NAME = "nycdata";

const pool = new Pool(dbConfig);
async function createTableIfNotExists() {
    const client = await pool.connect();
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
          t INT PRIMARY KEY,
          v1 INT,
          v2 INT,
          v3 INT,
          v4 INT,
          v5 INT,
          v6 INT,
          v7 INT,
          v9 INT,
          v10 INT,
          v11 INT
      );
    `;
    await client.query(createTableQuery);
    console.log(`Table ${TABLE_NAME} exists.`);
}

async function importCsvWithCopy() {
  const client = await pool.connect();
  try {
    await createTableIfNotExists();
    await client.query("BEGIN");

    const stream = fs.createReadStream(CSV_FILE_PATH);
    const copyQuery = `
      COPY ${TABLE_NAME} FROM STDIN WITH (FORMAT csv, HEADER true)
    `;
    const copyStream = client.query(copyFrom(copyQuery));

    stream.pipe(copyStream);

    stream.on("end", async () => {
      await client.query("COMMIT");
      console.log("CSV FILE IMPORTED");
      client.release();
      pool.end();
    });

    stream.on("error", async (err) => {
      await client.query("ROLLBACK");
      console.error("Fail:", err);
      client.release();
      pool.end();
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("CSV FILE IMPORTED:", err);
    client.release();
    pool.end();
  }
}

importCsvWithCopy();