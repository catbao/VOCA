const { Client } = require('pg');
const fs = require('fs');
const csv = require("csv-parser");

const dbConfig = {       //update to your own
  user: 'postgres',        
  host: 'localhost',       
  database: 'postgres1',  
  password: '000927',     
  port: 5432,  
  // idleTimeoutMillis: 0,
  connectionTimeoutMillis: 0        
};                

const CSV_FILE_PATH = "/Users/bao/Desktop/nycdata.csv"; // update to your own

const TABLE_NAME = "nycdata"; 
const COLUMNS = ["t", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v9", "v10", "v11"]; 

// 批量插入的大小
const BATCH_SIZE = 10000;
// 连接到数据库
const client = new Client(dbConfig);

async function createTableIfNotExists() {
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

async function importCsvToDb() {
  try {
    // 连接到数据库
    await client.connect();
    console.log("数据库连接成功！");

    // 创建表（如果不存在）
    await createTableIfNotExists();

    // 读取 CSV 文件并批量插入数据
    let batch = [];
    let rowCount = 0;

    fs.createReadStream(CSV_FILE_PATH)
      .pipe(csv())
      .on("data", (row) => {
        // 将行数据添加到批次中
        batch.push(row);
        rowCount++;

        // 如果批次大小达到 BATCH_SIZE，执行批量插入
        if (batch.length >= BATCH_SIZE) {
          const insertQuery = `
            INSERT INTO ${TABLE_NAME} (${COLUMNS.join(", ")})
            VALUES ${batch
              .map(
                (_, i) =>
                  `(${COLUMNS.map((_, j) => `$${i * COLUMNS.length + j + 1}`).join(", ")})`
              )
              .join(", ")}
          `;
          const values = batch.flatMap((row) => COLUMNS.map((col) => row[col]));

          // 执行批量插入
          client.query(insertQuery, values, (err) => {
            if (err) {
              console.error("批量插入失败:", err);
            } else {
              // console.log(`已插入 ${batch.length} 行数据。`);
            }
          });

          // 清空批次
          batch = [];
        }
      })
      .on("end", () => {
        // 插入剩余的批次数据
        if (batch.length > 0) {
          const insertQuery = `
            INSERT INTO ${TABLE_NAME} (${COLUMNS.join(", ")})
            VALUES ${batch
              .map(
                (_, i) =>
                  `(${COLUMNS.map((_, j) => `$${i * COLUMNS.length + j + 1}`).join(", ")})`
              )
              .join(", ")}
          `;
          const values = batch.flatMap((row) => COLUMNS.map((col) => row[col]));

          // 执行批量插入
          client.query(insertQuery, values, (err) => {
            if (err) {
              console.error("批量插入失败:", err);
            } else {
              console.log(`已插入 ${batch.length} 行数据。`);
            }
          });
        }

        console.log(`CSV 文件导入完成！共插入 ${rowCount} 行数据。`);
        client.end();
      });
  } catch (err) {
    console.error("CSV 文件导入失败:", err);
    client.end();
  }
}

// 运行导入函数
importCsvToDb();