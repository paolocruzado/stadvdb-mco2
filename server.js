import express from "express";
import mysql from "mysql2/promise";
import dotenv from "dotenv";
import { createTunnel } from "tunnel-ssh";

dotenv.config();
const app = express();
app.use(express.json());

async function openNode(node) {
  try {
    await createTunnel(
      { autoClose: true },                        
      { host: "127.0.0.1", port: node.localPort }, 
      node.ssh,                                   
      { srcAddr: "127.0.0.1", srcPort: node.localPort, dstAddr: "127.0.0.1", dstPort: 3306 }
    );

    console.log(`Tunnel open for ${node.name} on localhost:${node.localPort}`);

    const db = await mysql.createPool(node.db);
    const [rows] = await db.query("SELECT COUNT(*) AS count FROM title_basics");
    console.log(`Rows in ${node.name}:`, rows[0].count);

    return db;
  } catch (err) {
    console.error(`Error for ${node.name}:`, err);
    throw err;
  }
}

let db1, db2, db3;

async function initApp() {
  const nodes = [
    {
      name: "Node1",
      ssh: {
        host: process.env.SSH1_HOST,
        port: Number(process.env.SSH1_PORT),
        username: process.env.SSH1_USER,
        password: process.env.SSH1_PASS,
      },
      localPort: Number(process.env.DB1_PORT),
      db: {
        host: "127.0.0.1",
        port: Number(process.env.DB1_PORT),
        user: process.env.DB1_USER,
        password: process.env.DB1_PASS,
        database: process.env.DB1_NAME,
      },
    },
    {
      name: "Node2",
      ssh: {
        host: process.env.SSH2_HOST,
        port: Number(process.env.SSH2_PORT),
        username: process.env.SSH2_USER,
        password: process.env.SSH2_PASS,
      },
      localPort: Number(process.env.DB2_PORT),
      db: {
        host: "127.0.0.1",
        port: Number(process.env.DB2_PORT),
        user: process.env.DB2_USER,
        password: process.env.DB2_PASS,
        database: process.env.DB2_NAME,
      },
    },
    {
      name: "Node3",
      ssh: {
        host: process.env.SSH3_HOST,
        port: Number(process.env.SSH3_PORT),
        username: process.env.SSH3_USER,
        password: process.env.SSH3_PASS,
      },
      localPort: Number(process.env.DB3_PORT),
      db: {
        host: "127.0.0.1",
        port: Number(process.env.DB3_PORT),
        user: process.env.DB3_USER,
        password: process.env.DB3_PASS,
        database: process.env.DB3_NAME,
      },
    },
  ];

  db1 = await openNode(nodes[0]);
  db2 = await openNode(nodes[1]);
  db3 = await openNode(nodes[2]);

  console.log("All nodes initialized successfully.");
}

app.get("/ping", async (req, res) => {
  try {
    const [rows1] = await db1.query("SELECT COUNT(*) AS count FROM title_basics");
    const [rows2] = await db2.query("SELECT COUNT(*) AS count FROM title_basics");
    const [rows3] = await db3.query("SELECT COUNT(*) AS count FROM title_basics");

    res.json({
      node1_rows: rows1[0].count,
      node2_rows: rows2[0].count,
      node3_rows: rows3[0].count,
    });
  } catch (err) {
    console.error("Ping failed:", err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(process.env.PORT, async () => {
  try {
    await initApp();
    console.log(`Server running on port ${process.env.PORT}`);
  } catch (err) {
    console.error("Failed to initialize server:", err);
  }
});
