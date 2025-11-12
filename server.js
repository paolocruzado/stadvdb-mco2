import express from "express";
import dotenv from "dotenv";
import { createTunnel } from "tunnel-ssh";
import titleRoutes from "./routes/titles.js";
import mysql from "mysql2/promise";

dotenv.config();

const app = express();
app.use(express.json());
app.set("view engine", "ejs");
app.set("views", "./views");

let db1, db2, db3;

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
    return db;
  } catch (err) {
    console.error(`Error for ${node.name}:`, err);
    throw err;
  }
}

async function initNodes() {
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

  [db1, db2, db3] = await Promise.all(nodes.map(openNode));
  console.log("All nodes initialized successfully.");
}

async function startServer() {
  try {
    await initNodes();

    app.get("/", (req, res) => res.render("index"));

    app.use("/api/titles", titleRoutes(db1, db2, db3));

    app.listen(process.env.PORT, () => {
      console.log(`Server running on port ${process.env.PORT}`);
    });
  } catch (err) {
    console.error("Failed to start server:", err);
  }
}

startServer();
