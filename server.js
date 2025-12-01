import express from "express";
import dotenv from "dotenv";
import mysql from "mysql2/promise";
import cors from "cors";
import setupReplicator from "./replicator.js";
import concurrencyTestRoutes from "./routes/concurrencyTest.js";
import distributedConcurrencyRoutes from "./routes/distributedConcurrencyRoutes.js";
import crashRecoveryRoutes from "./routes/crashRecoveryRoutes.js";
import webApplicationRoutes from "./routes/webApplicationRoutes.js";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());



const db1 = mysql.createPool({
  host: process.env.DB1_HOST,
  port: Number(process.env.DB1_PORT),
  user: process.env.DB1_USER,
  password: process.env.DB1_PASS,
  database: process.env.DB1_NAME,
});

const db2 = mysql.createPool({
  host: process.env.DB2_HOST,
  port: Number(process.env.DB2_PORT),
  user: process.env.DB2_USER,
  password: process.env.DB2_PASS,
  database: process.env.DB2_NAME,
});

const db3 = mysql.createPool({
  host: process.env.DB3_HOST,
  port: Number(process.env.DB3_PORT),
  user: process.env.DB3_USER,
  password: process.env.DB3_PASS,
  database: process.env.DB3_NAME,
});

const replicator = setupReplicator(app, db1, db2, db3);

app.get("/", (req, res) => res.json({ ok: true }));
app.use("/api/concurrencyTest", concurrencyTestRoutes(db1, db2, db3, replicator));
app.use("/api/distributedConcurrency", distributedConcurrencyRoutes(db2, db3, replicator));
app.use("/api/crashRecovery", crashRecoveryRoutes(db1, db2, db3, replicator));
app.use("/api/webApp", webApplicationRoutes(db1, db2, db3, replicator));



app.listen(process.env.PORT, () => {
  console.log(`Backend API running on port ${process.env.PORT}`);
});
