import express from "express";
import dotenv from "dotenv";
import mysql from "mysql2/promise";
import titleRoutes from "./routes/titles.js";

dotenv.config();

const app = express();
app.use(express.json());
app.set("view engine", "ejs");
app.set("views", "./views");

const db1 = mysql.createPool({
  host: process.env.DB1_HOST || "127.0.0.1",
  port: Number(process.env.DB1_PORT), 
  user: process.env.DB1_USER,
  password: process.env.DB1_PASS,
  database: process.env.DB1_NAME,
});

const db2 = mysql.createPool({
  host: process.env.DB2_HOST || "127.0.0.1",
  port: Number(process.env.DB2_PORT),
  user: process.env.DB2_USER,
  password: process.env.DB2_PASS,
  database: process.env.DB2_NAME,
});

const db3 = mysql.createPool({
  host: process.env.DB3_HOST || "127.0.0.1",
  port: Number(process.env.DB3_PORT), 
  user: process.env.DB3_USER,
  password: process.env.DB3_PASS,
  database: process.env.DB3_NAME,
});

// Routes
app.get("/", (req, res) => res.render("index"));
app.use("/api/titles", titleRoutes(db1, db2, db3));

// Start server
app.listen(process.env.PORT, () => {
  console.log(`Server running on port ${process.env.PORT}`);
});
