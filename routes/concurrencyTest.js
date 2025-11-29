import express from "express";
import { runTransaction } from "../transactionRunner.js";

export default function concurrencyTestRoutes(db1, db2, db3, replicator) {
  const router = express.Router();

  const DEFAULTS = {
    readNode2: "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0000002'",
    writeNode2:
      "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0000002'",

    readNode3: "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0038698'",
    writeNode3:
      "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0038698'",
  };

  const normalizeLogs = (txn) => {
    txn.logs = Array.isArray(txn.logs) ? txn.logs : [txn.logs || ""];
    return txn;
  };

  const replicateWrite = async (txn, node, sql) => {
    if (txn.success) {
      await replicator.runReplication(node, sql);
      txn.logs.push(`[WRITE] ${node} replicated`);
    }
    return txn;
  };

  router.post("/runAllCases", async (req, res) => {
    const isolation = req.body.isolation || "READ COMMITTED";
    const scenario = req.body.scenario || "read-read";

    const {
      readNode2,
      writeNode2,
      readNode3,
      writeNode3,
    } = req.body.sql || {};

    const R2 = readNode2 || DEFAULTS.readNode2;
    const W2 = writeNode2 || DEFAULTS.writeNode2;
    const R3 = readNode3 || DEFAULTS.readNode3;
    const W3 = writeNode3 || DEFAULTS.writeNode3;

    const results = {};

    try {
      switch (scenario) {
        case "read-read": {
          const readOps2 = [
            async (c) => {
              const [rows] = await c.query(R2);
              return `[READ Node2] rows=${JSON.stringify(rows)}`;
            },
          ];
          const readOps3 = [
            async (c) => {
              const [rows] = await c.query(R3);
              return `[READ Node3] rows=${JSON.stringify(rows)}`;
            },
          ];

          const [r21, r22, r31, r32] = await Promise.all([
            runTransaction(db2, isolation, readOps2, "Node2-r1"),
            runTransaction(db2, isolation, readOps2, "Node2-r2"),
            runTransaction(db3, isolation, readOps3, "Node3-r1"),
            runTransaction(db3, isolation, readOps3, "Node3-r2"),
          ]);

          results["read-read"] = [r21, r22, r31, r32].map(normalizeLogs);
          break;
        }
        case "read-write": {
          const readOps2 = [
            async (c) => {
              const [rows] = await c.query(R2);
              return `[READ Node2] rows=${JSON.stringify(rows)}`;
            },
          ];

          const writeOps2 = [
            async (c) => {
              await c.query(W2);
              return `[WRITE Node2]`;
            },
          ];

          const readOps3 = [
            async (c) => {
              const [rows] = await c.query(R3);
              return `[READ Node3] rows=${JSON.stringify(rows)}`;
            },
          ];

          const writeOps3 = [
            async (c) => {
              await c.query(W3);
              return `[WRITE Node3]`;
            },
          ];

          let [r2, w2, r3, w3] = await Promise.all([
            runTransaction(db2, isolation, readOps2, "Node2-reader"),
            runTransaction(db2, isolation, writeOps2, "Node2-writer"),
            runTransaction(db3, isolation, readOps3, "Node3-reader"),
            runTransaction(db3, isolation, writeOps3, "Node3-writer"),
          ]);

          [r2, w2, r3, w3].forEach(normalizeLogs);

          w2 = await replicateWrite(w2, "node2", W2);
          w3 = await replicateWrite(w3, "node3", W3);

          results["read-write"] = [r2, w2, r3, w3];
          break;
        }
        case "write-write": {
          const writeOps2 = [
            async (c) => {
              await c.query(W2);
              return `[WRITE Node2]`;
            },
          ];

          const writeOps3 = [
            async (c) => {
              await c.query(W3);
              return `[WRITE Node3]`;
            },
          ];

          let [w21, w22, w31, w32] = await Promise.all([
            runTransaction(db2, isolation, writeOps2, "Node2-w1"),
            runTransaction(db2, isolation, writeOps2, "Node2-w2"),
            runTransaction(db3, isolation, writeOps3, "Node3-w1"),
            runTransaction(db3, isolation, writeOps3, "Node3-w2"),
          ]);

          [w21, w22, w31, w32].forEach(normalizeLogs);

          w21 = await replicateWrite(w21, "node2", W2);
          w22 = await replicateWrite(w22, "node2", W2);
          w31 = await replicateWrite(w31, "node3", W3);
          w32 = await replicateWrite(w32, "node3", W3);

          results["write-write"] = [w21, w22, w31, w32];
          break;
        }
        default:
          return res.status(400).json({ error: "Invalid scenario" });
      }

      res.json({ isolation, scenario, results });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
