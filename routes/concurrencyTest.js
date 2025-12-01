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

  router.post("/runAllCases", async (req, res) => {
    const isolation = req.body.isolation || "READ COMMITTED";
    const scenario = req.body.scenario || "read-read";

    const { readNode2, writeNode2, readNode3, writeNode3 } = req.body.sql || {};

    const R2 = readNode2 || DEFAULTS.readNode2;
    const W2 = writeNode2 || DEFAULTS.writeNode2;
    const R3 = readNode3 || DEFAULTS.readNode3;
    const W3 = writeNode3 || DEFAULTS.writeNode3;

    const results = {};

    try {
      switch (scenario) {
        case "read-read": {
          // Define operations as data objects
          const readOps2 = [
            { sql: R2, type: 'READ', logMsg: 'Reading Node2' }
          ];
          const readOps3 = [
            { sql: R3, type: 'READ', logMsg: 'Reading Node3' }
          ];

          const [r21, r22, r31, r32] = await Promise.all([
            runTransaction(db2, isolation, readOps2, "Node2-r1", replicator),
            runTransaction(db2, isolation, readOps2, "Node2-r2", replicator),
            runTransaction(db3, isolation, readOps3, "Node3-r1", replicator),
            runTransaction(db3, isolation, readOps3, "Node3-r2", replicator),
          ]);

          results["read-read"] = [r21, r22, r31, r32].map(normalizeLogs);
          break;
        }

        case "read-write": {
          const readOps2 = [
            { sql: R2, type: 'READ', logMsg: 'Reading Node2' }
          ];
          const writeOps2 = [
            { sql: W2, type: 'WRITE', logMsg: 'Writing Node2' }
          ];

          const readOps3 = [
            { sql: R3, type: 'READ', logMsg: 'Reading Node3' }
          ];
          const writeOps3 = [
            { sql: W3, type: 'WRITE', logMsg: 'Writing Node3' }
          ];

          const [r2, w2, r3, w3] = await Promise.all([
            runTransaction(db2, isolation, readOps2, "Node2-reader", replicator),
            runTransaction(db2, isolation, writeOps2, "Node2-writer", replicator),
            runTransaction(db3, isolation, readOps3, "Node3-reader", replicator),
            runTransaction(db3, isolation, writeOps3, "Node3-writer", replicator),
          ]);

          results["read-write"] = [r2, w2, r3, w3].map(normalizeLogs);
          break;
        }

        case "write-write": {
          const writeOps2 = [
            { sql: W2, type: 'WRITE', logMsg: 'Writing Node2' }
          ];
          const writeOps3 = [
            { sql: W3, type: 'WRITE', logMsg: 'Writing Node3' }
          ];

          const [w21, w22, w31, w32] = await Promise.all([
            runTransaction(db2, isolation, writeOps2, "Node2-w1", replicator),
            runTransaction(db2, isolation, writeOps2, "Node2-w2", replicator),
            runTransaction(db3, isolation, writeOps3, "Node3-w1", replicator),
            runTransaction(db3, isolation, writeOps3, "Node3-w2", replicator),
          ]);

          results["write-write"] = [w21, w22, w31, w32].map(normalizeLogs);
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