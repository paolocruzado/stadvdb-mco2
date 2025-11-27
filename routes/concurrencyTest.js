import express from "express";
import { runTransaction } from "../transactionRunner.js";

export default function concurrencyTestRoutes(db1, db2, db3, replicator) {
  const router = express.Router();

  // Helper: normalize logs
  const normalizeLogs = (txn) => {
    txn.logs = Array.isArray(txn.logs) ? txn.logs : [txn.logs || ""];
    return txn;
  };

  // Helper: replicate committed transaction
  const replicateWrite = async (txn, node, tconst) => {
    if (txn.success) {
      await replicator.runReplication(
        node,
        `UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='${tconst}'`
      );
      txn.logs.push(`[WRITE] ${node} + 1 (replicated)`);
    }
    return txn;
  };

  router.post("/runAllCases", async (req, res) => {
    const isolation = req.body.isolation || "READ COMMITTED";
    const scenario = req.body.scenario || "read-read";

    const results = {};

    try {
      switch (scenario) {
        // -------- Case 1: Read-Read (2 readers per node)
        case "read-read": {
          const readOpsNode2 = [
            async (c) => {
              const [[row]] = await c.query(
                "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0000002'"
              );
              return `[READ] Node2 runtimeMinutes=${row.runtimeMinutes}`;
            }
          ];
          const readOpsNode3 = [
            async (c) => {
              const [[row]] = await c.query(
                "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0038698'"
              );
              return `[READ] Node3 runtimeMinutes=${row.runtimeMinutes}`;
            }
          ];

          const txnResults = await Promise.all([
            runTransaction(db2, isolation, readOpsNode2, "Node2"),
            runTransaction(db2, isolation, readOpsNode2, "Node2-2"),
            runTransaction(db3, isolation, readOpsNode3, "Node3"),
            runTransaction(db3, isolation, readOpsNode3, "Node3-2")
          ]);

          results["read-read"] = txnResults.map(normalizeLogs);
          break;
        }

        // -------- Case 2: Read-Write (1 reader + 1 writer per node)
        case "read-write": {
          const readOpsNode2 = [
            async (c) => {
              const [[row]] = await c.query(
                "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0000002'"
              );
              return `[READ] Node2 runtimeMinutes=${row.runtimeMinutes}`;
            }
          ];
          const writeOpsNode2 = [
            async (c) => {
              await c.query(
                "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0000002'"
              );
              return "[WRITE] Node2 + 1";
            }
          ];

          const readOpsNode3 = [
            async (c) => {
              const [[row]] = await c.query(
                "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0038698'"
              );
              return `[READ] Node3 runtimeMinutes=${row.runtimeMinutes}`;
            }
          ];
          const writeOpsNode3 = [
            async (c) => {
              await c.query(
                "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0038698'"
              );
              return "[WRITE] Node3 + 1";
            }
          ];

          // Run transactions concurrently: reader + writer per node
          let [r2, w2, r3, w3] = await Promise.all([
            runTransaction(db2, isolation, readOpsNode2, "Node2-reader"),
            runTransaction(db2, isolation, writeOpsNode2, "Node2-writer"),
            runTransaction(db3, isolation, readOpsNode3, "Node3-reader"),
            runTransaction(db3, isolation, writeOpsNode3, "Node3-writer")
          ]);

          [r2, w2, r3, w3].forEach(normalizeLogs);

          // Replicate committed writes
          w2 = await replicateWrite(w2, "node2", "tt0000002");
          w3 = await replicateWrite(w3, "node3", "tt0038698");

          results["read-write"] = [r2, w2, r3, w3];
          break;
        }

        // -------- Case 3: Write-Write (2 writers per node)
        case "write-write": {
          const writeOpsNode2 = [
            async (c) => {
              await c.query(
                "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0000002'"
              );
              return "[WRITE] Node2 + 1";
            }
          ];
          const writeOpsNode3 = [
            async (c) => {
              await c.query(
                "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0038698'"
              );
              return "[WRITE] Node3 + 1";
            }
          ];

          let [w21, w22, w31, w32] = await Promise.all([
            runTransaction(db2, isolation, writeOpsNode2, "Node2-w1"),
            runTransaction(db2, isolation, writeOpsNode2, "Node2-w2"),
            runTransaction(db3, isolation, writeOpsNode3, "Node3-w1"),
            runTransaction(db3, isolation, writeOpsNode3, "Node3-w2")
          ]);

          [w21, w22, w31, w32].forEach(normalizeLogs);

          // replicate committed writes
          w21 = await replicateWrite(w21, "node2", "tt0000002");
          w22 = await replicateWrite(w22, "node2", "tt0000002");
          w31 = await replicateWrite(w31, "node3", "tt0038698");
          w32 = await replicateWrite(w32, "node3", "tt0038698");

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
