import express from "express";
import { runTransaction } from "../transactionRunner.js";

export default function crashRecoveryRoutes(db1, db2, db3, replicator) {
  const router = express.Router();
  const dbMap = { node1: db1, node2: db2, node3: db3 };

  async function setNodeStatusBroadcast(targetNode, isAlive) {
    const query = "UPDATE node_status SET is_alive = ?, last_changed = NOW() WHERE node_name = ?";
    const val = isAlive ? 1 : 0;

    for (const db of Object.values(dbMap)) {
      await db.query(query, [val, targetNode]);
    }
  }

  async function replayPendingReplications(targetNode) {
    const normalizedTarget = targetNode.toLowerCase();

    for (const [originName, originDb] of Object.entries(dbMap)) {
      const [pendingRows] = await originDb.query(
        `SELECT * FROM replication_log 
          WHERE target_node = ? AND status = 'pending' 
          ORDER BY created_at ASC`,
        [normalizedTarget]
      );

      console.log(`[Replay] Found ${pendingRows.length} pending replications for ${targetNode} from ${originName}`);

      for (const row of pendingRows) {
        try {
          const result = await replicator.runReplication(
            row.origin_node,
            row.sql_text,
            row.id,
            "REPEATABLE READ",
            true 
          );

          const allSucceeded = result.updatedNodes.every(u => !u.error);

          if (allSucceeded) {
            await originDb.query(
              `UPDATE replication_log 
                SET status = 'success', applied_at = NOW(), last_error = NULL 
              WHERE id = ?`,
              [row.id]
            );
            console.log(`[Replay] ${row.id} successfully applied to ${targetNode}`);
          } else {
            const failedNodes = result.updatedNodes
              .filter(u => u.error)
              .map(u => u.node)
              .join(", ");
            await originDb.query(
              `UPDATE replication_log 
                SET attempts = attempts + 1, last_error = ? 
              WHERE id = ?`,
              [`Failed nodes: ${failedNodes}`, row.id]
            );
            console.log(`[Replay] ${row.id} partially failed for: ${failedNodes}`);
          }
        } catch (err) {
          console.error(`[Replay] Fatal error for ${row.id}: ${err.message}`);
          await originDb.query(
            `UPDATE replication_log 
              SET attempts = attempts + 1, last_error = ? 
            WHERE id = ?`,
            [err.message, row.id]
          );
        }
      }
    }
  }

  router.get("/node-status", async (req, res) => {
    try {
      const [rows] = await db1.query("SELECT node_name, is_alive FROM node_status");
      const status = {};
      rows.forEach((r) => (status[r.node_name] = r.is_alive === 1));
      res.json(status);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/crash", async (req, res) => {
    const { node } = req.body;
    try {
      await setNodeStatusBroadcast(node, false);
      res.json({ status: "ok", message: `${node} set to OFFLINE on all nodes` });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/recover", async (req, res) => {
    const { node } = req.body;
    try {
      await setNodeStatusBroadcast(node, true);
      await replayPendingReplications(node);
      res.json({ status: "ok", message: `${node} recovered and pending replications replayed` });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/transaction", async (req, res) => {
    const { node, isolation, sql } = req.body;
    const db = dbMap[node];
    if (!db) return res.status(400).json({ error: "Invalid node" });

    try {
      // --- FIX: Define Operations as Data Objects ---
      // We assume this is a WRITE because the original code returned 'writeSql'
      // which triggers the replication logic in runTransaction.
      const ops = [
        {
          sql: sql,
          type: 'WRITE',
          logMsg: `Executed SQL on ${node}`
        }
      ];

      const txnResult = await runTransaction(
        db,
        isolation || "REPEATABLE READ",
        ops,
        `${node}-txn-${Date.now()}`,
        replicator
      );

      res.json({ status: "ok", node, isolation, txnResult });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}