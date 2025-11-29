import express from "express";
import { runTransaction } from "../transactionRunner.js";

export default function distributedConcurrencyRoutes(db2, db3, replicator) {
  const router = express.Router();

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

  router.post("/custom", async (req, res) => {
    const isolation = req.body.isolation || "READ COMMITTED";
    const transactions = req.body.transactions || [];

    if (!Array.isArray(transactions) || transactions.length === 0) {
      return res.status(400).json({ error: "No transactions provided" });
    }

    try {
      const txnPromises = transactions.map(async (txn, idx) => {
        const { node, type, sql } = txn;

        if (!node || !type || !sql) {
          return { logs: [`Transaction ${idx + 1} missing fields`], success: false };
        }

        const db = node === "node2" ? db2 : node === "node3" ? db3 : null;
        if (!db) {
          return { logs: [`Transaction ${idx + 1}: Invalid node ${node}`], success: false };
        }

        const ops = [
          async (c) => {
            if (type === "read") {
              const [rows] = await c.query(sql);
              return `[READ ${node}] rows=${JSON.stringify(rows)}`;
            } else if (type === "write") {
              await c.query(sql);
              return `[WRITE ${node}] executed`;
            } else {
              return `[Transaction ${idx + 1}] Invalid type ${type}`;
            }
          },
        ];

        let txnResult = await runTransaction(db, isolation, ops, `${node}-txn${idx + 1}`);
        txnResult = normalizeLogs(txnResult);

        if (type === "write") {
          txnResult = await replicateWrite(txnResult, node, sql);
        }

        return txnResult;
      });

      const results = await Promise.all(txnPromises);

      res.json({ isolation, results });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
