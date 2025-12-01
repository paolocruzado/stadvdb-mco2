import express from "express";
import { runTransaction } from "../transactionRunner.js";

export default function distributedConcurrencyRoutes(db2, db3, replicator) {
  const router = express.Router();

  const normalizeLogs = (txn) => {
    txn.logs = Array.isArray(txn.logs) ? txn.logs : [txn.logs || ""];
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

        // --- FIX: Define Operations as Data Objects ---
        // Instead of passing a function, we pass the SQL and metadata directly.
        const opType = type.toUpperCase(); // Ensure "READ" or "WRITE"
        
        const ops = [
          {
            sql: sql,
            type: opType,
            // logMsg is optional, the runner adds standard logging, 
            // but we can add specific context if needed.
            logMsg: `Executing ${opType} on ${node}` 
          }
        ];

        const txnResult = await runTransaction(
            db, 
            isolation, 
            ops, 
            `${node}-txn${idx + 1}`, 
            replicator
        );
        
        return normalizeLogs(txnResult);
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