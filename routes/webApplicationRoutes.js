import express from "express";
import mysql from "mysql2"; // Import mysql2 to format params
import { runTransaction } from "../transactionRunner.js";
import { LockManager } from "../lockmanager.js"; // Import LockManager

export default function webApplicationRoutes(db1, db2, db3, replicator) {
  const router = express.Router();
  
  // Initialize LockManager on Node 1 (Master)
  // We use Node 1 because locks require WRITE operations (INSERT/DELETE)
  const lockManager = new LockManager(db1);

  // Normalize logs helper
  const normalizeLogs = (txn) => {
    txn.logs = Array.isArray(txn.logs) ? txn.logs : [txn.logs || ""];
    return txn;
  };

  // Helper to throw detailed error if transaction failed
  const checkTxnSuccess = (txnResult) => {
    if (!txnResult.success) {
      const errorLog = txnResult.logs.find(l => typeof l === 'string' && l.includes('ERROR'));
      throw new Error(errorLog || "Transaction failed");
    }
    return txnResult;
  };

  // -------------------------
  // 1️⃣ Add Movie (Using Distributed Lock)
  // -------------------------
  router.post("/addMovie", async (req, res) => {
    const { title, startYear, runtimeMinutes, genres, isolation } = req.body;
    
    // Validate required fields
    if (!title || !startYear) {
        return res.status(400).json({ error: "Title and Start Year are required" });
    }

    const LOCK_KEY = "movie_id_generation";
    let lockAcquired = false;

    try {
      // 1. ACQUIRE LOCK (Waits if busy)
      // This prevents race conditions at the application level
      await lockManager.acquire(LOCK_KEY, "node1");
      lockAcquired = true;

      // 2. READ MAX ID (Safe to do now because we have the lock)
      // We read directly from db1 before starting the transaction runner
      const [rows] = await db1.query(
        "SELECT tconst FROM title_basics ORDER BY LENGTH(tconst) DESC, tconst DESC LIMIT 1"
      );
      
      let nextId = "tt0000001"; // Default if table is empty
      
      if (rows.length > 0) {
        const lastId = rows[0].tconst;
        const numPart = parseInt(lastId.replace("tt", ""), 10);
        if (!isNaN(numPart)) {
            // Increment and pad with zeros
            nextId = `tt${String(numPart + 1).padStart(7, "0")}`;
        }
      }

      // 3. PREPARE SQL
      const insertSql = `
        INSERT INTO title_basics (tconst, primaryTitle, startYear, runtimeMinutes, genres)
        VALUES (?, ?, ?, ?, ?)
      `;
      const params = [nextId, title, startYear, runtimeMinutes || null, genres || null];
      const formattedSql = mysql.format(insertSql, params);

      // 4. RUN TRANSACTION
      // Now we call the runner to execute the insert. 
      // We pass the fully formatted SQL so the runner handles the DB transaction and replication.
      const ops = [
        {
          sql: formattedSql,
          type: 'WRITE',
          logMsg: `[INSERT] Generated ID ${nextId}`
        }
      ];

      const txnResult = await runTransaction(
        db1, // Always write to Master
        isolation || "REPEATABLE READ",
        ops,
        "node1", // Strict node name for Replicator
        replicator
      );

      res.json(normalizeLogs(checkTxnSuccess(txnResult)));

    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    } finally {
      // 5. RELEASE LOCK (Always runs)
      if (lockAcquired) {
        await lockManager.release(LOCK_KEY);
      }
    }
  });

  // -------------------------
  // 3️⃣ Update Record
  // -------------------------
  router.post("/update", async (req, res) => {
    const { isolation, sql } = req.body;
    // sql expected structure: { query: string, params: any[] }
    if (!sql || !sql.query) return res.status(400).json({ error: "Missing SQL" });

    const db = db1; // ALWAYS node1 for writes

    try {
      // Manually format the SQL + Params into one string
      const formattedSql = mysql.format(sql.query, sql.params || []);

      const ops = [
        {
          sql: formattedSql,
          type: 'WRITE',
          logMsg: '[UPDATE] Executed on node1'
        }
      ];

      const txnResult = await runTransaction(
        db,
        isolation || "READ COMMITTED",
        ops,
        "node1", // FIX: Must be "node1" so replicator knows the source is the Master
        replicator
      );

      res.json({ logs: checkTxnSuccess(txnResult).logs });

    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  // -------------------------
  // 4️⃣ Search/View Records
  // -------------------------
  router.get("/search", async (req, res) => {
    const { title, genre, minRuntime, maxRuntime, startYear } = req.query;

    const conditions = [];
    const params = [];

    if (title) {
      conditions.push("primaryTitle LIKE ?");
      params.push(`%${title}%`);
    }
    if (genre) {
      conditions.push("genres LIKE ?");
      params.push(`%${genre}%`);
    }
    if (startYear) {
      conditions.push("startYear = ?");
      params.push(startYear);
    }
    if (minRuntime) {
      conditions.push("runtimeMinutes >= ?");
      params.push(minRuntime);
    }
    if (maxRuntime) {
      conditions.push("runtimeMinutes <= ?");
      params.push(maxRuntime);
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
    const rawSql = `SELECT tconst, primaryTitle, startYear, runtimeMinutes, genres
                    FROM title_basics ${whereClause} LIMIT 100`;

    // 1. COMBINE SQL AND PARAMS HERE
    const finalSql = mysql.format(rawSql, params);

    console.log("Search SQL (Formatted):", finalSql);

    try {
      let results = [];

      // 2. Pass the combined 'finalSql' to the runner
      const readOp = {
        sql: finalSql,
        type: 'READ',
        logMsg: 'Search Query'
      };

      // --- UPDATED LOGIC: SEARCH ONLY NODE 2 AND NODE 3 ---
      // We explicitly exclude Node 1 (Master) to enforce read-replica architecture.
      // We search both Node 2 and Node 3 and merge results (Scatter-Gather).
      const txnResults = await Promise.all([db2, db3].map(async (db, idx) => {
        // Safe check: if a db connection isn't available, skip it
        if (!db) return [];

        const nodeName = `node${idx + 2}`; // idx 0 -> node2, idx 1 -> node3

        const txnResult = await runTransaction(
          db,
          "REPEATABLE READ",
          [readOp],
          `search-${nodeName}`,
          replicator
        );

        if (!txnResult.success) {
          console.error(`Search failed on ${nodeName}:`, txnResult.logs);
          return [];
        }

        return txnResult.logs.filter(r => r.rows).flatMap(r => r.rows);
      }));

      // Combine and remove duplicates based on tconst
      const merged = {};
      txnResults.flat().forEach(r => merged[r.tconst] = r);
      results = Object.values(merged);

      res.json({ rows: results });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  // -------------------------
  // 5️⃣ Reports (text-based)
  // -------------------------
  router.get("/reports", async (req, res) => {
    const runOnNode = async (db, nodeName) => {
      const sql = `
        SELECT 
          FLOOR(startYear / 10) * 10 AS decade,
          COUNT(*) AS count,
          AVG(runtimeMinutes) AS avgRuntime
        FROM title_basics
        GROUP BY decade
        ORDER BY decade
      `;

      const txnResult = await runTransaction(
        db,
        "REPEATABLE READ",
        [{ sql: sql, type: 'READ', logMsg: 'Report Stats' }],
        `${nodeName}-reports`,
        replicator
      );

      if (!txnResult.success) {
        console.error(`Report failed on ${nodeName}:`, txnResult.logs);
        return [];
      }

      return txnResult.logs.find(r => r.rows)?.rows || [];
    };

    try {
      const [node2Stats, node3Stats] = await Promise.all([
        runOnNode(db2, "node2"),
        runOnNode(db3, "node3"),
      ]);

      const merged = {};

      const pushStats = (stats) => {
        for (const row of stats) {
          const decade = row.decade;
          if (!merged[decade]) {
            merged[decade] = {
              decade,
              totalCount: 0,
              weightedRuntimeSum: 0,
            };
          }

          const count = row.count || 0;
          const avg = row.avgRuntime || 0;

          merged[decade].totalCount += count;
          merged[decade].weightedRuntimeSum += avg * count;
        }
      };

      pushStats(node2Stats);
      pushStats(node3Stats);

      const finalMerged = Object.values(merged)
        .map(d => ({
          decade: d.decade,
          count: d.totalCount,
          avgRuntime:
            d.totalCount > 0
              ? (d.weightedRuntimeSum / d.totalCount)
              : null,
        }))
        .sort((a, b) => a.decade - b.decade);

      res.json({ decadeStats: finalMerged });

    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/delete", async (req, res) => {
    const { tconst, isolation } = req.body;
    
    // 1. Validation
    if (!tconst) {
        return res.status(400).json({ error: "tconst is required for deletion" });
    }

    const db = db1; // Writes/Deletes go to Master (Node 1)

    try {
      // 2. Format SQL safely
      const deleteSql = "DELETE FROM title_basics WHERE tconst = ?";
      const formattedSql = mysql.format(deleteSql, [tconst]);

      // 3. Define Operation
      const ops = [
        {
          sql: formattedSql,
          type: 'WRITE', // Triggers replication logic
          logMsg: `[DELETE] Deleting record ${tconst}`
        }
      ];

      // 4. Run Transaction
      const txnResult = await runTransaction(
        db,
        isolation || "READ COMMITTED",
        ops,
        "node1", // Source is Node 1
        replicator
      );

      // 5. Response
      res.json(normalizeLogs(checkTxnSuccess(txnResult)));

    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}