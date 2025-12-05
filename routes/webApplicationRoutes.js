import express from "express";
import mysql from "mysql2"; 
import { runTransaction } from "../transactionRunner.js";
import { LockManager } from "../lockmanager.js"; 

export default function webApplicationRoutes(db1, db2, db3, replicator) {
  const router = express.Router();
  
  const lockManager = new LockManager(db1);

  const normalizeLogs = (txn) => {
    txn.logs = Array.isArray(txn.logs) ? txn.logs : [txn.logs || ""];
    return txn;
  };

  const checkTxnSuccess = (txnResult) => {
    if (!txnResult.success) {
      const errorLog = txnResult.logs.find(l => typeof l === 'string' && l.includes('ERROR'));
      throw new Error(errorLog || "Transaction failed");
    }
    return txnResult;
  };

  // Helper: Check if db1 is alive, if not determine failover node based on startYear
  const getActiveDbForWrite = async (startYear = null) => {
    try {
      const [statusRow] = await db1.query(
        "SELECT is_alive FROM node_status WHERE node_name = 'node1'"
      );
      const isAlive = statusRow?.[0]?.is_alive ?? true;
      
      if (isAlive) {
        return { db: db1, node: "node1" };
      }
    } catch (err) {
      console.warn("[Failover] Could not check node1 status:", err.message);
    }

    // db1 is down, determine which replica to use based on startYear
    if (startYear !== null) {
      const targetNode = startYear <= 2010 ? "node2" : "node3";
      const targetDb = targetNode === "node2" ? db2 : db3;
      console.log(`[Failover] db1 is down, routing to ${targetNode} based on startYear=${startYear}`);
      return { db: targetDb, node: targetNode };
    }

    // If no startYear provided, try node2 first (default fallback)
    console.log("[Failover] db1 is down, defaulting to node2");
    return { db: db2, node: "node2" };
  };

  // Helper: Get active DB for read operations, with failover
  const getActiveDbForRead = async (preferredNodes = ["node2", "node3"]) => {
    const candidates = [];
    
    // Check preferred nodes first
    for (const nodeName of preferredNodes) {
      try {
        const db = nodeName === "node2" ? db2 : nodeName === "node3" ? db3 : db1;
        const [statusRow] = await db.query(
          "SELECT is_alive FROM node_status WHERE node_name = ?",
          [nodeName]
        );
        const isAlive = statusRow?.[0]?.is_alive ?? true;
        
        if (isAlive) {
          console.log(`[Read Failover] Using ${nodeName}`);
          return { db, node: nodeName };
        }
      } catch (err) {
        console.warn(`[Read Failover] Could not check ${nodeName}:`, err.message);
      }
    }

    // All preferred nodes down, fallback to node1
    console.log("[Read Failover] Preferred nodes down, falling back to node1");
    return { db: db1, node: "node1" };
  };

  router.post("/addMovie", async (req, res) => {
    const { title, startYear, runtimeMinutes, genres, isolation } = req.body;
    
    if (!title || !startYear) {
        return res.status(400).json({ error: "Title and Start Year are required" });
    }

    const LOCK_KEY = "movie_id_generation";
    let lockAcquired = false;

    try {
      // Use failover logic to determine which node to write to
      const { db: targetDb, node: targetNode } = await getActiveDbForWrite(startYear);
      
      await lockManager.acquire(LOCK_KEY, targetNode);
      lockAcquired = true;

      const [rows] = await targetDb.query(
        "SELECT tconst FROM title_basics ORDER BY LENGTH(tconst) DESC, tconst DESC LIMIT 1"
      );
      
      let nextId = "tt0000001";
      
      if (rows.length > 0) {
        const lastId = rows[0].tconst;
        const numPart = parseInt(lastId.replace("tt", ""), 10);
        if (!isNaN(numPart)) {
            nextId = `tt${String(numPart + 1).padStart(7, "0")}`;
        }
      }

      const insertSql = `
        INSERT INTO title_basics (tconst, primaryTitle, startYear, runtimeMinutes, genres)
        VALUES (?, ?, ?, ?, ?)
      `;
      const params = [nextId, title, startYear, runtimeMinutes || null, genres || null];
      const formattedSql = mysql.format(insertSql, params);

      const ops = [
        {
          sql: formattedSql,
          type: 'WRITE',
          logMsg: `[INSERT] Generated ID ${nextId}`
        }
      ];

      const txnResult = await runTransaction(
        targetDb, 
        isolation || "REPEATABLE READ",
        ops,
        targetNode,
        replicator
      );

      res.json(normalizeLogs(checkTxnSuccess(txnResult)));

    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    } finally {
      if (lockAcquired) {
        await lockManager.release(LOCK_KEY);
      }
    }
  });

  router.post("/update", async (req, res) => {
    const { isolation, sql } = req.body;
    if (!sql || !sql.query) return res.status(400).json({ error: "Missing SQL" });

    try {
      // Extract tconst from WHERE clause to determine target node's startYear
      const tconstMatch = sql.query.match(/WHERE\s+tconst\s*=\s*['"]?([^'"]+)['"]?/i);
      let startYear = null;
      
      if (tconstMatch) {
        const tconst = tconstMatch[1];
        // Query db1 first for startYear, if down query replicas
        try {
          const [rows] = await db1.query(
            "SELECT startYear FROM title_basics WHERE tconst = ?",
            [tconst]
          );
          if (rows.length > 0) {
            startYear = rows[0].startYear;
          }
        } catch (err) {
          console.warn("[Update] Could not query db1 for startYear:", err.message);
          // Try replicas
          for (const db of [db2, db3]) {
            try {
              const [rows] = await db.query(
                "SELECT startYear FROM title_basics WHERE tconst = ?",
                [tconst]
              );
              if (rows.length > 0) {
                startYear = rows[0].startYear;
                break;
              }
            } catch (e) {
              continue;
            }
          }
        }
      }

      // Use failover logic to determine which node to write to
      const { db: targetDb, node: targetNode } = await getActiveDbForWrite(startYear);

      const formattedSql = mysql.format(sql.query, sql.params || []);

      const ops = [
        {
          sql: formattedSql,
          type: 'WRITE',
          logMsg: `[UPDATE] Executed on ${targetNode}`
        }
      ];

      const txnResult = await runTransaction(
        targetDb,
        isolation || "REPEATABLE READ",
        ops,
        targetNode,
        replicator
      );

      res.json({ logs: checkTxnSuccess(txnResult).logs });

    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

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

    const finalSql = mysql.format(rawSql, params);

    console.log("Search SQL (Formatted):", finalSql);

    try {
      let results = [];

      const readOp = {
        sql: finalSql,
        type: 'READ',
        logMsg: 'Search Query'
      };

      // Check if BOTH node2 and node3 are alive
      let node2Alive = false;
      let node3Alive = false;

      try {
        const [statusRow] = await db2.query(
          "SELECT is_alive FROM node_status WHERE node_name = 'node2'"
        );
        node2Alive = statusRow?.[0]?.is_alive ?? true;
      } catch (err) {
        console.warn("[Search] Could not check node2 status:", err.message);
      }

      try {
        const [statusRow] = await db3.query(
          "SELECT is_alive FROM node_status WHERE node_name = 'node3'"
        );
        node3Alive = statusRow?.[0]?.is_alive ?? true;
      } catch (err) {
        console.warn("[Search] Could not check node3 status:", err.message);
      }

      let nodesToQuery = [];
      if (node2Alive && node3Alive) {
        // Both alive, query both
        nodesToQuery = [
          { db: db2, node: "node2" },
          { db: db3, node: "node3" }
        ];
      } else if (!node2Alive || !node3Alive) {
        // Either down, fallback to node1
        console.log("[Search] Either node2 or node3 are down, falling back to node1");
        nodesToQuery = [{ db: db1, node: "node1" }];
      }

      const txnResults = await Promise.all(nodesToQuery.map(async ({ db, node }) => {
        try {
          const txnResult = await runTransaction(
            db,
            "REPEATABLE READ",
            [readOp],
            `search-${node}`,
            replicator
          );

          if (!txnResult.success) {
            console.error(`Search failed on ${node}:`, txnResult.logs);
            return [];
          }

          return txnResult.logs.filter(r => r.rows).flatMap(r => r.rows);
        } catch (err) {
          console.error(`Search exception on ${node}:`, err.message);
          return [];
        }
      }));

      const merged = {};
      txnResults.flat().forEach(r => merged[r.tconst] = r);
      results = Object.values(merged);

      res.json({ rows: results });
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

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
      // Check if BOTH node2 and node3 are alive
      let node2Alive = false;
      let node3Alive = false;

      try {
        const [statusRow] = await db2.query(
          "SELECT is_alive FROM node_status WHERE node_name = 'node2'"
        );
        node2Alive = statusRow?.[0]?.is_alive ?? true;
      } catch (err) {
        console.warn("[Reports] Could not check node2 status:", err.message);
      }

      try {
        const [statusRow] = await db3.query(
          "SELECT is_alive FROM node_status WHERE node_name = 'node3'"
        );
        node3Alive = statusRow?.[0]?.is_alive ?? true;
      } catch (err) {
        console.warn("[Reports] Could not check node3 status:", err.message);
      }

      // Determine which nodes to query
      let nodesToQuery = [];
      if (node2Alive && node3Alive) {
        // Both alive, query both
        nodesToQuery = ["node2", "node3"];
      } else if (!node2Alive || !node3Alive) {
        // Both down, fallback to node1
        console.log("[Reports] Either node2 or node3 are down, falling back to node1");
        nodesToQuery = ["node1"];
      } 
      const nodeStats = [];
      
      for (const nodeName of nodesToQuery) {
        const db = nodeName === "node2" ? db2 : nodeName === "node3" ? db3 : db1;
        
        const sql = `
          SELECT 
            FLOOR(startYear / 10) * 10 AS decade,
            COUNT(*) AS count,
            AVG(runtimeMinutes) AS avgRuntime
          FROM title_basics
          GROUP BY decade
          ORDER BY decade
        `;

        try {
          const txnResult = await runTransaction(
            db,
            "REPEATABLE READ",
            [{ sql: sql, type: 'READ', logMsg: 'Report Stats' }],
            `${nodeName}-reports`,
            replicator
          );

          if (!txnResult.success) {
            console.error(`Report failed on ${nodeName}:`, txnResult.logs);
            nodeStats.push([]);
          } else {
            const stats = txnResult.logs.find(r => r.rows)?.rows || [];
            nodeStats.push(stats);
          }
        } catch (err) {
          console.error(`Report exception on ${nodeName}:`, err.message);
          nodeStats.push([]);
        }
      }

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

      for (const stats of nodeStats) {
        pushStats(stats);
      }

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
    
    if (!tconst) {
        return res.status(400).json({ error: "tconst is required for deletion" });
    }

    const db = db1; 

    try {
      const deleteSql = "DELETE FROM title_basics WHERE tconst = ?";
      const formattedSql = mysql.format(deleteSql, [tconst]);

      const ops = [
        {
          sql: formattedSql,
          type: 'WRITE', 
          logMsg: `[DELETE] Deleting record ${tconst}`
        }
      ];

      const txnResult = await runTransaction(
        db,
        isolation || "REPEATABLE READ",
        ops,
        "node1", 
        replicator
      );

      res.json(normalizeLogs(checkTxnSuccess(txnResult)));

    } catch (err) {
      console.error(err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}