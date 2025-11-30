import { v4 as uuidv4 } from "uuid";

export default function setupReplicator(app, db1, db2, db3) {
  const dbMap = { node1: db1, node2: db2, node3: db3 };

  const replicationMap = {
    node1: ["node2", "node3"],
    node2: ["node1"],
    node3: ["node1"],
  };

  async function determineTargetNode(originNode, sql) {
    const normalizedOrigin = originNode.toLowerCase().replace(/-.*/, "");

    if (normalizedOrigin === "node1") {
      const possibleTargets = replicationMap[normalizedOrigin] || [];

      if (/INSERT/i.test(sql)) {
        const match = sql.match(/VALUES\s*\(.*?(\d{4})/);
        if (!match) throw new Error("Cannot determine startYear for routing");
        const startYear = parseInt(match[1], 10);
        return startYear <= 2010 ? "node2" : "node3";
      } else {
        const tconstMatch = sql.match(/WHERE\s+tconst\s*=\s*['"]([^'"]+)['"]/i);
        if (!tconstMatch) throw new Error("Cannot determine tconst for routing");
        const tconst = tconstMatch[1];

        for (const target of possibleTargets) {
          const db = dbMap[target];
          const [rows] = await db.query(
            "SELECT 1 FROM title_basics WHERE tconst = ? LIMIT 1",
            [tconst]
          );
          if (rows.length > 0) return target;
        }

        throw new Error(`Cannot find target node for tconst ${tconst}`);
      }
    } else {
      const targets = replicationMap[normalizedOrigin] || [];
      if (targets.length === 0) throw new Error(`No target node defined for ${originNode}`);
      return targets[0];
    }
  }

  async function runReplication(originNode, sql, replicationId = null, isolation = "REPEATABLE READ", skipLogInsert = false) {
    if (!originNode || !sql) throw new Error("originNode and sql required");

    const normalizedOrigin = originNode.toLowerCase().replace(/-.*/, "");
    const originDb = dbMap[normalizedOrigin];
    const target = await determineTargetNode(originNode, sql); // single target
    const targetDb = dbMap[target];
    const repId = replicationId || uuidv4();
    const logs = [];

    if (!skipLogInsert) {
      await originDb.query(
        `INSERT INTO replication_log 
          (id, origin_node, target_node, sql_text, status, attempts) 
         VALUES (?, ?, ?, ?, ?, ?)`,
        [repId, originNode, target, sql, "pending", 0]
      );
    }

    try {
      const [statusRow] = await targetDb.query(
        "SELECT is_alive FROM node_status WHERE node_name = ?",
        [target]
      );
      const isAlive = statusRow?.[0]?.is_alive ?? true;

      if (!isAlive) {
        logs.push(`[${target}] Node is down, replication deferred`);
        await originDb.query(
          `UPDATE replication_log 
             SET attempts = attempts + 1, last_error = ?
           WHERE id = ?`,
          [`Target down: ${target}`, repId]
        );
        return { success: false, replicationId: repId, updatedNodes: [{ node: target, logs, error: "Node down" }] };
      }

      const conn = await targetDb.getConnection();
      try {
        await conn.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`);
        logs.push(`[${target}] Isolation = ${isolation}`);

        await conn.beginTransaction();
        logs.push(`[${target}] BEGIN`);

        await conn.query(sql);
        logs.push(`[${target}] APPLY SQL: ${sql}`);

        await conn.commit();
        logs.push(`[${target}] COMMIT`);

        await originDb.query(
          `UPDATE replication_log 
             SET status = 'success', applied_at = NOW(), last_error = NULL 
           WHERE id = ?`,
          [repId]
        );

        return { success: true, replicationId: repId, updatedNodes: [{ node: target, logs }] };

      } catch (err) {
        await conn.rollback();
        logs.push(`[${target}] ROLLBACK`);

        await originDb.query(
          `UPDATE replication_log 
             SET attempts = attempts + 1, last_error = ?
           WHERE id = ?`,
          [err.message, repId]
        );

        return { success: false, replicationId: repId, updatedNodes: [{ node: target, logs, error: err.message }] };
      } finally {
        conn.release();
      }

    } catch (outerErr) {
      logs.push(`[${target}] FATAL ERROR: ${outerErr.message}`);
      await originDb.query(
        `UPDATE replication_log 
           SET attempts = attempts + 1, last_error = ?
         WHERE id = ?`,
        [outerErr.message, repId]
      );
      return { success: false, replicationId: repId, updatedNodes: [{ node: target, logs, error: outerErr.message }] };
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

      for (const row of pendingRows) {
        try {
          await runReplication(row.origin_node, row.sql_text, row.id, undefined, true);
          console.log(`[Replay] Successfully replayed replication ${row.id} for ${targetNode}`);
        } catch (err) {
          console.error(`[Replay] Failed to replay replication ${row.id} for ${targetNode}: ${err.message}`);
        }
      }
    }
  }

  return { runReplication, replayPendingReplications, dbMap };
}
