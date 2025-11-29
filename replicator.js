import { v4 as uuidv4 } from "uuid";

export default function setupReplicator(app, db1, db2, db3) {
  const dbMap = { node1: db1, node2: db2, node3: db3 };

  // Node replication map (keys must match the transaction runner node names)
  const replicationMap = {
    "node1": ["node2", "node3"],
    "node2": ["node1"],
    "node3": ["node1"],
  };

  // --- Main replication function ---
  async function runReplication(originNode, sql, replicationId = null, isolation = "READ COMMITTED") {
    if (!originNode || !sql) throw new Error("originNode and sql required");
    const repId = replicationId || uuidv4();

    // Normalize originNode key for mapping
    let normalizedOrigin = originNode.toLowerCase().replace(/-.*/, ""); // "Node2-writer" → "node2"

    const targets = replicationMap[normalizedOrigin] || [];
    const updatedNodes = [];

    for (const target of targets) {
      const db = dbMap[target];
      const logs = [];

      try {
        // Check if target node is alive
        const [statusRow] = await db.query(
          "SELECT is_alive FROM node_status WHERE node_name = ?",
          [target]
        );
        const isAlive = statusRow?.[0]?.is_alive ?? true;

        if (!isAlive) {
          logs.push(`[${target}] ERROR: Node is down`);
          await dbMap.node1.query(
            `UPDATE replication_log SET status = 'failed', last_error = ? WHERE id = ?`,
            ["Node is down", repId]
          );
          updatedNodes.push({ node: target, logs, error: "Node is down" });
          continue;
        }

        // Apply SQL on target in transaction
        const conn = await db.getConnection();
        try {
          await conn.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`);
          logs.push(`[${target}] Isolation = ${isolation}`);

          await conn.beginTransaction();
          logs.push(`[${target}] BEGIN`);

          await conn.query(sql);
          logs.push(`[${target}] APPLY SQL: ${sql}`);

          await conn.commit();
          logs.push(`[${target}] COMMIT`);

          // Mark replication as success
          await dbMap.node1.query(
            `UPDATE replication_log SET status = 'success', applied_at = NOW(), last_error = NULL WHERE id = ?`,
            [repId]
          );

          updatedNodes.push({ node: target, logs });
        } catch (err) {
          logs.push(`[${target}] ERROR: ${err.message}`);
          await conn.rollback();
          logs.push(`[${target}] ROLLBACK`);

          await dbMap.node1.query(
            `UPDATE replication_log SET status = 'failed', last_error = ? WHERE id = ?`,
            [err.message, repId]
          );

          updatedNodes.push({ node: target, logs, error: err.message });
        } finally {
          conn.release();
        }
      } catch (outerErr) {
        logs.push(`[${target}] FATAL ERROR: ${outerErr.message}`);
        updatedNodes.push({ node: target, logs, error: outerErr.message });
      }
    }

    return { success: true, replicationId: repId, updatedNodes, sqlExecuted: sql };
  }

  // --- Replay failed replications for a specific node ---
  async function replayPendingReplications(targetNode) {
    const [pending] = await dbMap.node1.query(
      "SELECT * FROM replication_log WHERE target_node = ? AND status = 'failed' ORDER BY created_at ASC",
      [targetNode]
    );

    for (const row of pending) {
      try {
        await runReplication(row.origin_node, row.sql_text, row.id);
        console.log(`[Replay] Applied pending replication ${row.id} to ${targetNode}`);
      } catch (err) {
        console.error(`[Replay] Failed replication ${row.id}: ${err.message}`);
      }
    }
  }

  // --- API endpoints ---
  app.post("/api/replicate", async (req, res) => {
    try {
      const { originNode, sql, replicationId, isolation } = req.body;
      const result = await runReplication(originNode, sql, replicationId, isolation);
      res.json(result);
    } catch (err) {
      console.error("Replication error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  app.post("/api/replay/:node", async (req, res) => {
    const targetNode = req.params.node.toLowerCase();
    if (!dbMap[targetNode]) return res.status(400).json({ error: "Invalid node" });

    try {
      await replayPendingReplications(targetNode);
      res.json({ status: "replay triggered", targetNode });
    } catch (err) {
      console.error("Replay error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return { runReplication, replayPendingReplications, dbMap };
}
