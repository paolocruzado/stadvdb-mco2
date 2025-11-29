import { v4 as uuidv4 } from "uuid";

export default function setupReplicator(app, db1, db2, db3) {
  const appliedReplications = new Set();
  const dbMap = { node1: db1, node2: db2, node3: db3 };

  const replicationMap = {
    node1: ["node2", "node3"],
    node2: ["node1"],
    node3: ["node1"],
  };

  async function runReplication(origin, sql, replicationId, isolation = "READ COMMITTED") {
    if (!origin || !sql) throw new Error("origin and sql required");
    const repId = replicationId || uuidv4();

    if (appliedReplications.has(repId)) {
      return { status: "already applied", replicationId: repId, updatedNodes: [] };
    }
    appliedReplications.add(repId);

    const updatedNodes = [];
    const targets = replicationMap[origin] || [];

    for (const target of targets) {
      const db = dbMap[target];
      const conn = await db.getConnection();
      const logs = [];

      try {
        await conn.query(`SET SESSION TRANSACTION ISOLATION LEVEL ${isolation}`);
        logs.push(`[${target}] Isolation = ${isolation}`);

        await conn.beginTransaction();
        logs.push(`[${target}] BEGIN`);

        await conn.query(sql);
        logs.push(`[${target}] APPLY SQL: ${sql}`);

        await conn.commit();
        logs.push(`[${target}] COMMIT`);

        updatedNodes.push({ node: target, logs });
      } catch (err) {
        logs.push(`[${target}] ERROR: ${err.message}`);
        await conn.rollback();
        logs.push(`[${target}] ROLLBACK`);

        updatedNodes.push({ node: target, logs, error: err.message });
      } finally {
        conn.release();
      }
    }

    return {
      success: true,
      replicationId: repId,
      updatedNodes,
      sqlExecuted: sql
    };
  }

  app.post("/api/replicate", async (req, res) => {
    try {
      const { origin, sql, replicationId, isolation } = req.body;
      const result = await runReplication(origin, sql, replicationId, isolation);
      res.json(result);
    } catch (err) {
      console.error("Replication error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return { runReplication };
}
