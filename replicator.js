import { v4 as uuidv4 } from "uuid";

export default function setupReplicator(app, db1, db2, db3) {
  const appliedReplications = new Set();
  const dbMap = { node1: db1, node2: db2, node3: db3 };

  const replicationMap = {
    node1: ["node2", "node3"],
    node2: ["node1"],
    node3: ["node1"],
  };

  async function runReplication(origin, sql, replicationId) {
    if (!origin || !sql) throw new Error("origin and sql required");
    const repId = replicationId || uuidv4();

    if (appliedReplications.has(repId)) {
      return { status: "already applied", replicationId: repId, updatedNodes: [] };
    }

    appliedReplications.add(repId);

    const updatedNodes = [];

    const targets = replicationMap[origin] || [];
    for (const target of targets) {
      await dbMap[target].query(sql);
      updatedNodes.push(target);
    }

    return { success: true, replicationId: repId, updatedNodes, sqlExecuted: sql };
  }

  app.post("/api/replicate", async (req, res) => {
    try {
      const { origin, sql, replicationId } = req.body;
      const result = await runReplication(origin, sql, replicationId);
      res.json(result);
    } catch (err) {
      console.error("Replication error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return { runReplication };
}
