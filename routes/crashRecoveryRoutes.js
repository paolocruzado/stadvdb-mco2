import express from "express";

export default function crashRecoveryRoutes(db1, db2, db3, replicator) {
  const router = express.Router();
  const dbMap = { node1: db1, node2: db2, node3: db3 };

  // --- Helper: set node online/offline ---
  async function setNodeStatus(node, online) {
    const db = dbMap[node];
    if (!db) throw new Error(`Invalid node ${node}`);
    await db.query(
      "UPDATE node_status SET online = ?, last_updated = NOW() WHERE node = ?",
      [online, node]
    );
  }

  // --- Helper: replay pending replications for a node ---
  async function replayPendingReplications() {
    const [pending] = await dbMap.node1.query(
      "SELECT * FROM replication_log WHERE applied = FALSE ORDER BY created_at ASC"
    );

    for (const row of pending) {
      try {
        await replicator.runReplication(row.origin_node, row.sql_text, row.id);
        await dbMap.node1.query("UPDATE replication_log SET applied = TRUE WHERE id = ?", [row.id]);
      } catch (err) {
        console.error("Replay failed for replication", row.id, err.message);
      }
    }
  }

  // --- API: crash a node ---
  router.post("/crash", async (req, res) => {
    const { node } = req.body;
    try {
      await setNodeStatus(node, false);
      res.json({ status: "ok", message: `${node} set to offline` });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // --- API: recover a node ---
  router.post("/recover", async (req, res) => {
    const { node } = req.body;
    try {
      await setNodeStatus(node, true);
      // Optionally replay missed replications
      await replayPendingReplications();
      res.json({ status: "ok", message: `${node} recovered` });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
