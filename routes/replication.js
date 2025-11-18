import express from "express";
export default function replicationRoutes(db1, db2, db3, replicator) {
  const router = express.Router();

  router.post("/replicationTest", async (req, res) => {
    try {
      const sqlNode2 = "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0000002'";
      const sqlNode3 = "UPDATE title_basics SET runtimeMinutes = runtimeMinutes + 1 WHERE tconst='tt0038698'";

      const node2Result = await replicator.runReplication("node1", sqlNode2);
      const node3Result = await replicator.runReplication("node1", sqlNode3);

      res.json({ node2Replication: node2Result, node3Replication: node3Result });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.get("/getTitle", async (req, res) => {
    try {
        const [[node1Row2]] = await db1.query(
        "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0000002'"
        );
        const [[node2Row2]] = await db2.query(
        "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0000002'"
        );

        const [[node1Row3]] = await db1.query(
        "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0038698'"
        );
        const [[node3Row3]] = await db3.query(
        "SELECT runtimeMinutes FROM title_basics WHERE tconst='tt0038698'"
        );

        res.json({
        node2: { node1: node1Row2?.runtimeMinutes, node2: node2Row2?.runtimeMinutes },
        node3: { node1: node1Row3?.runtimeMinutes, node3: node3Row3?.runtimeMinutes }
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
  }); 


  return router;
}
