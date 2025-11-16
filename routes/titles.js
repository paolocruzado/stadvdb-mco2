// routes/titles.js
import express from "express";

export default function(db1, db2, db3) {
  const router = express.Router();

  // Fetch first 20 rows + total count from a DB
  async function fetchNodeData(db) {
    const [rows] = await db.query(
      "SELECT tconst, primaryTitle, startYear, endYear, runtimeMinutes, genres FROM title_basics LIMIT 20"
    );
    const [[{ count: countRows }]] = await db.query(
      "SELECT COUNT(*) AS count FROM title_basics"
    );
    return { allRows: rows, countRows };
  }

  // GET /api/titles → first 20 rows from all nodes
  router.get("/", async (req, res) => {
    try {
      const results = await Promise.all([db1, db2, db3].map(fetchNodeData));
      res.json({ nodes: results });
    } catch (err) {
      console.error("Failed to fetch:", err);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}
