// query.js
const express = require("express");
const getDb = require("./db");

const router = express.Router();

/**
 * POST /api/mql
 * Body:
 * {
 *   "collection": "sales",
 *   "type": "aggregate" | "find",
 *   "query": {} | []
 * }
 */
router.post("/api/mql", async (req, res) => {
  try {
    const { collection, type, query } = req.body;

    if (!collection || !type || !query) {
      return res.status(400).json({ error: "Invalid payload" });
    }

    const db = getDb();
    const col = db.collection(collection);

    let result;

    // -----------------------
    // SAFE MQL EXECUTION
    // -----------------------
    if (type === "find") {
      if (typeof query !== "object" || Array.isArray(query)) {
        return res.status(400).json({ error: "Find query must be an object" });
      }
      result = await col.find(query).limit(200).toArray();
    }

    else if (type === "aggregate") {
      if (!Array.isArray(query)) {
        return res.status(400).json({ error: "Aggregate query must be an array" });
      }
      result = await col.aggregate(query).toArray();
    }

    else {
      return res.status(400).json({ error: "Unsupported query type" });
    }

    res.json(result);

  } catch (err) {
    console.error("❌ MQL Error:", err);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
