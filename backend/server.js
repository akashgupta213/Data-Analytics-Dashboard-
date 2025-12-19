const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");

const app = express();
app.use(cors());
app.use(express.json());

// Connect to MongoDB
mongoose
  .connect(
    "mongodb+srv://rtofly3_db_user:gOYW0ZDhMW5E2OAr@cluster0.sbdu86b.mongodb.net/bigdata"
  )
  .then(() => console.log("✅ MongoDB Connected"))
  .catch(err => console.error(err));

const SaleSchema = new mongoose.Schema({}, { strict: false });
const Sale = mongoose.model("Sale", SaleSchema, "sales");

/* ===============================
   1️⃣ Revenue by Product (BAR)
================================ */
app.get("/api/revenue-by-product", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$Product",
          revenue: {
            $sum: { $multiply: ["$Quantity Ordered", "$Price"] }
          }
        }
      },
      { $sort: { revenue: -1 } }
    ]);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ===============================
   2️⃣ Revenue by Product Type (PIE)
================================ */
app.get("/api/revenue-by-product-type", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$Product Type",
          revenue: {
            $sum: { $multiply: ["$Quantity Ordered", "$Price"] }
          }
        }
      }
    ]);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Revenue by city (COMPARISON SAFE)
app.get("/api/revenue-by-city", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$City",
          revenue: {
            $sum: { $multiply: ["$Quantity Ordered", "$Price"] }
          }
        }
      },
      { $sort: { revenue: -1 } }
    ]);

    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to compute revenue by city" });
  }
});




/* ===============================
   4️⃣ Top 5 Products (TABLE / KPI)
================================ */
app.get("/api/top-products", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$Product",
          revenue: {
            $sum: { $multiply: ["$Quantity Ordered", "$Price"] }
          }
        }
      },
      { $sort: { revenue: -1 } },
      { $limit: 11 }
    ]);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// Quantity sold by product
app.get("/api/quantity-by-product", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$Product",
          quantity: { $sum: "$Quantity Ordered" }
        }
      },
      { $sort: { quantity: -1 } }
    ]);

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Failed to compute quantity by product" });
  }
});

// Orders count by city
app.get("/api/orders-by-city", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$City",
          orders: { $sum: 1 }
        }
      }
    ]);

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Failed to compute orders by city" });
  }
});


app.get("/api/quantity-by-product-type-city", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: {
            productType: "$Product Type",
            city: "$City"
          },
          quantity: { $sum: "$Quantity Ordered" }
        }
      },
      {
        $project: {
          _id: 0,
          type: "$_id.productType",
          city: "$_id.city",
          quantity: 1
        }
      }
    ]);

    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to compute quantity by product type & city" });
  }
});



// --------------------
// API: Top products by quantity
// --------------------
app.get("/api/top-products-by-quantity", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$Product",
          totalQuantity: { $sum: "$Quantity Ordered" }
        }
      },
      { $sort: { totalQuantity: -1 } },
      { $limit: 11 }
    ]);

    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch top products by quantity" });
  }
});


// ----------------------------
// CITY & PRODUCT ANALYSIS APIs
// ----------------------------

// ===============================
// CITY & PRODUCT ANALYSIS APIs
// ===============================


// Revenue by city per month (SMOOTH LINE - FIXED)
app.get("/api/revenue-by-city-over-time", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        // Convert string to Date safely (auto-detect format)
        $addFields: {
          orderDate: {
            $dateFromString: {
              dateString: "$Order Timestamp",
              onError: null,
              onNull: null
            }
          }
        }
      },
      {
        // Remove invalid dates
        $match: {
          orderDate: { $ne: null }
        }
      },
      {
        // Group by City + Month
        $group: {
          _id: {
            city: "$City",
            month: {
              $dateToString: {
                format: "%Y-%m",
                date: "$orderDate"
              }
            }
          },
          revenue: {
            $sum: {
              $multiply: ["$Quantity Ordered", "$Price"]
            }
          }
        }
      },
      {
        // Reshape output
        $project: {
          _id: 0,
          city: "$_id.city",
          month: "$_id.month",
          revenue: 1
        }
      },
      {
        // Sort by time
        $sort: { month: 1 }
      }
    ]);

    res.json(data);
  } catch (err) {
    console.error("❌ Revenue by city over time error:", err);
    res.status(500).json({
      error: "Failed to compute revenue by city over time"
    });
  }
});



// 2️⃣ City Contribution (Donut)
app.get("/api/city-contribution", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: "$City",
          revenue: {
            $sum: { $multiply: ["$Quantity Ordered", "$Price"] }
          }
        }
      }
    ]);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "City contribution failed" });
  }
});

// Product vs City – Revenue
app.get("/api/product-vs-city", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      {
        $group: {
          _id: {
            product: "$Product",
            city: "$City"
          },
          revenue: {
            $sum: { $multiply: ["$Quantity Ordered", "$Price"] }
          }
        }
      },
      {
        $project: {
          _id: 0,
          product: "$_id.product",
          city: "$_id.city",
          revenue: 1
        }
      }
    ]);

    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to compute product vs city" });
  }
});


// 4️⃣ Best Product per City (Table)
// Best product per city (FIXED)
app.get("/api/best-product-per-city", async (req, res) => {
  try {
    const data = await Sale.aggregate([
      // 1️⃣ Revenue per product per city
      {
        $group: {
          _id: {
            city: "$City",
            product: "$Product"
          },
          revenue: {
            $sum: { $multiply: ["$Quantity Ordered", "$Price"] }
          }
        }
      },

      // 2️⃣ Sort by city + highest revenue first
      {
        $sort: {
          "_id.city": 1,
          revenue: -1
        }
      },

      // 3️⃣ Pick top product per city
      {
        $group: {
          _id: "$_id.city",
          product: { $first: "$_id.product" },
          revenue: { $first: "$revenue" }
        }
      },

      // 4️⃣ Final shape
      {
        $project: {
          _id: 0,
          city: "$_id",
          product: 1,
          revenue: 1
        }
      }
    ]);

    res.json(data);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch best product per city" });
  }
});


// --------------------
// CUSTOM QUERY API
// (BUILDER MODE + MQL MODE)
// --------------------
app.post("/api/custom-query", async (req, res) => {
  try {
    /**
     * MODE 1: RAW MQL
     * ----------------
     * body = {
     *   mode: "mql",
     *   type: "find" | "aggregate",
     *   query: {} | []
     * }
     */
    if (req.body.mode === "mql") {
      const { type, query } = req.body;

      if (!type || !query) {
        return res.status(400).json({ error: "Invalid MQL payload" });
      }

      let data;

      if (type === "find") {
        if (typeof query !== "object" || Array.isArray(query)) {
          return res.status(400).json({ error: "Find query must be an object" });
        }
        data = await Sale.find(query).limit(100);
      }

      else if (type === "aggregate") {
        if (!Array.isArray(query)) {
          return res.status(400).json({ error: "Aggregate query must be an array" });
        }
        data = await Sale.aggregate(query);
      }

      else {
        return res.status(400).json({ error: "Unsupported MQL type" });
      }

      return res.json(data);
    }

    /**
     * MODE 2: QUERY BUILDER (OLD LOGIC)
     * ----------------
     * body = {
     *   city,
     *   productType,
     *   startDate,
     *   endDate,
     *   metric,
     *   agg,
     *   groupBy
     * }
     */

    const {
      city,
      productType,
      startDate,
      endDate,
      metric = "revenue",   // revenue | quantity
      agg = "sum",          // sum | avg | count
      groupBy               // city | product | productType
    } = req.body;

    const pipeline = [];

    // --------------------
    // ADD DATE FIELD
    // --------------------
    pipeline.push({
      $addFields: {
        orderDate: {
          $dateFromString: {
            dateString: "$Order Timestamp",
            format: "%d-%m-%Y %H:%M",
            onError: null,
            onNull: null
          }
        }
      }
    });

    // --------------------
    // MATCH (filters)
    // --------------------
    const match = {};

    if (city) match.City = city;
    if (productType) match["Product Type"] = productType;

    if (startDate || endDate) {
      match.orderDate = {};
      if (startDate) match.orderDate.$gte = new Date(startDate);
      if (endDate) match.orderDate.$lte = new Date(endDate);
    }

    if (Object.keys(match).length > 0) {
      pipeline.push({ $match: match });
    }

    // --------------------
    // GROUP
    // --------------------
    let groupId = null;

    if (groupBy === "city") groupId = "$City";
    if (groupBy === "product") groupId = "$Product";
    if (groupBy === "productType") groupId = "$Product Type";

    const groupStage = { _id: groupId };

    if (agg === "count") {
      groupStage.value = { $sum: 1 };
    } else {
      const field =
        metric === "quantity"
          ? "$Quantity Ordered"
          : { $multiply: ["$Quantity Ordered", "$Price"] };

      groupStage.value =
        agg === "avg"
          ? { $avg: field }
          : { $sum: field };
    }

    pipeline.push({ $group: groupStage });

    // --------------------
    // SORT
    // --------------------
    pipeline.push({ $sort: { value: -1 } });

    const data = await Sale.aggregate(pipeline);
    res.json(data);

  } catch (err) {
    console.error("❌ Custom Query Error:", err);
    res.status(500).json({ error: "Custom query failed" });
  }
});


app.listen(5000, () =>
  console.log("🚀 Server running on http://localhost:5000")
);
