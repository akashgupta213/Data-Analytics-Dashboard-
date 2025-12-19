// db.js
const mongoose = require("mongoose");

function getDb() {
  if (!mongoose.connection.readyState) {
    throw new Error("MongoDB not connected yet");
  }
  return mongoose.connection.db;
}

module.exports = getDb;
