rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "upu-db-1:27017", priority: 2 },
    { _id: 1, host: "upu-db-2:27017", priority: 1 },
    { _id: 2, host: "upu-db-3:27017", priority: 1 }
  ]
});

// Wait for the primary election to settle, then print status.
sleep(2000);
rs.status();
