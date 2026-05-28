$ErrorActionPreference = "Stop"

Write-Host "=== Scenario: Producer/Consumer on MongoDB replica set ===" -ForegroundColor Green

# 1) Ensure cluster is up
cd "$PSScriptRoot\.."
docker compose up -d | Out-Host
Start-Sleep -Seconds 5

docker exec upu-db-1 mongosh --quiet --eval "rs.status().members.forEach(m => print(m.name, m.stateStr))" | Out-Host

# 2) Optional cleanup from previous runs
docker exec upu-db-1 mongosh hospital --quiet --eval "db.events_queue.drop(); db.events_results.drop(); print('dropped events collections')" | Out-Host

# 3) Start consumer first (so it can immediately drain queue)
$consumer = Start-Process -FilePath python -ArgumentList "scripts\consumer.py --uri `"mongodb://localhost:27017/?directConnection=true`" --max-messages 2000 --idle-timeout-s 20 --write-concern majority" -NoNewWindow -PassThru

Start-Sleep -Seconds 1

# 4) Run producer
python scripts\producer.py --uri "mongodb://localhost:27017/?directConnection=true" --messages 2000 --batch-size 100 --sleep-ms 50 --write-concern majority --reset-queue

# 5) Wait for consumer to finish
$consumer.WaitForExit()

# 6) Print scenario summary metrics
Write-Host "`n=== Scenario summary from MongoDB ===" -ForegroundColor Green
$mongoSummaryScript = @'
const q = db.events_queue;
const r = db.events_results;
print('queue total:', q.countDocuments());
print('state NEW:', q.countDocuments({state:'NEW'}));
print('state PROCESSING:', q.countDocuments({state:'PROCESSING'}));
print('state DONE:', q.countDocuments({state:'DONE'}));
print('results total:', r.countDocuments());
const p = r.aggregate([
  {$group:{_id:null, avg:{$avg:'$end_to_end_ms'}, min:{$min:'$end_to_end_ms'}, max:{$max:'$end_to_end_ms'}}}
]).toArray();
if (p.length) {
  print('end_to_end_ms avg:', p[0].avg.toFixed(2), 'min:', p[0].min.toFixed(2), 'max:', p[0].max.toFixed(2));
}
'@
docker exec upu-db-1 mongosh hospital --quiet --eval $mongoSummaryScript | Out-Host

Write-Host "=== Scenario complete ===" -ForegroundColor Green
