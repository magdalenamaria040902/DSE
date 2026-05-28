param(
    [int[]]$ProducerCounts = @(1, 2, 4),
    [int[]]$ConsumerCounts = @(1, 2, 4),
    [int]$MessagesPerProducer = 1000,
    [int]$BatchSize = 100,
    [int]$SleepMs = 50,
    [int]$IdleTimeoutS = 20
)

$ErrorActionPreference = "Stop"
[System.Threading.Thread]::CurrentThread.CurrentCulture = [System.Globalization.CultureInfo]::InvariantCulture
[System.Threading.Thread]::CurrentThread.CurrentUICulture = [System.Globalization.CultureInfo]::InvariantCulture
Set-Location "$PSScriptRoot\.."

$uri = "mongodb://localhost:27017/?directConnection=true"
$summaryCsv = "concurrency_scaling_summary.csv"
"producers,consumers,messages_per_producer,total_target,done,results,avg_ms,min_ms,max_ms" | Set-Content $summaryCsv

docker compose up -d | Out-Null
Start-Sleep -Seconds 4

foreach ($pCount in $ProducerCounts) {
    foreach ($cCount in $ConsumerCounts) {
        Write-Host "=== Scaling run: producers=$pCount consumers=$cCount ===" -ForegroundColor Green

        docker exec upu-db-1 mongosh hospital --quiet --eval "db.events_queue.drop(); db.events_results.drop();" | Out-Null

        $totalTarget = $pCount * $MessagesPerProducer

        $consumers = @()
        for ($i = 1; $i -le $cCount; $i++) {
            $args = "scripts\consumer.py --uri `"$uri`" --max-messages $totalTarget --idle-timeout-s $IdleTimeoutS --write-concern majority --worker-name consumer-$i"
            $consumers += Start-Process -FilePath python -ArgumentList $args -NoNewWindow -PassThru
        }

        Start-Sleep -Seconds 1

        $producers = @()
        for ($i = 1; $i -le $pCount; $i++) {
            $resetFlag = if ($i -eq 1) { "--reset-queue" } else { "" }
            $args = "scripts\producer.py --uri `"$uri`" --messages $MessagesPerProducer --batch-size $BatchSize --sleep-ms $SleepMs --write-concern majority $resetFlag --worker-name producer-$i"
            $producers += Start-Process -FilePath python -ArgumentList $args -NoNewWindow -PassThru
        }

        foreach ($proc in $producers) { $proc.WaitForExit() }
        foreach ($proc in $consumers) { $proc.WaitForExit() }

        $mongoSummaryScript = @'
const q = db.events_queue; const r = db.events_results;
const agg = r.aggregate([
  {$group:{_id:null,avg:{$avg:'$end_to_end_ms'},min:{$min:'$end_to_end_ms'},max:{$max:'$end_to_end_ms'}}}
]).toArray();
const a = agg.length ? agg[0] : {avg:0,min:0,max:0};
const avg = (a.avg == null ? 0 : a.avg);
const min = (a.min == null ? 0 : a.min);
const max = (a.max == null ? 0 : a.max);
print(q.countDocuments({state:'DONE'}) + ',' + r.countDocuments() + ',' + avg + ',' + min + ',' + max);
'@
        $dbSummary = docker exec upu-db-1 mongosh hospital --quiet --eval $mongoSummaryScript

        $summaryLine = ($dbSummary -split "`r?`n" | Where-Object { $_ -match '^\d+,\d+,' } | Select-Object -Last 1)
        if (-not $summaryLine) {
            throw "Could not parse Mongo summary output for producers=$pCount consumers=$cCount. Raw output:`n$dbSummary"
        }

        $parts = $summaryLine.Split(",")
        $done = [int]$parts[0]
        $results = [int]$parts[1]
        $avgMs = [double]::Parse($(if ([string]::IsNullOrWhiteSpace($parts[2])) { "0" } else { $parts[2] }), [System.Globalization.CultureInfo]::InvariantCulture)
        $minMs = [double]::Parse($(if ($parts.Length -lt 4 -or [string]::IsNullOrWhiteSpace($parts[3])) { "0" } else { $parts[3] }), [System.Globalization.CultureInfo]::InvariantCulture)
        $maxMs = [double]::Parse($(if ($parts.Length -lt 5 -or [string]::IsNullOrWhiteSpace($parts[4])) { "0" } else { $parts[4] }), [System.Globalization.CultureInfo]::InvariantCulture)

        "$pCount,$cCount,$MessagesPerProducer,$totalTarget,$done,$results,$avgMs,$minMs,$maxMs" | Add-Content $summaryCsv
    }
}

Write-Host "`n=== Concurrency scaling summary saved to $summaryCsv ===" -ForegroundColor Green
Get-Content $summaryCsv | Out-Host
