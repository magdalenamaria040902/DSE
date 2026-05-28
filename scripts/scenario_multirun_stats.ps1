param(
    [int]$Runs = 5,
    [int]$Messages = 2000,
    [int]$BatchSize = 100,
    [int]$SleepMs = 50,
    [int]$IdleTimeoutS = 20
)

$ErrorActionPreference = "Stop"
[System.Threading.Thread]::CurrentThread.CurrentCulture = [System.Globalization.CultureInfo]::InvariantCulture
[System.Threading.Thread]::CurrentThread.CurrentUICulture = [System.Globalization.CultureInfo]::InvariantCulture
Set-Location "$PSScriptRoot\.."

$uri = "mongodb://localhost:27017/?directConnection=true"
$summaryCsv = "multirun_summary.csv"

"run,produced,done,results,avg_ms,min_ms,max_ms,producer_tput,consumer_tput" | Set-Content $summaryCsv

docker compose up -d | Out-Null
Start-Sleep -Seconds 4

for ($run = 1; $run -le $Runs; $run++) {
    Write-Host "=== Multi-run $run / $Runs ===" -ForegroundColor Green

    docker exec upu-db-1 mongosh hospital --quiet --eval "db.events_queue.drop(); db.events_results.drop();" | Out-Null

    $consumerLog = "consumer_run_$run.log"
    $producerLog = "producer_run_$run.log"

    $consumerArgs = "scripts\consumer.py --uri `"$uri`" --max-messages $Messages --idle-timeout-s $IdleTimeoutS --write-concern majority --worker-name consumer-$run"
    $consumer = Start-Process -FilePath python -ArgumentList $consumerArgs -NoNewWindow -RedirectStandardOutput $consumerLog -PassThru
    Start-Sleep -Seconds 1

    $producerArgs = "scripts\producer.py --uri `"$uri`" --messages $Messages --batch-size $BatchSize --sleep-ms $SleepMs --write-concern majority --reset-queue --worker-name producer-$run"
    $producer = Start-Process -FilePath python -ArgumentList $producerArgs -NoNewWindow -RedirectStandardOutput $producerLog -PassThru

    $producer.WaitForExit()
    $consumer.WaitForExit()

    $producerOut = Get-Content $producerLog -Raw
    $consumerOut = Get-Content $consumerLog -Raw

    $producerTput = ""
    if ($producerOut -match "throughput=([0-9.]+)\s+msg/s") {
        $producerTput = $matches[1]
    }
    $consumerTput = ""
    if ($consumerOut -match "throughput=([0-9.]+)\s+msg/s") {
        $consumerTput = $matches[1]
    }

    $mongoSummaryScript = @'
const q = db.events_queue; const r = db.events_results;
const agg = r.aggregate([
  {$group:{_id:null,avg:{$avg:'$end_to_end_ms'},min:{$min:'$end_to_end_ms'},max:{$max:'$end_to_end_ms'}}}
]).toArray();
const a = agg.length ? agg[0] : {avg:0,min:0,max:0};
const avg = (a.avg == null ? 0 : a.avg);
const min = (a.min == null ? 0 : a.min);
const max = (a.max == null ? 0 : a.max);
print(q.countDocuments() + ',' + q.countDocuments({state:'DONE'}) + ',' + r.countDocuments() + ',' + avg + ',' + min + ',' + max);
'@
    $dbSummary = docker exec upu-db-1 mongosh hospital --quiet --eval $mongoSummaryScript

    $summaryLine = ($dbSummary -split "`r?`n" | Where-Object { $_ -match '^\d+,\d+,\d+,' } | Select-Object -Last 1)
    if (-not $summaryLine) {
        throw "Could not parse Mongo summary output for run $run. Raw output:`n$dbSummary"
    }

    $parts = $summaryLine.Split(",")
    $produced = [int]$parts[0]
    $done = [int]$parts[1]
    $results = [int]$parts[2]
    $avgMs = [double]::Parse($(if ([string]::IsNullOrWhiteSpace($parts[3])) { "0" } else { $parts[3] }), [System.Globalization.CultureInfo]::InvariantCulture)
    $minMs = [double]::Parse($(if ($parts.Length -lt 5 -or [string]::IsNullOrWhiteSpace($parts[4])) { "0" } else { $parts[4] }), [System.Globalization.CultureInfo]::InvariantCulture)
    $maxMs = [double]::Parse($(if ($parts.Length -lt 6 -or [string]::IsNullOrWhiteSpace($parts[5])) { "0" } else { $parts[5] }), [System.Globalization.CultureInfo]::InvariantCulture)

    "$run,$produced,$done,$results,$avgMs,$minMs,$maxMs,$producerTput,$consumerTput" | Add-Content $summaryCsv
}

Write-Host "`n=== Multi-run summary saved to $summaryCsv ===" -ForegroundColor Green
Get-Content $summaryCsv | Out-Host
