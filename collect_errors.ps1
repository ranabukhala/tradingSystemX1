param(
    [int]    $Lines  = 1000,
    [string] $Since  = "",
    [string] $Output = ""
)

$Timestamp  = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$LogDir     = Join-Path $PSScriptRoot "logs"
$OutputFile = if ($Output) { $Output } else { Join-Path $LogDir "errors_$Timestamp.log" }
$LatestFile = Join-Path $LogDir "errors_latest.log"

$ErrorPattern = "error|exception|traceback|critical|fatal|failed|crash|killed|attributeerror|keyerror|valueerror|typeerror|importerror|assertionerror|no address|connection refused|timeout"

$Containers = @(
    "trading_admin_bot"
    "trading_admin_watchdog"
    "trading_connector_benzinga"
    "trading_connector_earnings"
    "trading_connector_fred"
    "trading_connector_polygon_news"
    "trading_connector_polygon_prices"
    "trading_execution_engine"
    "trading_finnhub_fundamentals"
    "trading_finnhub_news"
    "trading_finnhub_sentiment"
    "trading_finnhub_websocket"
    "trading_fmp_earnings"
    "trading_fmp_enrichment"
    "trading_fmp_sectors"
    "trading_fmp_technical"
    "trading_pipeline_deduplicator"
    "trading_pipeline_entity_resolver"
    "trading_pipeline_normalizer"
    "trading_position_monitor"
    "trading_pretrade_filter"
    "trading_redpanda"
    "trading_redpanda_console"
    "trading_regime_poller"
    "trading_signals_aggregator"
    "trading_signals_ai_summarizer"
    "trading_signals_telegram"
)

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

Write-Host ""
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "   Trading System - Error Log Collector" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Timestamp : $Timestamp" -ForegroundColor Gray
Write-Host "  Output    : $OutputFile" -ForegroundColor Gray
if ($Since) {
    Write-Host "  Since     : $Since" -ForegroundColor Gray
} else {
    Write-Host "  Lines     : last $Lines per service" -ForegroundColor Gray
}
Write-Host ""

$divider = "=" * 80
$subdiv  = "-" * 80

Set-Content -Path $OutputFile -Value "$divider`r`nTrading System Error Log`r`nGenerated : $Timestamp`r`n$divider`r`n" -Encoding UTF8

$totalErrors        = 0
$servicesWithErrors = 0
$servicesChecked    = 0
$servicesMissing    = 0

foreach ($container in $Containers) {

    $inspect = docker inspect $container 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  [--] $container - not found" -ForegroundColor DarkGray
        $servicesMissing++
        continue
    }

    $status = docker inspect --format="{{.State.Status}}" $container 2>$null

    if ($Since) {
        $rawLogs = docker logs $container --since $Since 2>&1
    } else {
        $rawLogs = docker logs $container --tail $Lines 2>&1
    }

    $errorLines = $rawLogs | Where-Object { $_ -match $ErrorPattern }
    $errorCount = ($errorLines | Measure-Object).Count

    $servicesChecked++

    if ($errorCount -eq 0) {
        Write-Host "  [OK] $container ($status)" -ForegroundColor Green
        continue
    }

    Write-Host "  [!!] $container ($status) - $errorCount error lines" -ForegroundColor Red

    $servicesWithErrors++
    $totalErrors += $errorCount

    $block  = "$subdiv`r`n"
    $block += "SERVICE: $container  |  STATUS: $status  |  ERRORS: $errorCount`r`n"
    $block += "$subdiv`r`n"
    $block += ($errorLines -join "`r`n")
    $block += "`r`n`r`n"

    Add-Content -Path $OutputFile -Value $block -Encoding UTF8
}

$summary  = "$divider`r`n"
$summary += "SUMMARY`r`n"
$summary += "  Services checked  : $servicesChecked`r`n"
$summary += "  With errors       : $servicesWithErrors`r`n"
$summary += "  Total error lines : $totalErrors`r`n"
$summary += "  Not found         : $servicesMissing`r`n"
$summary += "$divider`r`n"
Add-Content -Path $OutputFile -Value $summary -Encoding UTF8

Write-Host ""
Write-Host "  ------------------------------------------" -ForegroundColor DarkGray
Write-Host "  Services checked  : $servicesChecked"
Write-Host "  With errors       : " -NoNewline
if ($servicesWithErrors -gt 0) {
    Write-Host $servicesWithErrors -ForegroundColor Red
} else {
    Write-Host $servicesWithErrors -ForegroundColor Green
}
Write-Host "  Total error lines : " -NoNewline
if ($totalErrors -gt 0) {
    Write-Host $totalErrors -ForegroundColor Red
} else {
    Write-Host $totalErrors -ForegroundColor Green
}
if ($servicesMissing -gt 0) {
    Write-Host "  Not found         : $servicesMissing" -ForegroundColor DarkGray
}
Write-Host "  ------------------------------------------" -ForegroundColor DarkGray
Write-Host ""

if ($totalErrors -eq 0) {
    Write-Host "  All services clean - no errors found" -ForegroundColor Green
    Write-Host ""
}

Copy-Item -Path $OutputFile -Destination $LatestFile -Force

Write-Host "  Log saved : $OutputFile" -ForegroundColor Cyan
Write-Host "  Latest    : $LatestFile" -ForegroundColor Cyan
Write-Host ""
