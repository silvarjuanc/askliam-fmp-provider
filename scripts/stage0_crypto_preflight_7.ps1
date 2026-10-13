# AskLiam V6.5.1 — seven-asset, read-only native-perp Stage0 preflight.
# Does not call ANY write endpoint. No secrets are stored, emitted or logged.
# Compatible with Windows PowerShell 5.1+ and PowerShell 7+.
# Usage: .\scripts\stage0_crypto_preflight_7.ps1 -Headers $headers
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [System.Collections.IDictionary]$Headers,
    [ValidateRange(5,60)][int]$PauseSeconds=15,
    [ValidatePattern('^https://')][string]$BaseUri='https://askliam-fmp-provider.onrender.com',
    [string]$CsvPath=''
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$base=$BaseUri.TrimEnd('/')
$symbols=@('ADA','DOGE','ETH','LINK','SOL','SUI','XRP')
$results=New-Object System.Collections.ArrayList
$day=[DateTime]::UtcNow.ToString('yyyy-MM-dd')
$expectedRun='STAGE0-' + [DateTime]::UtcNow.ToString('yyyyMMdd') + '-V6.5.1'

function Assert-True([bool]$Condition,[string]$Reason) {
    if(-not $Condition) { throw $Reason }
}

try {
    Assert-True ($Headers.Contains('X-Askliam-Token')) 'Missing X-Askliam-Token request header.'
    Assert-True (-not [string]::IsNullOrWhiteSpace([string]$Headers['X-Askliam-Token'])) 'Empty authorization header.'

    # Redis-only authorized GET: verifies the real runtime write switch.
    $audit=Invoke-RestMethod -Uri "$base/stage0/crypto/checkpoint?day=$day&symbol=BTC" -Method GET -Headers $Headers -TimeoutSec 60
    Assert-True ($audit.version -eq 'V6.5.1') 'Invalid engine version.'
    Assert-True ($audit.crypto_write_enabled -eq $false) 'STOP: crypto write permission is ENABLED.'
    Assert-True ($audit.pending_reconciliation -eq $false) 'STOP: reconciliation is pending.'
    Assert-True ($audit.symbol_checkpoint_verified -eq $true) 'STOP: BTC checkpoint not confirmed.'
    Assert-True ($audit.last_verified_run_id -eq $expectedRun) 'STOP: BTC checkpoint not from the expected UTC day.'

    $state=Invoke-RestMethod -Uri "$base/stage0/batch/status" -Method GET -Headers $Headers -TimeoutSec 60
    Assert-True ($state.version -eq 'V6.5.1') 'Invalid Stage0 batch version.'
    Assert-True ($state.pending_reconciliation -eq $false) 'STOP: Stage0 global pending journal.'

    $plan=Invoke-RestMethod -Uri "$base/stage0/daily/plan" -Method GET -Headers $Headers -TimeoutSec 60
    Assert-True ($plan.version -eq 'V6.5.1') 'Invalid Stage0 plan version.'
    Assert-True ($plan.pending_reconciliation -eq $false) 'STOP: new global pending journal.'
    Assert-True ([int]$plan.universe_enabled_count -eq 169) 'STOP: enabled universe changed; review before preflight.'

    Write-Host "Stage0 V6.5.1 | $day UTC | BTC verified | writes disabled" -ForegroundColor Green

    foreach($symbol in $symbols) {
        Assert-True ([DateTime]::UtcNow.ToString('yyyy-MM-dd') -eq $day) 'STOP: UTC date rollover.'

        # Exactly one read-only preview per asset. Each issues three
        # Hyperliquid Info reads plus three-tab read-only Sheets comparison.
        $uri="$base/stage0/crypto/sheet-dry-run?symbol=$symbol"
        $r=Invoke-RestMethod -Uri $uri -Headers $Headers -Method GET -TimeoutSec 120

        Assert-True ($r.version -eq 'V6.5.1') "$symbol : version mismatch."
        Assert-True ($r.symbol -eq $symbol) "$symbol : symbol mismatch."
        Assert-True ($r.run_id_preview_only -eq $expectedRun) "$symbol : run ID mismatch."
        Assert-True ($r.status -eq 'SHEET_DRY_RUN_PREPARED') "$symbol : not prepared; status=$($r.status)"
        Assert-True ($r.reader_only -eq $true) "$symbol : no read-only proof."
        Assert-True ($r.source -eq 'HYPERLIQUID_NATIVE_PERP') "$symbol : unexpected provider."
        Assert-True ([int]$r.requests_performed -eq 3) "$symbol : expected exactly three Hyperliquid calls."
        Assert-True ($r.sheet_writes_performed -eq $false) "$symbol : sheet write flag unexpectedly true."
        Assert-True ($r.checkpoint_advanced -eq $false) "$symbol : checkpoint advanced."
        Assert-True ($r.ranking_permitted -eq $false) "$symbol : ranking permitted."
        Assert-True ($r.execution_permitted -eq $false) "$symbol : execution permitted."
        Assert-True ($r.fmp_requests_performed -eq $false) "$symbol : FMP was queried."

        $prep=$r.prepared
        $sc=$r.sheet_comparison
        Assert-True ($prep.canonical_symbol -eq $symbol) "$symbol : prepared asset mismatch."
        Assert-True ($prep.source -eq 'HYPERLIQUID_NATIVE_PERP') "$symbol : raw source mismatch."
        Assert-True ($prep.history_kind -eq 'HYPERLIQUID_NATIVE_PERPETUAL') "$symbol : not native perpetual."
        Assert-True ([int]$prep.daily_rows_prepared -eq 150) "$symbol : daily bars not 150."
        Assert-True ([int]$prep.upsert_unique_keys -eq 150) "$symbol : duplicate history keys."
        Assert-True ([int]$prep.snapshot_rows_prepared -eq 1) "$symbol : unexpected snapshots."
        Assert-True ($prep.independent_spot_acquired -eq $false) "$symbol : spot classification changed."
        Assert-True ($prep.mandatory_broad_pass -eq $false) "$symbol : false ranking readiness."
        Assert-True ($prep.sheet_writes_performed -eq $false) "$symbol : prepared writes enabled."
        Assert-True ($prep.checkpoint_advanced -eq $false) "$symbol : prepared checkpoint advanced."
        Assert-True (-not [string]::IsNullOrWhiteSpace([string]$prep.ohlcv_sha256)) "$symbol : missing dataset hash."

        Assert-True ($sc.status -eq 'SHEET_DRY_RUN_PREPARED') "$symbol : sheet comparison invalid."
        Assert-True ($sc.snapshot_collision -eq $false) "$symbol : snapshot collision."
        Assert-True ($sc.approved_tabs_only -eq $true) "$symbol : unapproved Sheet tab."
        Assert-True ($sc.sheet_writes_performed -eq $false) "$symbol : Sheet mutated."
        Assert-True ($sc.checkpoint_advanced -eq $false) "$symbol : checkpoint mutated."
        $p=$sc.plans
        Assert-True ([int]$p.Raw_OHLCV.inserts -eq 150) "$symbol : raw inserts no longer exactly 150."
        Assert-True ([int]$p.Raw_OHLCV.updates -eq 0) "$symbol : existing raw updates scheduled."
        Assert-True ([int]$p.Market_Data_Snapshot.inserts -eq 1) "$symbol : snapshot already exists."
        Assert-True ([int]$p.Market_Data_Snapshot.updates -eq 0) "$symbol : snapshot updates scheduled."
        Assert-True ([int]$p.FMP_Runtime_State.inserts -eq 0) "$symbol : FMP state inserts scheduled."
        Assert-True ([int]$p.FMP_Runtime_State.updates -eq 0) "$symbol : FMP state updates scheduled."

        [void]$results.Add([pscustomobject]@{
            utc_day=$day; symbol=$symbol; status='SHEET_DRY_RUN_PREPARED'
            history_source=[string]$prep.source
            history_kind=[string]$prep.history_kind
            first_date_utc=[string]$prep.first_date_utc
            last_date_utc=[string]$prep.last_date_utc
            rows=[int]$prep.daily_rows_prepared
            history_inserts=[int]$p.Raw_OHLCV.inserts
            history_updates=[int]$p.Raw_OHLCV.updates
            snapshot_inserts=[int]$p.Market_Data_Snapshot.inserts
            fmp_changes=([int]$p.FMP_Runtime_State.inserts + [int]$p.FMP_Runtime_State.updates)
            sha256=[string]$prep.ohlcv_sha256
            writes=$false; ranking=$false; execution=$false
        })
        Write-Host "$symbol : READ-ONLY PASS (150 new history rows, 1 snapshot)" -ForegroundColor Green
        if($symbol -ne $symbols[-1]) { Start-Sleep -Seconds $PauseSeconds }
    }
    $results | Format-Table symbol,status,rows,history_inserts,history_updates,snapshot_inserts,fmp_changes,writes -AutoSize
    Write-Host 'ALL 7 VALIDATED: PREPARATION ONLY. NO IMPORTS COMMITTED.' -ForegroundColor Green
}
catch {
    Write-Warning ("STOP: "+$_.Exception.Message)
    Write-Warning 'NO POST, NO AUTOMATIC RETRY. No asset was imported by this script.'
}
finally {
    if($CsvPath -and $results.Count -gt 0) {
        $results | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
        Write-Host "Read-only results: $CsvPath"
    }
}
