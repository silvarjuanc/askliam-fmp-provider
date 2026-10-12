# AskLiam V6.5.1. Bounded, resumable FMP Stage0 runner (PowerShell 5.1+).
# Does not log headers, tokens, or source credentials.
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [System.Collections.IDictionary]$Headers,
    [ValidateRange(1,120)][int]$MaxSteps=20,
    [ValidateRange(2,300)][int]$PauseSeconds=5,
    [ValidateRange(40,239)][int]$MinimumRemainingQuota=45,
    [ValidatePattern('^https://')][string]$BaseUri='https://askliam-fmp-provider.onrender.com',
    [string]$CsvPath=''
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$base=$BaseUri.TrimEnd('/')
$attempted=@{}
$results=New-Object System.Collections.ArrayList

function Get-Stage0Status {
    Invoke-RestMethod -Uri "$base/stage0/batch/status" -Headers $Headers -Method GET -ErrorAction Stop
}
function Get-Stage0Plan {
    Invoke-RestMethod -Uri "$base/stage0/daily/plan" -Headers $Headers -Method GET -ErrorAction Stop
}

try {
    for($i=1; $i -le $MaxSteps; $i++) {
        $state=Get-Stage0Status
        if($state.version -ne 'V6.5.1') { throw 'Unexpected engine version' }
        if($state.pending_reconciliation) {
            Write-Warning 'STOP: Pending reconciliation requires inspection.'
            break
        }
        $remaining=[int]$state.quota.remaining
        if($state.quota.blocked -or $remaining -le $MinimumRemainingQuota) {
            Write-Warning "STOP: FMP quota reserve reached ($remaining)."
            break
        }
        $plan=Get-Stage0Plan
        if([int]$plan.universe_enabled_count -le 0 -or $plan.pending_reconciliation) {
            Write-Warning 'STOP: Universe unavailable or pending reconciliation.'
            break
        }
        if([int]$plan.fmp_remaining_today -le 0 -or @($plan.next_symbols).Count -eq 0) {
            Write-Host 'All FMP candidates have been attempted for today.'
            break
        }
        $symbol=[string]$plan.next_symbols[0]
        if([string]::IsNullOrWhiteSpace($symbol) -or $attempted.ContainsKey($symbol)) {
            Write-Warning "STOP: Queue failed to advance ($symbol)."
            break
        }
        $attempted[$symbol]=$true
        Write-Host ("Step {0}/{1}: {2} (quota remaining {3})" -f $i,$MaxSteps,$symbol,$remaining)

        $r=Invoke-RestMethod -Uri "$base/stage0/daily/step" -Headers $Headers -Method POST -ErrorAction Stop
        $outcome=''
        $reason=''
        $responseStatus=''
        if($null -ne $r.PSObject.Properties['status']) { $responseStatus=[string]$r.status }
        if($responseStatus -eq 'PROVIDER_DEFERRED') {
            if([string]$r.symbol -ne $symbol -or $r.checkpoint_advanced -ne $false) {
                throw 'Provider response inconsistent with expected symbol'
            }
            $outcome='PROVIDER_DEFERRED'
            $reason=[string]$r.reason
        } elseif($r.commit_status -eq 'COMMITTED' -and $r.verify_status -eq 'PASS' -and
                 $r.checkpoint_status -eq 'COMMITTED_VERIFY_PASS' -and
                 [string]$r.daily_symbol -eq $symbol) {
            $outcome='COMMITTED_VERIFY_PASS'
        } elseif($responseStatus -eq 'FMP_QUEUE_EMPTY') {
            Write-Host 'FMP queue empty.'
            break
        } else {
            Write-Warning "STOP: Unknown or retryable result for $symbol."
            break
        }
        [void]$results.Add([PSCustomObject]@{
            utc_time=[DateTime]::UtcNow.ToString('o')
            symbol=$symbol
            status=$outcome
            reason=$reason
        })
        Write-Host ("  {0} {1}" -f $outcome,$reason)
        if($i -lt $MaxSteps) { Start-Sleep -Seconds $PauseSeconds }
    }
} catch {
    $httpStatus=''
    if($_.Exception.Response) { $httpStatus=[int]$_.Exception.Response.StatusCode }
    Write-Warning "STOP: Request or validation failure. HTTP=$httpStatus. Do not blindly retry."
} finally {
    if($CsvPath -and $results.Count -gt 0) {
        $results | Export-Csv -Path $CsvPath -NoTypeInformation -Encoding UTF8
        Write-Host "Audit CSV: $CsvPath"
    }
    try {
        $s=Get-Stage0Status
        $p=Get-Stage0Plan
        [PSCustomObject]@{
            Version=$s.version
            Universe=$p.universe_enabled_count
            FMPVerified=$p.fmp_completed_today
            FMPDeferred=$p.fmp_deferred_today
            FMPRemaining=$p.fmp_remaining_today
            OtherRoutesPending=$p.unsupported_today
            QuotaRemaining=$s.quota.remaining
            PendingReconciliation=$s.pending_reconciliation
            NextSymbol=($p.next_symbols -join ', ')
            StepsThisInvocation=$results.Count
        } | Format-List
    } catch {
        Write-Warning 'Final status unavailable. Inspect read-only batch/status manually.'
    }
}
