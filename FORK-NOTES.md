# Fork notes: Hummingbot client changes for Gateway on Ergo

This branch (`feat/ergo`) is Hummingbot v2.18.0 (upstream commit `3351adad0`) plus two commits: `89ab848da` and `669886ac1`. They change how the client's Gateway connector waits for, tracks and books swaps. The fork exists to be used with the Gateway fork that adds an Ergo chain (`feat/ergo` of the Gateway repository; see its `FORK-NOTES.md`).

This file is for someone who wants to run the client with that Gateway, or merge a new upstream release into this fork.

## 1. Why a stock client cannot track an Ergo swap

Two rules in the stock Gateway connector (`hummingbot/connector/gateway/gateway_base.py`) assume fast blocks and transactions that expire.

- **A fixed 120-second wait.** `SIGNATURE_POLL_TIMEOUT` and `TX_NOT_FOUND_DEADLINE` are both 120 seconds. The comments in the file tie them to Solana's blockhash window of about 90 seconds. Ergo blocks average 120 seconds (`blockTimeSeconds` in the Ergo network config). A swap can be mined after the client has stopped waiting.
- **Re-submission after "not found".** If a transaction is still NOT_FOUND when the wait ends, the stock client treats it as dropped and submits the swap again, and counts NOT_FOUND toward the order tracker's lost-order limit. That is safe only where an unseen transaction can never land later. An Ergo transaction has no expiry. One the node has lost may still be mined, so a second submission could trade twice.

The commits change both rules, add `ergo` to the client's chain list, send a price limit with swaps on connectors that enforce one, and book a fill from the amounts Gateway says the transaction fixes.

## 2. What the two commits change

`89ab848da`: track swaps to a final state on slow-block chains and bind their price.
`669886ac1`: book a swap from the amounts its transaction fixes, and read what Gateway states.

- **Chain list.** `ergo` is added to the `Chain` enum. A new function `known_chains()` returns those names followed by any other chain the running Gateway reports in `/config/chains`. The AMM data feed, Gateway pricing in the market data provider, market start-up and `gateway balance` with no chain named use it.
- **Waits from the network config.** If a network's Gateway config has `blockTimeSeconds` (and optionally `confirmations`), the swap's own poll and the NOT_FOUND deadline wait the longer of 120 seconds and `blockTimeSeconds x (confirmations + 5)`, capped at 3600 seconds. With the shipped Ergo mainnet config (120 s, 1 confirmation) that is 720 seconds. A network config that cannot be read at start is read again on status polls and new swaps, at most every 30 seconds.
- **No re-submission where "not found" proves nothing.** NOT_FOUND counts as proof of a dropped transaction only if the network config has been read, names no `blockTimeSeconds`, and does not say `transactionsExpire: false`. Otherwise the swap is never re-submitted, its hash is saved with the order as soon as Gateway names it, the order stays open, NOT_FOUND never counts toward the lost-order limit, and one warning is raised for an order still not found after the deadline. Such an order ends only on CONFIRMED or FAILED. If the reply to the submit call is lost on such a network, the order fails with the marker `SUBMIT_OUTCOME_UNKNOWN`; the order executor then does not place it again and the LP executor holds the withdrawn tokens.
- **Price limit.** An AMM swap on a connector that enforces one is sent `minAmountOut` (SELL) or `maxAmountIn` (BUY), computed from the order price and slippage, rounded to 15 significant digits and then to the quote token's decimals in the tightening direction (up for a SELL's minimum, down for a BUY's maximum). The order fails before anything is sent if the decimals are unknown or the limit cannot be written as a JSON number. A connector is in the set if it is `ergodex` (built in) or if Gateway states `binds_price_limit: true` for it in `/config/connectors`. A stated `false` never removes `ergodex`. No other connector is sent a limit.
- **Booking from `pendingData`.** If Gateway answers a swap PENDING with `pendingData`, the client keeps those amounts, with the transaction's signature, on the open order (saved with the order, so it survives a client restart). Nothing is booked from them until poll reports that same transaction confirmed. Then the fill is booked from those amounts and fee. A Gateway that sends none is handled as before.
- **Orders with no usable price.** The client asks Gateway for a quote alongside the submission and books a confirmed fill at that quote, or at price 0 with an error logged if none arrived. It used to book NaN.
- **Other.** `cancel()` on a Gateway swap order logs a warning and returns; the order stays open. A swap still unresolved when its own poll ends keeps the order open on its hash. `transactionsExpire: false` forbids re-submission and never allows one the older rule would not. The re-submission rule is read once per swap, before its first submit.

## 3. What stays identical for Solana and Ethereum, and what does not

Stay identical, when the client has read the network config: the 120-second waits and the re-submission rule. The stock Solana and Ethereum network templates in the Gateway fork carry neither `blockTimeSeconds` nor `confirmations` (checked in `src/templates/chains/solana/` and `ethereum/`). No price limit is sent to any connector other than `ergodex`, unless Gateway states `binds_price_limit: true` for it.

Change for every chain:

- A swap unresolved when its own poll ends keeps the order open on its hash instead of marking it failed. The status loop then resolves it.
- A timeout error that names its transaction is polled even when no retries remain. The LP close path can therefore wait up to the poll timeout.
- Order status results stay paired with their own order when some tracked orders have no hash yet.
- A failure raised after the status loop has already resolved the order is logged and ignored.
- A fill is never booked at NaN or a non-positive price; the quote fallback above applies to any order placed without a usable price.
- `cancel()` on a Gateway swap order no longer raises.
- Until a network config has been read, NOT_FOUND is taken to prove nothing, so an unreadable Solana or Ethereum config at start leads to no re-submission until it is read. Once it is read, the old rules apply.
- `known_chains()` always includes the enum members, so in `TradingCore` the Gateway chain test no longer falls back to a two-name set when Gateway has not reported. In the market data provider the Gateway-network test now matches `"<chain>-"` rather than a bare chain prefix. `gateway balance` with no chain named lists only chains Gateway has reported, once it has reported any.

## 4. Upstream files touched

Source files:

| File | Why |
|---|---|
| `hummingbot/connector/gateway/gateway_base.py` | Reads block timing and `transactionsExpire` from the network config; block-based waits; the rule for when NOT_FOUND proves a drop; `TransactionUnresolvedError`; polling a timeout's transaction before any retry; keeps orders paired with their hashes; no NaN fill price. |
| `hummingbot/connector/gateway/gateway.py` | Price limit computation and the connector set it applies to; keeps `pendingData` and books from it on confirmation; quote for orders with no usable price; handles an unresolved or possibly submitted swap; `cancel()` override. |
| `hummingbot/connector/gateway/gateway_in_flight_order.py` | A `pending_swap_data` field, saved to and restored from the order's JSON. |
| `hummingbot/connector/gateway/common_types.py` | Adds `ERGO` to `Chain`; adds `known_chains()`. |
| `hummingbot/core/gateway/gateway_http_client.py` | `execute_swap` takes `min_amount_out` and `max_amount_in`, sent as `minAmountOut` and `maxAmountIn` on the AMM route only (a `ValueError` on any other route). |
| `hummingbot/core/trading_core.py` | Uses `known_chains()` to recognise Gateway connector names. |
| `hummingbot/data_feed/amm_gateway_data_feed.py` | Uses `known_chains()` to check the network name. |
| `hummingbot/data_feed/market_data_provider.py` | Uses `known_chains()` to decide which connectors Gateway prices. |
| `hummingbot/client/command/gateway_command.py` | `gateway balance` with no chain named uses `known_chains()`. |
| `hummingbot/strategy_v2/executors/order_executor/order_executor.py` | Does not retry an order whose failure carries `SUBMIT_OUTCOME_UNKNOWN`. |
| `hummingbot/strategy_v2/executors/lp_executor/lp_executor.py` | A close-out swap with that marker is not placed again; the withdrawn tokens are held. |

Test files: `test/hummingbot/connector/gateway/test_gateway_base.py`, `test_gateway_http_client_swap_routes.py`, `test_gateway_known_chains.py` (new), `test_gateway_swap_reconciliation.py` (new); `test/hummingbot/core/test_trading_core.py`; `test/hummingbot/data_feed/test_amm_gateway_data_feed.py`, `test_market_data_provider.py`; `test/hummingbot/strategy_v2/executors/lp_executor/test_lp_executor.py`, `order_executor/test_gateway_slippage.py`.

## 5. What is proven and what is not

Proven:

- Unit tests, run in a container. At `669886ac1`, on the touched test files: 654 pass and 2 are skipped (615 and 2 after the first commit). `flake8` was clean after the first commit. These figures are from the commit messages.
- A read-only run of the patched client library against the forked Gateway, on 25 November 2026. It used the first commit (`89ab848da`), not `669886ac1`. The client library read chains, connectors, network status, token lists, balances, pool info and quotes for `ergo`; built a Gateway connector object for `ergodex/amm`; read the network timing (120 s blocks, 1 confirmation, 720-second waits); and computed price limits. It sent no swap and no wallet request. The Gateway in that run was also an earlier commit than the current Gateway head.

Not proven:

- No real swap has been tracked end to end. The status loop on a real transaction, the not-found handling over time, the lost-order limit and the restore of saved order state were not exercised against a live transaction.
- Strategies and executors other than through their unit tests, `TradingCore` start-up with trading on, and the application's own `gateway` commands were not exercised.
- Strategies other than the two built-in executors do not read the `SUBMIT_OUTCOME_UNKNOWN` marker and may place a possibly submitted order again (stated in the first commit message).
- A swap failed as possibly submitted has no hash. If it landed, its fill is not booked and the chain must be checked by hand.
- Where NOT_FOUND proves nothing, a transaction that was truly dropped leaves its order open.

Two behaviours seen in the read-only run that you should know about:

- A quote with only a pair named lets Gateway choose the pool, and `get_quote_price` sends no pool address, so a BUY and a SELL on the same pair can be priced on different pools.
- On a token with few decimals, the price limit can equal the quote. For SigUSD (2 decimals), a 1 percent tolerance on 1 ERG rounded to no tolerance in the run. The rounding is deliberate: a pool pays whole units, and tests in the second commit pin it.

## 6. Merging a new upstream release

The changes are in the Gateway connector files and a few callers, so conflicts are most likely in `gateway_base.py` and `gateway.py`.

1. `git fetch upstream --tags`. Choose the release tag.
2. Merge it into a working branch made from `feat/ergo`: `git merge <tag>`. `feat/ergo` is published on the fork remote at the same head as the local branch, so use a merge, not a rebase, which would need a force push.
3. Resolve conflicts in the files in section 4. Keep upstream's change and re-apply the fork's: the block-timing reads, `proves_dropped` passed through `_execute_with_retry` and `_await_signature_terminal`, the `known_chains()` callers, and the price-limit code.
4. Search the merged tree for new uses of the `Chain` enum or of a hard-coded chain list (`grep -rn "Chain\b\|GATEWAY_CHAINS\|'ethereum'" hummingbot`) and route any new one through `known_chains()`.
5. Run what upstream's CI runs (`.github/workflows/workflow.yml`): `pre-commit run --files <changed files>`, then `make test` and `make report_coverage`, then `diff-cover --compare-branch=origin/master --fail-under=80 coverage.xml` on a pull request. The CI environment is the conda environment from `setup/environment.yml`.
6. Run the Gateway test files in section 4 on their own and compare with 654 passed and 2 skipped. A different count is expected if upstream changed those files.
7. Check that the Gateway fork you pair it with still answers `binds_price_limit` in `/config/connectors`, `pendingData` on a PENDING swap, and `transactionsExpire` in the Ergo network config. The client reads all three.
