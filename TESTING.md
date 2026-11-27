# Testing Hummingbot on Ergo: instructions for testers

Written 27 November 2026, for the Gateway fork at commit `0bffbce7` or later and this client fork at commit `2bc84cd44` or later.

## Disclaimer: read this first

- **This is experimental, unaudited software.** Nobody outside the project has reviewed it. It may contain bugs that lose funds.
- **No transaction built by this code has been sent on mainnet yet.** Everything so far was proven with unit tests, with contract checks against recorded mainnet pool boxes, and with read-only runs against a mainnet node. You may be the first person to sign and send with it.
- **Testing means mainnet and real funds.** The Ergo testnet has no pools configured, so there is nothing to trade there.
- **Use a new wallet made only for this test, holding only what you can afford to lose** (about 3 ERG is enough). Never use your main wallet or its mnemonic.
- **LithosDex swaps are signed with an unreleased alpha build of the Ergo signing library** (`ergo-lib-wasm-nodejs@0.29.0-alpha-2f840d3`). No node has yet accepted a LithosDex swap signed this way.
- **The NonKYC connector has never been run with an API key.** If you test it, use a new key with trading only, no withdrawal right.
- This is not an official Hummingbot, Spectrum, LithosDex or NonKYC product, and none of them endorses it. It comes with no warranty and no support promise. Nothing here is financial advice. You run it at your own risk.

## What you are testing

Two repositories that work together. In both, the default branch is `feat/ergo`.

| Repository | What it is |
|---|---|
| `https://github.com/YOLO-Sigmachain/gateway` | Hummingbot Gateway v2.18.0 plus an Ergo chain (`ergo`) and one connector, `ergodex`. It swaps on Spectrum-style pools and LithosDex pools, and adds and removes liquidity on Spectrum-style pools. It builds the transaction, signs it with your wallet key and posts it to your own Ergo node. |
| `https://github.com/YOLO-Sigmachain/hummingbot` | Hummingbot client v2.18.0, changed so it can track swaps on a chain with 2-minute blocks. It also adds a NonKYC spot connector and a controller that arbitrages two Ergo pools of one pair. |

Each repository has a `FORK-NOTES.md` ([client](./FORK-NOTES.md), [Gateway](https://github.com/YOLO-Sigmachain/gateway/blob/feat/ergo/FORK-NOTES.md)) that says what changed, what is proven and what is not. Read its "proven and not proven" section before you send anything.

**You can test Gateway on its own**, with `curl`. That is stages 1 to 3 below and it is the most useful test right now. The client (stage 4) comes after.

## What you need

- **Your own Ergo mainnet node**, fully synced, with the extra index on (`ergo.node.extraIndex = true` in the node config) and caught up. Gateway uses no explorer and sends the node no API key. Check it:

  ```bash
  curl -s http://127.0.0.1:9053/info | jq '{fullHeight, headersHeight, stateType}'   # synced, stateType "utxo"
  curl -s http://127.0.0.1:9053/blockchain/indexedHeight                             # indexedHeight equals fullHeight
  ```

- **Node.js 22 or newer** and `pnpm`. The Gateway README still says Node 20; the fork's `package.json` requires 22.
- **Linux or macOS**, with `git`, `curl`, `jq` and a C++ build toolchain (see the Gateway README, "Prerequisites").
- For stage 4 only: Anaconda or Miniconda, and several GB of disk for the client's environment.
- A machine with at least 4 GB of free memory for the install and build. The running server used about 250 MB.

## Stage 1: install Gateway and read (no wallet, no funds)

```bash
git clone https://github.com/YOLO-Sigmachain/gateway.git
cd gateway
git log -1 --format=%h          # 0bffbce7 or later
pnpm install --frozen-lockfile
pnpm build
pnpm run setup:with-defaults    # copies the config templates into conf/
```

If your node is not at `http://127.0.0.1:9053`, set `nodeURL` in `conf/chains/ergo/mainnet.yml`.

Start the server. The passphrase encrypts the wallet file; choose a long random one and keep it out of your shell history:

```bash
read -s GATEWAY_PASSPHRASE && export GATEWAY_PASSPHRASE
START_SERVER=true node dist/index.js --dev
```

`--dev` is plain HTTP on `127.0.0.1:15888` only. If the port is busy, the server refuses to start and says so. Unlike upstream, it kills nothing.

In a second terminal:

```bash
G=http://127.0.0.1:15888
curl -s "$G/chains/ergo/status?network=mainnet" | jq .
curl -s "$G/tokens?chainNetwork=ergo-mainnet" | jq '.tokens[].symbol'
curl -s "$G/trading/amm/quote-swap?chainNetwork=ergo-mainnet&baseToken=ERG&quoteToken=RSN&amount=1&side=SELL" | jq .
curl -s "$G/trading/amm/quote-swap?chainNetwork=ergo-mainnet&baseToken=ERG&quoteToken=SigUSD&amount=1&side=BUY" | jq .
```

What to look at:

- `status` shows your node's current height.
- Each quote names the pool it used in `poolAddress`. Compare the price with what a DEX front end shows for the same pool and size.
- With no pool named, Gateway picks the best allowed pool for the size and direction. LithosDex pools are found on chain automatically. Pools charging more than 3% and ERG pools holding under 100 ERG are skipped. To quote one pool, add `&poolAddress=<pool NFT id>`.

A fresh config **cannot sign or send anything**. Every swap is refused with a 503 until you write a `submitMode` line into the config (stage 2). Stage 1 is safe to run as is.

## Stage 2: check mode (real wallet, nothing is sent)

In check mode Gateway builds and signs a real swap, and your node validates it, pool contract included. The node does not accept or relay it. No funds move.

1. Stop the server (Ctrl-C). The mode is changed only while it is stopped.
2. In `conf/chains/ergo/mainnet.yml`, add one top-level line: `submitMode: check`
3. Start the server again, make one read request, and confirm the mode in the server's output **before anything else**:

   ```bash
   curl -s "$G/chains/ergo/status?network=mainnet" > /dev/null
   ```

   The server must have printed a line that starts `ergo-mainnet.submitMode is check: signed transactions are only checked by the node, never sent`. If it says `send` or `not set`, stop. The same lines are in `logs/logs_gateway_app.log.<date>`:

   ```bash
   grep -h "submitMode is" logs/logs_gateway_app.log.* | tail -1
   ```

4. Add the test wallet. Type the mnemonic into a file in memory, not on the command line:

   ```bash
   umask 077
   nano /dev/shm/w.json
   #   {"chain":"ergo","privateKey":"word1 word2 ... word15","setDefault":true}
   curl -s -X POST $G/wallet/add -H 'content-type: application/json' --data-binary @/dev/shm/w.json | jq .
   shred -u /dev/shm/w.json
   ```

   The answer names the address. It is the first address of the mnemonic (`m/44'/429'/0'/0/0`), the same first address Nautilus shows. Check that it is the one you funded.

5. Read the balance, then check a swap:

   ```bash
   A=<that address>
   curl -s -X POST $G/chains/ergo/balances -H 'content-type: application/json' \
     -d "{\"network\":\"mainnet\",\"address\":\"$A\",\"tokens\":[\"ERG\",\"RSN\"]}" | jq .

   curl -s -w '\nHTTP %{http_code}\n' -X POST $G/trading/amm/execute-swap -H 'content-type: application/json' \
     -d '{"chainNetwork":"ergo-mainnet","baseToken":"ERG","quoteToken":"RSN","amount":1,"side":"SELL","slippagePct":1}'
   ```

| Answer | Meaning |
|---|---|
| **HTTP 409**, `Check only: the node accepted transaction <id> as valid; nothing was sent. If it were sent, it would move ...` | The node validated the signed swap. This is the result we hope for. |
| HTTP 400, `Check only, nothing was sent: Ergo node rejected ...` | The node refused it. Report the text. If it says the pool box is spent, someone traded first: run the check again. |
| HTTP 503 | No verdict, nothing sent. Report the text. |

ERG/RSN is used because RSN has 3 decimals. On SigUSD (2 decimals) a 1 ERG trade leaves no room for slippage and is refused when the pool moves at all; use 10 ERG or more there.

**The most valuable single test:** a check of a swap on a LithosDex pool. Quote a LithosDex pair (for example ERG/LIT), take the `poolAddress` from the quote, and run the same execute with `"quoteToken":"LIT"` and `"poolAddress":"<that id>"`. A 409 "Check only: the node accepted ..." would be the first proof that a node accepts a LithosDex swap signed with the alpha library. A 400 with the node's reason is just as useful.

## Stage 3: send one small swap each way (funds move)

Do this only after stage 2 gave a 409 for the same pair. Cost: two miner fees of 0.0011 ERG plus the pool fee on about 1 ERG each way.

1. Stop the server. Change the line to `submitMode: send`. Start, make one read request, and confirm the server printed `ergo-mainnet.submitMode is send: signed transactions are sent to the node`.
2. Send the same SELL as in stage 2. Expected: HTTP 200 with `"status":0` (pending), the transaction id in `signature`, and a `pendingData` block with the amounts the transaction fixes.
3. Poll until `txStatus` is 1. One block averages 2 minutes.

   ```bash
   T=<transaction id>
   curl -s -X POST $G/chains/ergo/poll -H 'content-type: application/json' \
     -d "{\"network\":\"mainnet\",\"signature\":\"$T\"}" | jq '{txStatus, txBlock, fee, note: .txData.note}'
   ```

   | `txStatus` | Meaning |
   |---|---|
   | 0 | Pending. Poll again. |
   | 1 | Confirmed. Compare your balance change with `pendingData`. |
   | -1 | Failed on proof: another transaction spent the pool box first and is 3 blocks deep. Nothing of yours moved. |
   | -2 | The node does not know it. This is not final. Poll again; the `note` says why. |

4. When it is confirmed, send the BUY (`"side":"BUY"`, same body) and poll it the same way.

Other answers at execute:

- **HTTP 409 "pool moved":** nothing was sent. Retry with a fresh quote.
- **HTTP 400:** the node refused it. Nothing was sent.
- **HTTP 504 with a transaction id:** the outcome is unknown. Poll that id. **Do not send the swap again**; an Ergo transaction has no expiry and may still confirm.

If anything in send mode surprises you, stop the server first and look afterwards. A transaction already submitted is recorded in `db/ergo/mainnet-pending.json` and is picked up again at the next start. When you are done, set the line back to `submitMode: check`.

Liquidity on Spectrum-style pools (`quote-liquidity`, `add`, `remove`, `position-info`) follows the same check-then-send order. The routes and their answers are in Gateway's `FORK-NOTES.md`, section 4.1.

## Stage 4: the Hummingbot client

Run this only with Gateway already working from stages 1 to 3, and still running. The install steps below are upstream's; the installed client application has not been run by us as a whole, only its test suites and its Gateway library.

```bash
git clone https://github.com/YOLO-Sigmachain/hummingbot.git
cd hummingbot
git log -1 --format=%h          # 2bc84cd44 or later
make install
conda activate hummingbot
make run                        # the interactive client
```

The client talks to Gateway on `localhost:15888` without SSL by default, which matches `--dev`.

In the client:

- `gateway status` and `gateway list` should show Gateway online and the `ergodex` connector.
- `gateway balance ergo` should show the balances of the wallet you added in stage 2.
- **A strategy names the network, not the DEX:** the connector name is `ergo-mainnet`. Gateway picks the venue.

What exists to test, smallest risk first:

| Item | How | Notes |
|---|---|---|
| Price feed | any script that quotes on `ergo-mainnet` | Read-only. |
| One order on a named pool | the order executor, with `pool_address` | The client waits up to 12 minutes for a swap to confirm and never re-sends one whose outcome is unknown. |
| Two-pool arbitrage | controller `ergo_pool_arb` (`controllers/generic/ergo_pool_arb.py`) | Needs `pool_a`, `pool_b` (two pool NFT ids of one pair) and `order_amount`. It buys on one pool, then sells on the other once the buy has confirmed. Has run only in unit tests. Client `FORK-NOTES.md`, section 2.3. |
| NonKYC | `connect nonkyc`, then the ten keyed checks | The checks are listed in client `FORK-NOTES.md`, section 2.4, under "What is still to check live". Limit orders only; minimum order is 1 USDT on ERG/USDT. Use a new trading-only key. |
| Exchange against pool | classic `amm_arb`, one side `ergo-mainnet`, the other `nonkyc` | Last. Read client `FORK-NOTES.md`, sections 2.2 and 2.4 first. |

Two things to know before you run any strategy:

- **A restart forgets safety state.** If the client is holding or has halted after a swap of unknown outcome, that state is in memory only. Check your wallet on chain before you restart.
- **A halted controller is waiting for you.** `ergo_pool_arb` and `amm_arb` stop and log an error when one side traded and the other did not. They do not fix it themselves.

## What to report

For each stage you ran, send to the person or group that gave you these instructions:

- the Gateway and client commits (`git log -1 --format=%h` in each);
- your node version, and whether `indexedHeight` equalled `fullHeight`;
- the exact request, the HTTP code and the full answer;
- transaction ids of anything you sent, and whether your balance change matched `pendingData`;
- these lines from the Gateway log (run in the Gateway folder):

  ```bash
  grep -h -E "submitMode is|check only|Check only|Submitted Ergo|rejected|refused|could not" logs/logs_gateway_app.log.* | tail -40
  ```

Text from the node is stripped of signature bytes before Gateway prints it.

**Never send your mnemonic, the Gateway passphrase, an exchange API key or secret, or any file from `conf/wallets/`.** Nobody on the project will ask for them.

## Known limits

- Not built: LithosDex liquidity, LithosDex order mode, pool creation, Dexy, the yolo chain.
- One Gateway process per `db/` folder. Two processes sharing it lose each other's pending transactions.
- A swap that failed as "possibly submitted" has no fill booked in the client. If it landed, you see it only on chain.
- The NonKYC ERG markets are thin (ERG/USDT traded about 2,800 ERG a day on 26 November). Keep orders small.
- The full lists are in each repository's `FORK-NOTES.md`: Gateway section 8, client section 5.
