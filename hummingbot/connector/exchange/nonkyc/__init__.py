"""
NonKYC spot exchange connector.

Source: ported from NonKYCExchange/hummingbot, folder
hummingbot/connector/exchange/nonkyc/ at commit 4d9a98e1e (2026-02-20),
written for Hummingbot 2.12. That code is licensed under the Apache License,
Version 2.0, the same licence as Hummingbot. Copyright of the original code
stays with its authors.

This copy was changed for Hummingbot 2.18 and to repair the faults found by
two reviews before the port. Main changes: one fill path keyed by NonKYC's
trade id, a lookup by client id when an order request has an unknown outcome,
LIMIT orders only, local checks of the market rules, signatures over the exact
bytes sent, millisecond nonces, 0.2% default fee, and order book sequencing by
the book's own sequence number.
"""
