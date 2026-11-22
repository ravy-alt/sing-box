Base: SagerNet/sing-quic v0.7.2-0.20261114063120-0979e001618d.

Hysteria, Hysteria2 and TUIC expose optional shared-session context/metadata hooks. These preserve URL-test classification across lifetime-owned offers and report the route of the exact reused session. Foreground cancellation never takes ownership of the shared connection attempt.

Hysteria and Hysteria2 also report each successful underlying UDP transport selection, including port hopping. Hysteria2 Realm family races retain independent metadata and publish only the authenticated winner. These hooks let sing-box follow the current transport without retaining discarded routes. No wire-format changes.
