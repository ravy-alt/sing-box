Base: SagerNet/sing-mux v0.3.10-0.20261110064256-375087483ea2.

Local additions expose optional session metadata capture/selection callbacks. sing-box uses them to attach the actual transport route to each multiplexed stream, including reused sessions. No wire-format changes.
