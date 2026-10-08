"""Imports inside functions, per function, today (test_no_inline_imports)."""

EXPECTED_INLINE_IMPORT_VIOLATIONS: dict[str, int] = {
    'hyperscale/core/engines/client/udp/protocols/dtls/__init__.py::_prep_bins': 3,
    'hyperscale/core/engines/client/udp/protocols/dtls/demux/__init__.py::force_routing_demux': 1,
    'hyperscale/core/engines/client/udp/protocols/dtls/err.py::patch_ssl_errors': 1,
    'hyperscale/core/engines/client/udp/protocols/dtls/err.py::raise_as_ssl_module_error': 1,
    'hyperscale/core/engines/client/udp/protocols/dtls/patch.py::do_patch': 1,
    'hyperscale/core/engines/client/udp/protocols/dtls/sslconnection.py::SSLConnection._init_server': 1,
    'hyperscale/core/engines/client/udp/protocols/dtls/util.py::_BIO.__del__': 1,
    'hyperscale/core/engines/client/udp/protocols/dtls/util.py::_EC_KEY.__del__': 1,
    'hyperscale/core/engines/client/udp/protocols/dtls/wrapper.py::DtlsSocket._recvfrom_on_server_side': 1,
    'hyperscale/core/jobs/runner/local_server_pool.py::run_thread': 3,
}
