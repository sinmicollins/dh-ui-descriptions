"""Minimal streamlit stand-in so dq_common / data_quality_app import cleanly
outside `streamlit run`: widgets return their defaults, display calls are
no-ops, caching decorators pass through. requests / keyring / google.cloud
stay real — only the UI layer is faked.

Must be installed (install()) BEFORE importing dq_common or the apps.
"""

import sys
import types


class _Ctx:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _passthrough_cache(*args, **kwargs):
    """st.cache_data / st.cache_resource replacement: bare or parameterized."""
    def _wrap(fn):
        fn.clear = lambda: None
        return fn
    if args and callable(args[0]):
        return _wrap(args[0])
    return _wrap


class _Widgets:
    def _noop(self, *a, **k):
        return None

    set_page_config = markdown = write = caption = info = _noop
    success = warning = error = header = subheader = _noop
    dataframe = download_button = _noop

    def data_editor(self, data, **k):
        return data

    def expander(self, *a, **k):
        return _Ctx()

    def text_input(self, label, value="", **k):
        return value

    def text_area(self, label, value="", **k):
        return value

    def checkbox(self, label, value=False, **k):
        return value

    def selectbox(self, label, options, index=0, **k):
        options = list(options)
        return options[index] if options else None

    def radio(self, label, options, index=0, **k):
        options = list(options)
        return options[index] if options else None

    def button(self, *a, **k):
        return False

    def file_uploader(self, *a, **k):
        return None

    def columns(self, spec, **k):
        n = spec if isinstance(spec, int) else len(spec)
        return [_Ctx() for _ in range(n)]

    def spinner(self, *a, **k):
        return _Ctx()


def install():
    """Register the fake streamlit module (idempotent)."""
    existing = sys.modules.get("streamlit")
    if existing is not None and getattr(existing, "__fake__", False):
        return existing
    mod = types.ModuleType("streamlit")
    widgets = _Widgets()
    for name in dir(_Widgets):
        if not name.startswith("_"):
            setattr(mod, name, getattr(widgets, name))
    setattr(mod, "sidebar", _Widgets())
    setattr(mod, "session_state", {})
    setattr(mod, "cache_data", _passthrough_cache)
    setattr(mod, "cache_resource", _passthrough_cache)
    setattr(mod, "__fake__", True)
    sys.modules["streamlit"] = mod
    return mod
