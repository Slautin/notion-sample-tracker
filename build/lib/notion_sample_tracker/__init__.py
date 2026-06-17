def create_app(*args, **kwargs):
    from notion_sample_tracker.app import create_app as _create_app

    return _create_app(*args, **kwargs)


__all__ = ["create_app"]
