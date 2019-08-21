class Metadata:

    def __init__(self, timeout=None):
        self.timeout = timeout


def parse_metadata(config):
    if isinstance(config, dict):
        meta_dict = config.get("_metadata")
        if meta_dict is not None and isinstance(meta_dict, dict):
            return Metadata(timeout=meta_dict.get("timeout"))
    return Metadata()
