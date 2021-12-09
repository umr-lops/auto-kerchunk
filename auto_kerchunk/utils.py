def parse_url(url):
    pattern = "://"
    if pattern not in url:
        return "file", url

    scheme, path = url.split(pattern)
    return scheme, path


def parse_dict_option(option):
    if not option:
        return {}
    return dict(item.split("=") for item in option.split(";"))
