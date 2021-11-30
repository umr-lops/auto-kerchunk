def parse_url(url):
    pattern = "://"
    if pattern not in url:
        return "", url

    scheme, path = url.split(pattern)
    return scheme, path
