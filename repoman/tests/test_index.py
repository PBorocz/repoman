import pytest
from pathlib import Path

from index import get_org_links, get_tags, get_last_mod


################################################################################
# Make sure that we can extract "Novoid-style" file tags and dates from paths
################################################################################
TAG_PATHS = (
    (Path("20211229 a Sample File name.pdf"), []),
    (Path("20211229 a Sample File name -- .pdf"), []),
    (Path("20211229 a Sample File name -- tag1 tag2 .pdf"), ["tag1", "tag2"]),
)
@pytest.mark.parametrize("path,result", TAG_PATHS)
def test_get_tags(path, result):
    for ith, tag in enumerate(get_tags(path)):
        assert tag in result
        result.remove(tag)
    assert not result, "Sorry, not all tags found!"

LMOD_PATHS = (
    (Path("2021-12-29 a Sample File name.pdf"), "2021-12-29 00:00"),
    (Path("2022-01-20T13:45 a Sample File name.pdf"), "2022-01-20 00:00"),
)
@pytest.mark.parametrize("path,result", LMOD_PATHS)
def test_get_last_mod(path, result):
    assert get_last_mod(path) == result


################################################################################
# Make sure we can extract org-links (and descriptions) from org text lines.
################################################################################
ORG_LINK_PATTERNS = (
    ("[[https://foo.bar.com]]",
     (("https://foo.bar.com", None),)),

    ("[[https://foo.bar.com][A Description]]",
     (("https://foo.bar.com", "A Description"),)),

    ("asdf asdf a [[https://foo.bar.com][A Description]] asdf asdf adsf",
     (            ("https://foo.bar.com", "A Description"),)),

    ("asdf [[https://foo.bar.com]] wqer   [[https://bar.foo.org][A Desc]] asdf ",
     (     ("https://foo.bar.com", None), ("https://bar.foo.org", "A Desc"),)),
)

@pytest.mark.parametrize("text,result", ORG_LINK_PATTERNS)
def test_get_org_links(text, result):

    links = get_org_links(Path("."), text)

    assert len(links) == len(result)
    for ith, (url, desc) in enumerate(links):
        assert url  == result[ith][0]
        assert desc == result[ith][1]
