import re
import unicodedata


_SLUG_SUB = r'[\s.]+'

def slugify(
    line: str,
    pattern: re.Pattern[str] | None = _SLUG_SUB,
    max_len: int | None = 63,
) -> str:
    """
    Slugify a string. Run

    Example:
    >>> slugify(u'Héllø W.1')
    'hell-w-1'
    >>> slugify('My Cool Label')
    'my-cool-label'
    >>> slugify('foo_bar-123')
    'foo_bar-123'
    >>> len(slugify('X' * 200))
    63
    >>> len(slugify('X' * 200, max_len=2))
    2

    Args:
        line: the input String
        pattern: a regular expression pattern, describing forbidden characters
        max_len: optional,

    Returns:
        a transformed version of the input String
    """

    line = unicodedata.normalize('NFKD', line).encode('ascii', 'ignore').decode()
    line = line.strip().lower()

    # if a RegEx was provided, swap any matching characters to hyphens
    if pattern:
        line = re.sub(
            pattern,
            '-',
            line,
        )

    # if max_length was provided, truncate
    if max_len:
        return line[:max_len]

    return line