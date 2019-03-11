"""Port of the glob stdlib module, that takes a hook as an argument to
   support multiple file systems.
"""

import posixpath
import re
import fnmatch

# pylint: disable=too-many-branches,missing-docstring

__all__ = ["glob", "iglob", "escape"]

def glob(pathname, hook, recursive=False):
    """Return a list of paths matching a pathname pattern.

    The pattern may contain simple shell-style wildcards a la
    fnmatch. However, unlike fnmatch, filenames starting with a
    dot are special cases that are not matched by '*' and '?'
    patterns.

    If recursive is true, the pattern '**' will match any files and
    zero or more directories and subdirectories.
    """
    return list(iglob(pathname, hook=hook, recursive=recursive))

def iglob(pathname, hook, recursive=False):
    """Return an iterator which yields the paths matching a pathname pattern.

    The pattern may contain simple shell-style wildcards a la
    fnmatch. However, unlike fnmatch, filenames starting with a
    dot are special cases that are not matched by '*' and '?'
    patterns.

    If recursive is true, the pattern '**' will match any files and
    zero or more directories and subdirectories.
    """
    it = _iglob(pathname, recursive, False, hook=hook)
    if recursive and _isrecursive(pathname):
        s = next(it)  # skip empty string
        assert not s
    return it

def _iglob(pathname, recursive, dironly, hook):
    dirname, basename = posixpath.split(pathname)
    if not has_magic(pathname):
        assert not dironly
        if basename:
            if hook.exists(pathname):
                yield pathname
        else:
            # Patterns ending with a slash should match only directories
            if hook.isdir(dirname):
                yield pathname
        return
    if not dirname:
        if recursive and _isrecursive(basename):
            for entry in _glob2(dirname, basename, dironly, hook=hook):
                yield entry
        else:
            for entry in _glob1(dirname, basename, dironly, hook=hook):
                yield entry
        return
    # `posixpath.split()` returns the argument itself as a dirname if it is a
    # drive or UNC path.  Prevent an infinite recursion if a drive or UNC path
    # contains magic characters (i.e. r'\\?\C:').
    if dirname != pathname and has_magic(dirname):
        dirs = _iglob(dirname, recursive, True, hook=hook)
    else:
        dirs = [dirname]
    if has_magic(basename):
        if recursive and _isrecursive(basename):
            glob_in_dir = _glob2
        else:
            glob_in_dir = _glob1
    else:
        glob_in_dir = _glob0
    for dirname in dirs:
        for name in glob_in_dir(dirname, basename, dironly, hook=hook):
            yield posixpath.join(dirname, name)

# These 2 helper functions non-recursively glob inside a literal directory.
# They return a list of basenames.  _glob1 accepts a pattern while _glob0
# takes a literal basename (so it only has to check for its existence).

def _glob1(dirname, pattern, dironly, hook):
    names = list(_iterdir(dirname, dironly, hook=hook))
    if not _ishidden(pattern):
        names = (x for x in names if not _ishidden(x))
    return fnmatch.filter(names, pattern)

# pylint: disable=unused-argument
def _glob0(dirname, basename, dironly, hook):
    if not basename:
        # `posixpath.split()` returns an empty basename for paths ending with a
        # directory separator.  'q*x/' should match only directories.
        if hook.isdir(dirname):
            return [basename]
    else:
        if hook.exists(posixpath.join(dirname, basename)):
            return [basename]
    return []

# Following functions are not public but can be used by third-party code.

def glob0(dirname, pattern, hook):
    return _glob0(dirname, pattern, False, hook=hook)

def glob1(dirname, pattern, hook):
    return _glob1(dirname, pattern, False, hook=hook)

# This helper function recursively yields relative pathnames inside a literal
# directory.

def _glob2(dirname, pattern, dironly, hook):
    assert _isrecursive(pattern)
    yield pattern[:0]
    for entry in _rlistdir(dirname, dironly, hook=hook):
        yield entry

# If dironly is false, yields all file names inside a directory.
# If dironly is true, yields only directory names.
def _iterdir(dirname, dironly, hook):
    if not dirname:
        if isinstance(dirname, bytes):
            dirname = bytes(posixpath.curdir, 'ASCII')
        else:
            dirname = posixpath.curdir
    try:
        for entry in hook.listdir(dirname):
            if not dironly or hook.isdir(posixpath.join(dirname, entry)):
                yield entry
    except OSError:
        return

# Recursively yields relative pathnames inside a literal directory.
def _rlistdir(dirname, dironly, hook):
    names = list(_iterdir(dirname, dironly, hook=hook))
    for x in names:
        if not _ishidden(x):
            yield x
            path = posixpath.join(dirname, x) if dirname else x
            for y in _rlistdir(path, dironly, hook=hook):
                yield posixpath.join(x, y)


magic_check = re.compile('([*?[])')
magic_check_bytes = re.compile(b'([*?[])')

def has_magic(s):
    if isinstance(s, bytes):
        match = magic_check_bytes.search(s)
    else:
        match = magic_check.search(s)
    return match is not None

def _ishidden(path):
    return path[0] in ('.', b'.'[0])

def _isrecursive(pattern):
    if isinstance(pattern, bytes):
        return pattern == b'**'
    return pattern == '**'

def escape(pathname):
    """Escape all special characters.
    """
    # Escaping is done by wrapping any of "*?[" between square brackets.
    # Metacharacters do not work in the drive part and shouldn't be escaped.
    drive, pathname = posixpath.splitdrive(pathname)
    if isinstance(pathname, bytes):
        pathname = magic_check_bytes.sub(br'[\1]', pathname)
    else:
        pathname = magic_check.sub(r'[\1]', pathname)
    return drive + pathname
