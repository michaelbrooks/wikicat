"""
This file knows how to parse DBpedia N-Triple data files
and return the results record-by-record.

Contains code from rdflib, a python library
for processing RDF. This includes an N-Triples parser:
https://github.com/RDFLib/rdflib/blob/master/rdflib/plugins/parsers/ntriples.py
"""

import re

# a no-op for Python 2
# see https://github.com/RDFLib/rdflib/blob/master/rdflib/py3compat.py
def b(s): return s

uriref = b(r'<([^:]+:[^\s"<>]+)>')
literal = b(r'"([^"\\]*(?:\\.[^"\\]*)*)"')
litinfo = b(r'(?:@([a-z]+(?:-[a-z0-9]+)*)|\^\^') + uriref + b(r')?')

r_line = re.compile(b(r'([^\r\n]*)(?:\r\n|\r|\n)'))
r_wspace = re.compile(b(r'[ \t]*'))
r_wspaces = re.compile(b(r'[ \t]+'))
r_tail = re.compile(b(r'[ \t]*\.[ \t]*'))
r_uriref = re.compile(uriref)
r_nodeid = re.compile(b(r'_:([A-Za-z][A-Za-z0-9]*)'))
r_literal = re.compile(literal + litinfo)

class ParseError(Exception):
    pass

quot = {b('t'): u'\t', b('n'): u'\n', b('r'): u'\r', b('"'): u'"', b('\\'):
    u'\\'}
r_safe = re.compile(b(r'([\x20\x21\x23-\x5B\x5D-\x7E]+)'))
r_quot = re.compile(b(r'\\(t|n|r|"|\\)'))
r_uniquot = re.compile(b(r'\\u([0-9A-F]{4})|\\U([0-9A-F]{8})'))

# if true, some extra stuff happens
validate = False

def unquote(s):
    """Unquote an N-Triples string."""
    if not validate:
        return s.decode('unicode-escape')
    else:
        result = []
        while s:
            m = r_safe.match(s)
            if m:
                s = s[m.end():]
                result.append(m.group(1).decode('ascii'))
                continue

            m = r_quot.match(s)
            if m:
                s = s[2:]
                result.append(quot[m.group(1)])
                continue

            m = r_uniquot.match(s)
            if m:
                s = s[m.end():]
                u, U = m.groups()
                codepoint = int(u or U, 16)
                if codepoint > 0x10FFFF:
                    raise ParseError("Disallowed codepoint: %08X" % codepoint)
                result.append(unichr(codepoint))
            elif s.startswith(b('\\')):
                raise ParseError("Illegal escape at: %s..." % s[:10])
            else:
                raise ParseError("Illegal literal character: %r" % s[0])
        return u''.join(result)

r_hibyte = re.compile(ur'([\x80-\xFF])')


def uriquote(uri):
    if not validate:
        return uri
    else:
        return r_hibyte.sub(
            lambda m: '%%%02X' % ord(m.group(1)), uri)

class NTripleParser(object):
    """
    An iterator that returns triples from
    an N-Triples file.
    """

    def __init__(self, file):
        self.iterator = file.__iter__()

    def parseline(self):
        self.eat(r_wspace)
        if (not self.line) or self.line.startswith(b('#')):
            return  # The line is empty or a comment

        subject = self.subject()
        self.eat(r_wspaces)

        predicate = self.predicate()
        self.eat(r_wspaces)

        object = self.object()
        self.eat(r_tail)

        if self.line:
            raise ParseError("Trailing garbage %s" % repr(self.line))

        return subject, predicate, object

    def peek(self, token):
        return self.line.startswith(token)

    def eat(self, pattern):
        m = pattern.match(self.line)
        if not m:  # @@ Why can't we get the original pattern?
            # print(dir(pattern))
            # print repr(self.line), type(self.line)
            raise ParseError("Failed to eat %s" % pattern)
        self.line = self.line[m.end():]
        return m

    def subject(self):
        # @@ Consider using dictionary cases
        subj = self.uriref() or self.nodeid()
        if not subj:
            raise ParseError("Subject must be uriref or nodeID")
        return subj

    def predicate(self):
        pred = self.uriref()
        if not pred:
            raise ParseError("Predicate must be uriref")
        return pred

    def object(self):
        objt = self.uriref() or self.nodeid() or self.literal()
        if objt is False:
            raise ParseError("Unrecognised object type")
        return objt

    def uriref(self):
        if self.peek(b('<')):
            uri = self.eat(r_uriref).group(1)
            uri = unquote(uri)
            uri = uriquote(uri)

            #return URI(uri)
            return uri

        return False

    def nodeid(self):
        if self.peek(b('_')):
            # Fix for https://github.com/RDFLib/rdflib/issues/204
            bnode_id = self.eat(r_nodeid).group(1).decode()

            # rdflib does some other stuff here to build a bNode object
            # we just want the string

            return bnode_id

        return False

    def literal(self):
        if self.peek(b('"')):
            lit, lang, dtype = self.eat(r_literal).groups()
            if lang:
                lang = lang.decode()
            else:
                lang = None
            if dtype:
                dtype = dtype.decode()
            else:
                dtype = None
            if lang and dtype:
                raise ParseError("Can't have both a language and a datatype")
            lit = unquote(lit)

            # we don't care much about the language and type
            #return Literal(lit, lang, dtype)

            return lit

        return False

    def next(self):
        """
        Get the next triple.
        :return:
        """

        triple = None
        # this loop skips blank lines and comments (where parseline() returns None)
        while triple is None:
            # this will raise a StopException if there are no more lines
            # remove the trailing newline
            self.line = self.iterator.next().strip()
            triple = self.parseline()

        return triple

    def __iter__(self):
        return self

def _test_stream():
    import nose.tools as nt

    import cStringIO as StringIO

    str = StringIO.StringIO("""
        # started 2013-07-10T03:11:48Z
        <http://dbpedia.org/resource/Category:Futurama> <http://www.w3.org/2000/01/rdf-schema#label> "Futurama"@en .
        <http://dbpedia.org/resource/Category:World_War_II> <http://www.w3.org/2000/01/rdf-schema#label> "World War II"@en .
        <http://dbpedia.org/resource/Category:Programming_languages> <http://www.w3.org/2000/01/rdf-schema#label> "Programming languages"@en .
        <http://dbpedia.org/resource/Category:Professional_wrestling> <http://www.w3.org/2000/01/rdf-schema#label> "Professional wrestling"@en .
        <http://dbpedia.org/resource/Category:Algebra> <http://www.w3.org/2000/01/rdf-schema#label> "Algebra"@en .
        <http://dbpedia.org/resource/Category:Anime> <http://www.w3.org/2000/01/rdf-schema#label> "Anime"@en .
        <http://dbpedia.org/resource/Category:Abstract_algebra> <http://www.w3.org/2000/01/rdf-schema#label> "Abstract algebra"@en .
        <http://dbpedia.org/resource/Category:Mathematics> <http://www.w3.org/2000/01/rdf-schema#label> "Mathematics"@en .
        <http://dbpedia.org/resource/Category:Linear_algebra> <http://www.w3.org/2000/01/rdf-schema#label> "Linear algebra"@en .
        <http://dbpedia.org/resource/Category:Calculus> <http://www.w3.org/2000/01/rdf-schema#label> "Calculus"@en .
        <http://dbpedia.org/resource/Category:Monarchs> <http://www.w3.org/2000/01/rdf-schema#label> "Monarchs"@en .
        <http://dbpedia.org/resource/Category:British_monarchs> <http://www.w3.org/2000/01/rdf-schema#label> "British monarchs"@en .
        <http://dbpedia.org/resource/Category:Star_Trek> <http://www.w3.org/2000/01/rdf-schema#label> "Star Trek"@en .
        <http://dbpedia.org/resource/Category:People> <http://www.w3.org/2000/01/rdf-schema#label> "People"@en .
        <http://dbpedia.org/resource/Category:Popes> <http://www.w3.org/2000/01/rdf-schema#label> "Popes"@en .
        <http://dbpedia.org/resource/Category:Desserts> <http://www.w3.org/2000/01/rdf-schema#label> "Desserts"@en .
        <http://dbpedia.org/resource/Category:Fruit> <http://www.w3.org/2000/01/rdf-schema#label> "Fruit"@en .
        <http://dbpedia.org/resource/Category:Lists> <http://www.w3.org/2000/01/rdf-schema#label> "Lists"@en .
        <http://dbpedia.org/resource/Category:Computer_science> <http://www.w3.org/2000/01/rdf-schema#label> "Computer science"@en .
        <http://dbpedia.org/resource/Category:The_Simpsons> <http://www.w3.org/2000/01/rdf-schema#label> "The Simpsons"@en .
        <http://dbpedia.org/resource/Category:Algorithms> <http://www.w3.org/2000/01/rdf-schema#label> "Algorithms"@en .
        <http://dbpedia.org/resource/Category:Data_structures> <http://www.w3.org/2000/01/rdf-schema#label> "Data structures"@en .
    """)

    expectation = [
        (u'http://dbpedia.org/resource/Category:Futurama', u'http://www.w3.org/2000/01/rdf-schema#label', u'Futurama'),
        (u'http://dbpedia.org/resource/Category:World_War_II', u'http://www.w3.org/2000/01/rdf-schema#label', u'World War II'),
        (u'http://dbpedia.org/resource/Category:Programming_languages', u'http://www.w3.org/2000/01/rdf-schema#label', u'Programming languages'),
        (u'http://dbpedia.org/resource/Category:Professional_wrestling', u'http://www.w3.org/2000/01/rdf-schema#label', u'Professional wrestling'),
        (u'http://dbpedia.org/resource/Category:Algebra', u'http://www.w3.org/2000/01/rdf-schema#label', u'Algebra'),
        (u'http://dbpedia.org/resource/Category:Anime', u'http://www.w3.org/2000/01/rdf-schema#label', u'Anime'),
        (u'http://dbpedia.org/resource/Category:Abstract_algebra', u'http://www.w3.org/2000/01/rdf-schema#label', u'Abstract algebra'),
        (u'http://dbpedia.org/resource/Category:Mathematics', u'http://www.w3.org/2000/01/rdf-schema#label', u'Mathematics'),
        (u'http://dbpedia.org/resource/Category:Linear_algebra', u'http://www.w3.org/2000/01/rdf-schema#label', u'Linear algebra'),
        (u'http://dbpedia.org/resource/Category:Calculus', u'http://www.w3.org/2000/01/rdf-schema#label', u'Calculus'),
        (u'http://dbpedia.org/resource/Category:Monarchs', u'http://www.w3.org/2000/01/rdf-schema#label', u'Monarchs'),
        (u'http://dbpedia.org/resource/Category:British_monarchs', u'http://www.w3.org/2000/01/rdf-schema#label', u'British monarchs'),
        (u'http://dbpedia.org/resource/Category:Star_Trek', u'http://www.w3.org/2000/01/rdf-schema#label', u'Star Trek'),
        (u'http://dbpedia.org/resource/Category:People', u'http://www.w3.org/2000/01/rdf-schema#label', u'People'),
        (u'http://dbpedia.org/resource/Category:Popes', u'http://www.w3.org/2000/01/rdf-schema#label', u'Popes'),
        (u'http://dbpedia.org/resource/Category:Desserts', u'http://www.w3.org/2000/01/rdf-schema#label', u'Desserts'),
        (u'http://dbpedia.org/resource/Category:Fruit', u'http://www.w3.org/2000/01/rdf-schema#label', u'Fruit'),
        (u'http://dbpedia.org/resource/Category:Lists', u'http://www.w3.org/2000/01/rdf-schema#label', u'Lists'),
        (u'http://dbpedia.org/resource/Category:Computer_science', u'http://www.w3.org/2000/01/rdf-schema#label', u'Computer science'),
        (u'http://dbpedia.org/resource/Category:The_Simpsons', u'http://www.w3.org/2000/01/rdf-schema#label', u'The Simpsons'),
        (u'http://dbpedia.org/resource/Category:Algorithms', u'http://www.w3.org/2000/01/rdf-schema#label', u'Algorithms'),
        (u'http://dbpedia.org/resource/Category:Data_structures', u'http://www.w3.org/2000/01/rdf-schema#label', u'Data structures'),
    ]

    parser = NTripleParser(str)

    parsed = 0
    for idx, triple in enumerate(parser):
        nt.eq_(expectation[idx], triple)
        parsed += 1

    nt.eq_(len(expectation), parsed)



if __name__ == "__main__":
    import sys

    try:
        _test_stream()
        print "Tests Passed"
    except AssertionError as e:
        print >> sys.stderr, "ERROR: TESTS FAILED"
        print >> sys.stderr, e

