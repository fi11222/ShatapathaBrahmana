#!/usr/bin/python3
# -*- coding: utf-8 -*-

import psycopg2
import sys
import urllib.request
import re
import os.path

__author__ = 'Nicolas Reimen'

g_dbServer = 'localhost'
g_dbDatabase = 'ShatapathaBrahmana'
g_dbUser = 'postgres'
g_dbPassword = 'murugan!'

# http://www.sacred-texts.com/hin/sbr/sbe12/sbe1263.htm
# http://www.sacred-texts.com/hin/sbr/sbe26/sbe2678.htm
# http://www.sacred-texts.com/hin/sbr/sbe41/sbe4166.htm
# http://www.sacred-texts.com/hin/sbr/sbe43/sbe4375.htm
# http://www.sacred-texts.com/hin/sbr/sbe44/sbe44124.htm
g_begin_end = [(1203, 1263), (2603, 2678), (4103, 4166), (4303, 4375), (44003, 44124)]

g_entities_replace = [
    ('&ucirc;', 'ū'),
    ('&#257;', 'ā'),
    ('&ntilde;', 'ñ'),
    ('&ocirc;', 'o'),
    ('&#0771;', ''),
    ('&Ucirc;', 'ū'),
    ('&uuml;', 'ū'),
    ('&icirc;', 'ī'),
    ('&acirc;', 'ā'),
    ('&#8216;', '\''),
    ('&Otilde;', 'o'),
    ('&amp;', '&'),
    ('&Icirc;', 'ī'),
    ('&#2365;', 'ऽ'),
    ('&#7749;', 'ṅ'),
    ('&#8217;', '\''),
    ('&euml;', 'e'),
    ('&Acirc;', 'ā')
]

g_italics_replace = {
    ('<I>G</I>', 'g'),
    ('<I>Kh</I>', 'kh'),
    ('<I>ri</I>', 'ṛ'),
    ('<I>s</I>', 'ṣ'),
    ('<I>g</I>', 'g'),
    ('<I>h</I>', 'ḥ'),
    ('<I>d</I>', 'ḍ'),
    ('<I>t</I>', 'ṭ'),
    ('<I>gh</I>', 'gh'),
    ('<I>li</I>', 'ḷ'),
    ('<I>r</I>', 'r'),
    ('<I>n</I>', 'ṇ'),
    ('<I>m</I>', 'ṃ'),
    ('<I>k</I>', 'k'),
    ('<I>K</I>', 'k'),
    ('<I>Ri</I>', 'ṛ'),
    ('<I>kh</I>', 'kh'),
    ('<I>dh</I>', 'ḍh'),
    ('<I>th</I>', 'ṭ'),
    ('<I>&ntilde;</I>', 'ñ'),
    ('<I>S</I>', 'ṣ')
}

g_cache = '/home/fi11222/disk-partage/Dev/ShatapathaBrahmana/cache/'


# ---------------------------------------------------- Functions -------------------------------------------------------
def save_verse(
        p_db_connection, p_book, p_chapter, p_section, p_verse_number, p_verse_text):
    """
    Saves a verse to the database

    :param p_db_connection: active database connexion
    :param p_book: Book number
    :param p_chapter: Chapter Number
    :param p_section: Section Number
    :param p_verse_number: Verse Number
    :param p_verse_text: Verse Text (IAST)
    :return:
    """

    l_cursor_w = p_db_connection.cursor()
    try:
        l_cursor_w.execute("""
                insert into 
                    "TB_TRANSLATION"(
                        "N_BOOK"
                        , "N_CHAPTER"
                        , "N_SECTION"
                        , "N_VERSE"
                        , "TX_VERSE"
                    )
                    values( %s, %s, %s, %s, %s );
            """, (p_book, p_chapter, p_section, p_verse_number, p_verse_text))

        p_db_connection.commit()
    except Exception as e0:
        p_db_connection.rollback()

        print('DB ERROR:', repr(e0))
        print(l_cursor_w.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_w.close()


# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| Get and Load Shatapatha Brahmana Translation               |')
    print('|                                                            |')
    print('| load_translation.py                                        |')
    print('|                                                            |')
    print('| v. 1.0 - 24/01/2019                                        |')
    print('+------------------------------------------------------------+')

    l_db_connection = psycopg2.connect(
        host=g_dbServer,
        database=g_dbDatabase,
        user=g_dbUser,
        password=g_dbPassword
    )

    # empty tables TB_PADAPATHA and TB_WORDS
    l_cursor_write = l_db_connection.cursor()

    try:
        l_cursor_write.execute("""
                delete from "TB_TRANSLATION"
            """)

    except Exception as e:
        print('DB ERROR:', repr(e))
        print(l_cursor_write.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_write.close()

    l_italics = []
    l_entities = []
    for l_begin, l_end in g_begin_end:
        l_book_index = str(l_begin)[:2]

        for l_index in range(l_begin, l_end+1):
            l_url = 'http://www.sacred-texts.com/hin/sbr/sbe{0}/sbe{1}.htm'.format(l_book_index, l_index)
            print(l_url)

            l_filename = l_url.split('/')[-1]
            print(l_filename)

            l_path_cache = os.path.join(g_cache, l_filename)
            if os.path.isfile(l_path_cache):
                with open(l_path_cache, 'r', encoding='utf8') as l_fo:
                    l_page = l_fo.read()
            else:
                l_url_downloader = urllib.request.urlopen(l_url)
                l_bytes = l_url_downloader.read()

                l_page = l_bytes.decode("utf8")
                l_url_downloader.close()

                with open(l_path_cache, 'w', encoding='utf8') as l_fo:
                    l_fo.write(l_page)

            print('length:', len(l_page))

            # <span class="margnote"><FONT COLOR="GREEN" SIZE="-1"><a name="1:1:1:1">1:1:1:1</A>
            l_page = re.sub(
                r'<span\sclass="margnote">' +
                r'<FONT COLOR="GREEN" SIZE="-1"><a name="(\d+:\d+:\d+:\d+)">\d+:\d+:\d+:\d+</A>',
                r'%%%\1\n',
                l_page)

            # <A NAME="page_4"><FONT SIZE=1 COLOR=GREEN>p. 4</FONT></A>
            l_page = re.sub(r'<A NAME="page_\d+"><FONT SIZE=1 COLOR=GREEN>p. \d+</FONT></A>', '', l_page)

            l_page = re.sub(r'&nbsp;', ' ', l_page)

            # <A NAME="fr_84"></A><A HREF="#fn_84"><FONT SIZE="1">2</FONT></A>
            l_page = re.sub(
                r'<A NAME="fr_\d+"></A><A HREF="#fn_\d+"><FONT SIZE="1">\d+</FONT></A>',
                '',
                l_page)

            l_page = '#_#_#' + l_page + '#=#=#'
            l_page = re.sub(r'#_#_#.*?%%%', '%%%', l_page, flags=re.DOTALL | re.MULTILINE)
            l_page = re.sub(r'<HR>.*?#=#=#', '', l_page, flags=re.DOTALL | re.MULTILINE)

            # <FONT SIZE="-1"> ... </FONT>
            l_page = re.sub(r'<FONT SIZE="-1">.*?</FONT>', '', l_page, flags=re.DOTALL | re.MULTILINE)

            l_italics += re.findall(r'<I>[^<]+</I>', l_page)
            l_entities += re.findall(r'&[^;]+;', l_page)

            for l_from, l_to in g_italics_replace:
                l_page = re.sub(l_from, l_to, l_page)
            for l_from, l_to in g_entities_replace:
                l_page = re.sub(l_from, l_to, l_page)

            # HTML tags removal
            l_page = re.sub(r'<[^>]+>', '', l_page, flags=re.DOTALL | re.MULTILINE)

            l_page = re.sub(r'\n\s+', '\n', l_page, flags=re.DOTALL | re.MULTILINE)

            # sys.exit(0)
            # print(l_page)
            l_page += '%%%99:0:0:0'
            l_book = 0
            l_chapter = 0
            l_section = 0
            l_verse = 0
            l_text = None
            for l_line in l_page.split('\n'):
                # print(l_line)
                l_match = re.search(r'%%%(\d+):(\d+):(\d+):(\d+)', l_line)
                if l_match:
                    if l_text is not None:
                        l_text = re.sub(r'\s+', ' ', l_text).strip()
                        save_verse(l_db_connection, l_book, l_chapter, l_section, l_verse, l_text)
                        l_text = None

                    l_book = l_match.group(1)
                    l_chapter = l_match.group(2)
                    l_section = l_match.group(3)
                    l_verse = l_match.group(4)
                else:
                    if l_text is None:
                        l_text = l_line
                    else:
                        l_text += ' ' + l_line

    print(set(l_entities))
    print(set(l_italics))
