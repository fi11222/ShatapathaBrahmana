#!/usr/bin/python3
# -*- coding: utf-8 -*-

import psycopg2
import sys
import re
import urllib.parse
import urllib.request
import os
import time
from socket import timeout

__author__ = 'Nicolas Reimen'

g_dbServer = 'localhost'
g_dbDatabase = 'ShatapathaBrahmana'
g_dbUser = 'postgres'
g_dbPassword = 'murugan!'

g_iast_to_velthuis = [
    ('ā', 'aa'),
    ('ī', 'ii'),
    ('ū', 'uu'),
    ('ṛ', '.r'),
    ('ṝ', '.rr'),
    ('ḷ', '.l'),
    ('ḹ', '.ll'),
    ('ṃ', '.m'),
    ('ḥ', '.h'),
    ('ṅ', '"n'),
    ('ñ', '~n'),
    ('ṭ', '.t'),
    ('ḍ', '.d'),
    ('ṇ', '.n'),
    ('ś', '"s'),
    ('ṣ', '.s'),
    ('ṁ', '.m')
]

g_cache = '/home/fi11222/disk-partage/Dev/ShatapathaBrahmana/TMP/'

g_delete = False

g_already_saved = dict()


# ---------------------------------------------------- Functions -------------------------------------------------------
def save_parsing(p_db_connection, p_id, p_page, p_begin, p_end):
    """
    Saves a verse to the database

    :param p_db_connection: active database connexion
    :param p_id: Verse ID
    :param p_page: HTML page returned by INRIA website
    :return: nothing
    """

    if p_id in g_already_saved.keys():
        l_segment = g_already_saved[p_id] + 1
    else:
        l_segment = 0

    l_cursor_w = p_db_connection.cursor()
    try:
        l_cursor_w.execute("""
                insert into 
                    "TB_PARSING"(
                        "ID_VERSE"
                        , "TX_PARSING_HTML"
                        , "N_LENGTH"
                        , "N_SEGMENT"
                        , "N_BEGIN"
                        , "N_END"
                    )
                    values( %s, %s, %s, %s, %s, %s );
            """, (p_id, p_page, len(p_page), l_segment, p_begin, p_end))

        p_db_connection.commit()
        g_already_saved[p_id] = l_segment
    except Exception as e0:
        p_db_connection.rollback()

        print('DB ERROR:', repr(e0))
        print(l_cursor_w.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_w.close()


def empty_folder(p_path):
    """
    Removes all files (not directories) from a folder

    :param p_path: pth to the folder to empty
    :return: nothing
    """
    for l_file in os.listdir(p_path):
        l_file_path = os.path.join(p_path, l_file)
        try:
            if os.path.isfile(l_file_path):
                os.unlink(l_file_path)
            # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e0:
            print(e0)


def fetch_parsing(p_db_connection, p_row_count, p_row_number, p_id, p_original, p_text, p_begin):
    """

    :param p_db_connection:
    :param p_row_count:
    :param p_row_number:
    :param p_id:
    :param p_original:
    :param p_text:
    :param p_begin:
    :return:
    """

    l_text0 = p_text
    print(
        '-------------------------',
        l_id, '[{0}/{1}]'.format(p_row_number, p_row_count),
        '-------------------------')
    print('O>', p_original)

    # 14;3;2;28
    # ===================================== date: fri, jul ṛṛ :: - (csthfrom: "v p lehmann" subject:
    # sbto: jgardner@blueveeguiovaedumime-version: status: rokṣ-status:
    l_text0 = re.sub(
        r'===================================== date: fri, jul ṛṛ :: - \(csthfrom: "v p lehmann" subject: ' +
        r'sbto: jgardner@blueveeguiovaedumime-version: status: rokṣ-status:', '', l_text0)

    # text cleanup
    l_text0 = re.sub(r'[:*/;!?><^=\[\]\\\-]', ' ', l_text0)
    l_text0 = re.sub(r'{[^}]+}', '', l_text0)

    # `
    l_text0 = re.sub(r'`', "'", l_text0)

    # pitq;n 12;9;3;12 12;9;3;15
    l_text0 = re.sub(r'q', 'ṛ', l_text0)

    # 'ṃ
    l_text0 = re.sub(r"'ṃ", "'m", l_text0)

    # rṃ
    l_text0 = re.sub(r'rṃ', 'ṛṃ', l_text0)

    # ḥḥ
    l_text0 = re.sub(r'ḥ+', 'ḥ', l_text0)

    # rasḥ
    l_text0 = re.sub(r'rasḥ', 'rasaḥ', l_text0)

    # tiṣṭhṃstaṃ 1;4;4;7
    l_text0 = re.sub(r'tiṣṭhṃstaṃ', 'tiṣṭhaṃstaṃ', l_text0)

    # mṃmayenaiva
    l_text0 = re.sub(r'mṃmayenaiva', 'maṃmayenaiva', l_text0)

    # rudr aṃ 3;2;4;20
    l_text0 = re.sub(r'rudr\saṃ', 'rudraṃ', l_text0)

    l_text0 = re.sub(r'\s+', ' ', l_text0).strip()
    print('I>', l_text0)

    for l_iast, l_vel in g_iast_to_velthuis:
        l_text0 = re.sub(l_iast, l_vel, l_text0)

    l_urlencoded = urllib.parse.quote_plus(l_text0)

    print('V>', l_text0)
    print('U>', l_urlencoded)
    # print(urllib.parse.urlencode(l_text))

    l_url = 'https://sanskrit.inria.fr/cgi-bin/SKT/sktgraph.cgi?' + \
            'lex=SH&st=t&us=f&cp=t&text={0}&t=VH&topic=&mode=g&corpmode=&corpdir=&sentno='.format(l_urlencoded)
    print(l_url)

    l_page = ''
    l_retries = 0
    l_timeout = 15
    while l_page == '':
        try:
            l_url_downloader = urllib.request.urlopen(l_url, timeout=l_timeout)
            l_bytes = l_url_downloader.read()

            l_page = l_bytes.decode("utf8")
            l_url_downloader.close()
        except (timeout, urllib.request.URLError) as eu:
            if isinstance(eu, urllib.request.URLError):
                if isinstance(eu.reason, timeout):
                    print('timeout captured through URLError:', repr(eu))
                else:
                    l_page = 'URLError captured: ' + repr(eu)
                    print(l_page)
                    print('Aborting ...')
                    break

            # if l_retries < 5:
            if l_retries < 2:
                print('Request timeout. Waiting for five seconds')
                time.sleep(5)
                l_retries += 1
                l_timeout += 5
                print('Retrying ({0}) timeout = {1} s.  ...'.format(l_retries, l_timeout))
            else:
                print('Request timeout. Not trying anymore. Attempting split')
                split_verse(p_db_connection, p_row_count, p_row_number, p_id, p_original, p_text, p_begin)
                break

    if l_page == '':
        return

    print('-->', len(l_page))

    l_page_html = l_page
    # <span class=""red"">Wrong input </span><span class=""blue"">Undefined token : ></span>
    if len(l_page) < 4000:
        l_match = re.search(
            r'<span\sclass="red">([^<]+)</span><span\sclass="blue">([^<]*)</span>',
            l_page)
        if l_match:
            l_error = l_match.group(1).strip() + ' - ' + l_match.group(2).strip()
            l_page = '*** ERROR *** : ' + l_error
            print(l_page)
            # "*** ERROR *** : Fatal error - index out of bounds"
            if l_error == 'Maximum input size exceeded - ' or l_error == 'Fatal error - index out of bounds':
                split_verse(p_db_connection, p_row_count, p_row_number, p_id, p_original, p_text, p_begin)

    save_parsing(p_db_connection, p_id, l_page, p_begin, p_begin + len(p_text) - 1)
    l_path_cache = os.path.join(g_cache, '{0}.html'.format(p_id))
    print('Saving to cache:', l_path_cache)
    with open(l_path_cache, 'w', encoding='utf8') as l_fo:
        l_fo.write(l_page_html)


def split_verse(p_db_connection, p_row_count, p_row_number, p_id, p_original, p_text, p_begin):
    """

    :param p_db_connection:
    :param p_row_count:
    :param p_row_number:
    :param p_id:
    :param p_original:
    :param p_text:
    :param p_begin:
    :return:
    """
    l_words = p_text.split()
    l_half_len = len(l_words) // 2
    if l_half_len > 2:
        print('Splitting ...')
        l_first_half = ' '.join(l_words[:l_half_len])
        fetch_parsing( p_db_connection, p_row_count, p_row_number, p_id, p_original, l_first_half, p_begin)
        fetch_parsing(p_db_connection, p_row_count, p_row_number, p_id, p_original,
                      ' '.join(l_words[l_half_len:]),
                      p_begin + len(l_first_half) + 1)
        # +1 because a space is lost between the 2 halves
    else:
        print('Too short to split. Saving as "No Response"')
        l_page0 = 'Segment: {0}\nNo Response'.format(p_text)
        save_parsing(p_db_connection, p_id, l_page0, p_begin, p_begin + len(p_text) - 1)


# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| Get parsing of Sanskrit text from INRIA site               |')
    print('|                                                            |')
    print('| get_parsing.py                                             |')
    print('|                                                            |')
    print('| v. 1.0 - 29/01/2019                                        |')
    print('+------------------------------------------------------------+')

    # empty_folder(g_cache)

    l_db_connection = psycopg2.connect(
        host=g_dbServer,
        database=g_dbDatabase,
        user=g_dbUser,
        password=g_dbPassword
    )

    if g_delete:
        # empty tables TB_PARSING and TB_WORDS
        l_cursor_write = l_db_connection.cursor()
        try:
            l_cursor_write.execute("""
                    delete from "TB_PARSING"
                """)

        except Exception as e:
            print('DB ERROR:', repr(e))
            print(l_cursor_write.query)
            sys.exit(0)
        finally:
            # release DB objects once finished
            l_cursor_write.close()

    l_cursor_read = l_db_connection.cursor()

    # Count number of rows
    l_row_count = 0
    try:
        l_cursor_read.execute("""
                select count(1) "COUNT"
                from "TB_SANSKRIT" "S" left outer join "TB_PARSING" "P" on "P"."ID_VERSE" = "S"."ID_VERSE" 
                where "P"."ID_VERSE" is null
            """)

        for l_count, in l_cursor_read:
            l_row_count = l_count
    except Exception as e:
        print('DB ERROR:', repr(e))
        print(l_cursor_read.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read.close()

    l_cursor_read = l_db_connection.cursor()
    l_row_number = 0
    try:
        l_cursor_read.execute("""
                select 
                    "S"."ID_VERSE"
                    , "S"."TX_VERSE"
                    , "S"."TX_VERSE_ORIGINAL"
                from "TB_SANSKRIT" "S" left outer join "TB_PARSING" "P" on "P"."ID_VERSE" = "S"."ID_VERSE" 
                where "P"."ID_VERSE" is null
                order by "S"."N_BOOK", "S"."N_CHAPTER", "S"."N_SECTION", "S"."N_VERSE"
            """)

        for l_id, l_text, l_original in l_cursor_read:
            fetch_parsing(l_db_connection, l_row_count, l_row_number, l_id, l_original, l_text, 0)
            l_row_number += 1

    except Exception as e:
        print('DB ERROR:', repr(e))
        print(l_cursor_read.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read.close()
