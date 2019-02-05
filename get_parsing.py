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

g_cache = '/home/fi11222/disk-partage/Dev/Shatapatha Brahmana/TMP/'

g_delete = False


# ---------------------------------------------------- Functions -------------------------------------------------------
def save_parsing(
        p_db_connection, p_id, p_page):
    """
    Saves a verse to the database

    :param p_db_connection: active database connexion
    :param p_id: Verse ID
    :param p_page: HTML page returned by INRIA website
    :return: nothing
    """

    l_cursor_w = p_db_connection.cursor()
    try:
        l_cursor_w.execute("""
                insert into 
                    "TB_PARSING"(
                        "ID_VERSE"
                        , "TX_PARSING_HTML"
                        , "N_LENGTH"
                    )
                    values( %s, %s, %s );
            """, (p_id, p_page, len(p_page)))

        p_db_connection.commit()
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


# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| Get parsing of Sanskrit text from INRIA site               |')
    print('|                                                            |')
    print('| get_parsing.py                                             |')
    print('|                                                            |')
    print('| v. 1.0 - 29/01/2019                                        |')
    print('+------------------------------------------------------------+')

    empty_folder(g_cache)

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
            print('-------------------------', l_id, '-------------------------')
            print(l_original)
            print(l_text)

            # text cleanup
            l_text = re.sub(r'[*/;!?><^=\[\]\\]', '', l_text)
            l_text = re.sub(r'{[^}]+}]', '', l_text)

            # `
            l_text = re.sub(r'`', "'", l_text)

            for l_iast, l_vel in g_iast_to_velthuis:
                l_text = re.sub(l_iast, l_vel, l_text)

            l_text = re.sub(r'\s+', ' ', l_text).strip()
            l_urlencoded = urllib.parse.quote_plus(l_text)

            print(l_text)
            print(l_urlencoded)
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
                            print('URLError captured:', repr(eu))
                            print('Aborting ...')
                            break

                    if l_retries < 5:
                        print('Request timeout. Waiting for five seconds')
                        time.sleep(5)
                        l_retries += 1
                        l_timeout += 3
                        print('Retrying ({0}) timeout = {1} s.  ...'.format(l_retries, l_timeout))
                    else:
                        print('Stop trying')
                        break

            if l_page == '':
                continue

            print('-->', len(l_page))

            l_page_html = l_page
            # <span class=""red"">Wrong input </span><span class=""blue"">Undefined token : ></span>
            if len(l_page) < 4000:
                l_match = re.search(
                    r'<span\sclass="red">Wrong\sinput\s*</span><span\sclass="blue">([^<]+)</span>',
                    l_page)
                if l_match:
                    l_page = '*** ERROR *** : ' + l_match.group(1)

            save_parsing(l_db_connection, l_id, l_page)
            l_path_cache = os.path.join(g_cache, '{0}.html'.format(l_id))
            with open(l_path_cache, 'w', encoding='utf8') as l_fo:
                l_fo.write(l_page_html)

    except Exception as e:
        print('DB ERROR:', repr(e))
        print(l_cursor_read.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read.close()
