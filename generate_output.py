#!/usr/bin/python3
# -*- coding: utf-8 -*-

import psycopg2
import traceback
import sys
import os.path
import json
import re

__author__ = 'Nicolas Reimen'

g_dbServer = 'localhost'
g_dbDatabase = 'ShatapathaBrahmana'
g_dbUser = 'postgres'
g_dbPassword = 'murugan!'

g_output_path = '/home/fi11222/disk-partage/Dev/ShatapathaBrahmana/output/'

g_html_top = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{0}</title>
<link rel="stylesheet" type="text/css" href="output.css" media="screen,tv">
</head>
<body>
"""

g_html_bottom = """
</body></html>
"""


def get_words(p_db_connection, p_id):
    """
    Retrieve the word list for a given verse and generate the HTML output

    :param p_db_connection: live DB connection
    :param p_id: ID of the verse to retrieve
    :return: the HTML string
    """
    l_cursor_read0 = p_db_connection.cursor()
    l_html = '<table>\n'
    try:
        l_cursor_read0.execute("""
            select 
                "TX_WORD"
                , "TX_GRAMMAR"
                , "TX_LEXICON_SH"
                , "TX_LEMMA"
            from
                "TB_WORD"
            where
                "ID_VERSE" = %s
            order by "ID_WORD"
        """, (p_id, ))

        # print(l_cursor_read0.query.decode('utf8'))
        for l_word, l_grammar_json, l_lex_json, l_lemma_json in l_cursor_read0:
            l_grammar = json.loads(l_grammar_json)
            l_lexicon = json.loads(l_lex_json)
            l_lemma = json.loads(l_lemma_json)

            l_html += (
                '<tr><td class="tooltip">{0}' +
                '<span class="tooltiptext">{1}</span></td>\n').format(
                    l_word, ' / '.join(l_grammar))

            l_html += '<td>{0}</td>\n'.format('-'.join(l_lemma))

            # if len(l_grammar) == 1:
            #     l_html += '<td>{0}</td>\n'.format(l_grammar[0])
            # else:
            #    l_html += '<td><ul>\n'
            #     for l_gr in l_grammar:
            #        l_html += '<li>{0}</li>\n'.format(l_gr)
            #    l_html += '</ul></td>\n'

            l_lexicon = [re.sub(r'^([^\s]+)\s', r'<span class="lexword">\1</span> ', l_lex)
                         for l_lex in l_lexicon]

            if len(l_lexicon) == 1:
                l_html += '<td>{0}</td>\n'.format(l_lexicon[0])
            else:
                l_html += '<td><ul>\n'
                for l_lex in l_lexicon:
                    l_html += '<li>{0}</li>\n'.format(l_lex)
                l_html += '</ul></td>\n'
            l_html += '</tr>\n'

    except Exception as e:
        print('DB ERROR:', repr(e))
        traceback.print_exc()
        print(l_cursor_read0.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read0.close()

    l_html += '</table>\n'
    return l_html


# ---------------------------------------------------- Main section ----------------------------------------------------
if __name__ == "__main__":
    print('+------------------------------------------------------------+')
    print('| Outputs verse sheets                                       |')
    print('|                                                            |')
    print('| generate_output.py                                         |')
    print('|                                                            |')
    print('| v. 1.0 - 01/02/2019                                        |')
    print('+------------------------------------------------------------+')

    l_db_connection = psycopg2.connect(
        host=g_dbServer,
        database=g_dbDatabase,
        user=g_dbUser,
        password=g_dbPassword
    )

    l_cursor_read = l_db_connection.cursor()

    try:
        l_cursor_read.execute("""
            select 
                "S"."ID_VERSE"
                , "S"."N_BOOK"
                , "S"."N_CHAPTER"
                , "S"."N_SECTION"
                , "S"."N_VERSE"
                , "S"."TX_VERSE" 
                , "T"."TX_VERSE"
            from "TB_SANSKRIT" "S" left outer join "TB_TRANSLATION" "T" on 
                "T"."N_BOOK" = "S"."N_BOOK" 
                and "T"."N_CHAPTER" = "S"."N_CHAPTER" 
                and "T"."N_SECTION" = "S"."N_SECTION" 
                and "T"."N_VERSE" = "S"."N_VERSE"
            where "S"."ID_VERSE" in (143413, 143414, 143415)
        """)

        for l_id_verse, l_book, l_chapter, l_section, l_verse, l_skt_txt, l_trans_txt in l_cursor_read:
            print('------------------------', l_id_verse, '-------------------------------')
            print(l_skt_txt)
            print(l_trans_txt)

            l_file_name = '{0:02}_{1:02}_{2:02}_{3:03}.html'.format(l_book, l_chapter, l_section, l_verse)
            print(l_file_name)
            l_path = os.path.join(g_output_path, l_file_name)

            with open(l_path, 'w') as l_file_out:
                l_file_out.write(g_html_top.format(
                    '{0}.{1}.{2}.{3}'.format(l_book, l_chapter, l_section, l_verse)))
                l_file_out.write(
                    '<p class="verse_number">{0}:{1}:{2}:{3}</p>\n'.format(
                        l_book, l_chapter, l_section, l_verse))
                l_file_out.write('<p>{0}</p>\n'.format(l_skt_txt))
                l_file_out.write('<p>{0}</p>\n'.format(l_trans_txt))
                l_file_out.write(get_words(l_db_connection, l_id_verse))
                l_file_out.write(g_html_bottom)

    except Exception as e:
        print('DB ERROR:', repr(e))
        traceback.print_exc()
        print(l_cursor_read.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read.close()
