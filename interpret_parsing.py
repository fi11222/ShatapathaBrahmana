#!/usr/bin/python3
# -*- coding: utf-8 -*-

import psycopg2
import sys
import re
import traceback
import lxml.html
import html
import os
import urllib.request
import json

__author__ = 'Nicolas Reimen'

g_dbServer = 'localhost'
g_dbDatabase = 'ShatapathaBrahmana'
g_dbUser = 'postgres'
g_dbPassword = 'murugan!'

g_cache = '/home/fi11222/disk-partage/Dev/ShatapathaBrahmana/inria/'

g_verse_stop = False

g_conversion_mixed = [
    ('ऐ', 'ai'),
    ('औ', 'au'),
    ('ख', 'kh'),
    ('घ', 'gh'),
    ('छ', 'ch'),
    ('झ', 'jh'),
    ('ठ', 'ṭh'),
    ('ढ', 'ḍh'),
    ('थ', 'th'),
    ('ध', 'dh'),
    ('फ', 'ph'),
    ('भ', 'bh')
]


# ---------------------------------------------------- functions & classes ---------------------------------------------
def iast_2_mixed(p_string):
    """

    :param p_string:
    :return:
    """
    l_result = p_string

    for l_deva, l_iast in g_conversion_mixed:
        l_result = re.sub(l_iast, l_deva, l_result)

    return l_result


def get_lex(p_link_lex):
    """
    Get the list of lexical entries based on the list of links provided

    :param p_link_lex: list of links
    :return: list of lexical entries
    """
    l_lex0 = []
    for l_link in p_link_lex:
        l_file_name = l_link.split('/')[-1]
        l_file_name = re.sub(r'#.*$', '', l_file_name)

        l_path_cache = os.path.join(g_cache, l_file_name)
        if os.path.isfile(l_path_cache):
            with open(l_path_cache, 'r', encoding='utf8') as l_fo0:
                l_page = l_fo0.read()
        else:
            l_url_downloader = urllib.request.urlopen(l_link)
            l_bytes = l_url_downloader.read()

            l_page = l_bytes.decode("utf8")
            l_url_downloader.close()

            with open(l_path_cache, 'w', encoding='utf8') as l_fo0:
                l_fo0.write(l_page)

        # <a class="navy" name="a.mzavaada"><i><span class="trans12">a&#7747;&#347;av&#257;da</span></i></a>
        # [<a class="blue" href="58.html#vaada"><i><span class="trans12">v&#257;da</span></i></a>]
        # <a class="red" href="/cgi-bin/SKT/sktdeclin.cgi?q=a.mzavaada;g=Mas;font=roma">m.</a>
        # phil. doctrine de la partie et du tout.<p></p>
        # &#160;<a class="navy" name="a.mzahara">
        l_name = re.findall(r'#(.*)$', l_link)[0]

        # l_page = re.sub(r'^&#160;(<a\sclass="navy"\sname="[^"]+")>', '£-£-£-£\1', l_page)
        # l_page = re.sub(r'^<span class="deva" lang="sa">', '£-£-£-£', l_page)
        # l_page = re.sub(r'^<a name="bottom">', '£-£-£-£', l_page)
        # l_page = re.sub('bottom', '_-_-_-_-_', l_page)
        l_page = re.sub(r'\n&#160;(<a\sclass="navy"\sname="[^"]+">)', '\n' + r'__ENTRY_BOUNDARY__\1', l_page)
        l_page = re.sub(r'\n<span\sclass="deva"\slang="sa">', '\n__ENTRY_BOUNDARY__', l_page)
        l_page = re.sub(r'\n<a\sname="bottom">', '\n__ENTRY_BOUNDARY__', l_page)
        # print(l_page)

        l_pattern = r'<a\sclass="navy"\sname="{0}">(.*?)__ENTRY_BOUNDARY__'.format(l_name)
        try:
            l_lex_entry = re.findall(l_pattern, l_page, flags=re.DOTALL | re.MULTILINE)[0]
        except IndexError:
            print('*** LEX ENTRY NOT FOUND ***')
            print('p_link_lex:', p_link_lex)
            print('l_pattern: ', l_pattern)
            with open('lex_page.html', 'w') as l_lex_file:
                l_lex_file.write(l_page)
            l_lex0.append('Lexicon Entry Not Found')
            continue

        l_lex_entry = re.sub(r'<[^>]+>', '', l_lex_entry)
        l_lex_entry = re.sub(r'\s+', ' ', l_lex_entry).strip()
        l_lex_entry = html.unescape(l_lex_entry)

        l_lex0.append(l_lex_entry)

    return l_lex0


def save_sentence(p_db_connection, p_id, p_sentence, p_id_parsing, p_padapatha, p_inverted_table):
    """
    Saves the interpretation list to the DB

    :param p_db_connection: Live DB connection
    :param p_id: Verse ID
    :param p_sentence: list of sentence components
    :param p_id_parsing: ID in TB_PARSING
    :param p_padapatha: The padapatha recovered from the INRIA parsing
    :param p_inverted_table: inverted (col/row) of the INRIA table representation
    :return: nothing
    """
    l_cursor_w = p_db_connection.cursor()
    try:
        for l_begin0, l_end0, l_word0, l_lemma0, l_grammar0, l_lex0 in p_sentence:
            l_cursor_w.execute("""
                        insert into 
                            "TB_WORD"(
                                "ID_VERSE"
                                , "TX_WORD"
                                , "TX_GRAMMAR"
                                , "TX_LEXICON_SH"
                                , "TX_LEMMA"
                                , "N_BEGIN"
                                , "N_END"
                            )
                            values( %s, %s, %s, %s, %s, %s, %s );
                    """, (p_id, l_word0,
                          json.dumps(l_grammar0),
                          json.dumps(l_lex0),
                          json.dumps(l_lemma0),
                          l_begin0, l_end0))

            p_db_connection.commit()
    except Exception as e0:
        p_db_connection.rollback()

        print('DB ERROR:', repr(e0))
        print(l_cursor_w.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_w.close()

    l_cursor_w = p_db_connection.cursor()
    try:
        l_cursor_w.execute("""
                    update "TB_PARSING"
                    set "TX_PADAPATHA" = %s, "TX_INVERTED" = %s
                    where "ID_PARSING" = %s;
                """, (p_padapatha, json.dumps(p_inverted_table), p_id_parsing))

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
    print('| Interpret Shatapatha Brahmana Sanskrit INRIA Parsing       |')
    print('|                                                            |')
    print('| interpret_parsing.py                                       |')
    print('|                                                            |')
    print('| v. 1.0 - 30/01/2019                                        |')
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
                delete from "TB_WORD"
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
                    "P"."ID_VERSE"
                    , "P"."TX_PARSING_HTML"
                    , "P"."N_BEGIN"
                    , "S"."TX_VERSE"
                    , "P"."ID_PARSING"
                from "TB_PARSING" "P" join "TB_SANSKRIT" "S" on "P"."ID_VERSE" = "S"."ID_VERSE"
                where "N_LENGTH" > 3000
                order by "ID_PARSING"
            """)

        for l_id_verse, l_html, l_begin_position, l_skt_text, l_id_parsing in l_cursor_read:
            # check for <!DOCTYPE html>
            l_match = re.search(r'<!DOCTYPE\shtml>', l_html)
            if not l_match:
                continue

            l_skt_text = re.sub(r'\|', '', l_skt_text)
            l_skt_text = re.sub(r'\s+', ' ', l_skt_text)
            l_skt_text = iast_2_mixed(l_skt_text)

            print('------------------------', l_id_verse, '-------------------------------')
            print(len(l_html))
            l_html = re.sub(r'<head>.*</head>', '', l_html, flags=re.DOTALL | re.MULTILINE)
            print(len(l_html))
            l_html = html.unescape(l_html)
            print(len(l_html))

            # href="https://sanskrit.inria.fr ... "
            l_html = re.sub(r'href="(https://sanskrit.inria.fr[^"]+)"', r'href=\1', l_html)
            # href="javascript ... "
            l_html = re.sub(r'href="(javascript[^"]+)"', r'href=\1', l_html)

            # with open('./inria.html', 'w', encoding='utf8') as l_fo:
            #    l_fo.write(l_html)

            l_root = None
            try:
                l_root = lxml.html.fromstring(l_html)
            except Exception as p:
                print('HTML ERROR:', repr(p))
                traceback.print_exc()
                sys.exit(0)

            l_top_table = l_root.xpath('//table[@class=\'center\']')[0]

            l_match = re.search(r'Gérard Huet', lxml.html.tostring(l_top_table, encoding='unicode'))
            if l_match:
                print('*** ERROR *** found Gérard Huet in table')
                sys.exit(0)
                # continue

            l_first_row = True
            l_row_count = 0
            l_sentence = []
            l_padapatha = ''
            l_col_count = 0
            l_inverted_table = []
            for l_row in l_top_table:
                print(l_row.tag)
                if l_first_row:
                    for l_cell in l_row:
                        l_padapatha += l_cell.text
                        l_inverted_table.append([l_cell.text])
                        l_col_count += 1
                    l_first_row = False
                    print(len(l_inverted_table), l_inverted_table)
                    continue
                else:
                    l_cell_count = 0
                    l_col_position = l_begin_position
                    l_end_previous = 0
                    for l_cell in l_row:
                        l_cell_string = lxml.html.tostring(l_cell, encoding='unicode')
                        print('   ', l_cell.tag, l_cell.attrib, len(l_cell_string))

                        # 9 = len('<td></td>')
                        if 'colspan' not in l_cell.attrib.keys() and len(l_cell_string) > 9:
                            print('*** ERROR *** [{0}] No colspan on a non-empty <td>'.format(l_cell_string))
                            sys.exit(0)

                        if 'colspan' in l_cell.attrib.keys():
                            if l_col_position - l_end_previous > 2 and l_row_count == 0:
                                print('Hole:', l_end_previous+1, l_col_position-1)
                                l_sentence.append((l_end_previous+1, l_col_position-1, '__HOLE__', [], [], []))

                            l_colspan = int(l_cell.attrib['colspan'])
                            l_sub_td = l_cell[0][0][0]
                            print('      ', l_sub_td.tag, l_sub_td.attrib)

                            l_onclick = l_sub_td.attrib['onclick']
                            l_word = l_sub_td.text

                            l_grammar = re.findall(r'{\s*([^}]+)\s*}', l_onclick)
                            l_link_lex = re.findall(r'<a\shref=(https://sanskrit.inria.fr[^>]+)>', l_onclick)
                            # <a href=https://sanskrit.inria.fr/DICO/1.html#atha><i>atha</i></a>
                            l_lemma = re.findall(
                                r'<a\shref=https://sanskrit.inria.fr/DICO/\d+.html#[^>]+><i>([^<]+)</i></a>',
                                l_onclick)
                            l_lex = get_lex(l_link_lex)

                            l_begin = l_col_position
                            l_col_position += l_colspan
                            l_end = l_col_position-1

                            print('      l_row_count:', l_row_count)
                            print('      l_begin    :', l_begin)
                            print('      l_end      :', l_end)
                            print('      l_word     :', l_word)
                            print('      l_lemma    :', l_lemma)
                            print('      l_grammar  :', l_grammar)
                            print('      l_link_lex :', l_link_lex)
                            print('      l_lex      :', l_lex)
                            print('      l_inverted_table[l_begin]:', l_inverted_table[l_begin])

                            if l_begin_position == 0:
                                l_inverted_table[l_begin].append(
                                    (l_colspan, l_word, l_lemma, l_grammar, l_link_lex))
                                for i in range(l_begin+1, l_col_position):
                                    l_inverted_table[i].append(
                                        (1, '__PLACEHOLDER__', [], [], []))

                            # print(l_inverted_table)

                            if l_row_count == 0:
                                l_sentence.append((l_begin, l_end, l_word, l_lemma, l_grammar, l_lex))
                            else:
                                for i in range(len(l_sentence)):
                                    (l_begin1, l_end1, l_word1, l_lemma1, l_grammar1, l_lex1) = l_sentence[i]
                                    if len(l_word) == 1 and l_end == l_end1:
                                        l_sent_new = l_sentence[:i+1] + \
                                                     [(l_begin, l_end, l_word, l_lemma, l_grammar, l_lex)] + \
                                                     l_sentence[i+1:]
                                        l_sentence = l_sent_new
                                        break
                                    elif l_word1 == '__HOLE__' and (l_begin == l_begin1 or l_begin == l_begin1-1):
                                        if l_end == l_end1 or l_end == l_end1 - 1:
                                            l_sent_new = l_sentence[:i] + \
                                                         [(l_begin, l_end, l_word, l_lemma, l_grammar, l_lex)] + \
                                                         l_sentence[i+1:]
                                            l_sentence = l_sent_new
                                            break
                                        elif l_end < l_end1-1:
                                            l_sent_new = l_sentence[:i] + \
                                                         [(l_begin, l_end, l_word, l_lemma, l_grammar, l_lex)] + \
                                                         [(l_end+1, l_end1, '__HOLE__', [], [], [])] +\
                                                         l_sentence[i+1:]
                                            l_sentence = l_sent_new
                                            break

                            l_end_previous = l_end
                        else:
                            l_inverted_table[l_col_position].append(
                                (1, '__EMPTY__', [], [], []))
                            l_col_position += 1
                        l_cell_count += 1
                l_row_count += 1

            l_padapatha = iast_2_mixed(l_padapatha)
            save_sentence(l_db_connection, l_id_verse, l_sentence, l_id_parsing, l_padapatha, l_inverted_table)
            if l_id_verse == 143375 and g_verse_stop:
                print(l_skt_text)
                print(l_padapatha)
                for l_begin, l_end, l_word, l_lemma, l_grammar, l_lex in l_sentence:
                    print('{0:5} {1:5} {2} {3} {4} {5} {6}'.format(
                        l_begin, l_end, l_word, l_padapatha[l_begin:l_end+1], l_lemma, l_grammar, l_lex))

                for r in range(l_col_count):
                    for c in range(l_row_count):
                        if c == 0:
                            print('{0:2}'.format(l_inverted_table[r][c]), end=' ')
                        else:
                            l_colspan, l_word, l_lemma, l_grammar, l_lex = l_inverted_table[r][c]
                            print('{0:2}'.format(
                                '..' if l_word == '__EMPTY__' else '++' if l_word == '__PLACEHOLDER__' else l_colspan
                            ), end=' ')
                    print()
                sys.exit(0)

    except Exception as e:
        print('DB ERROR:', repr(e))
        traceback.print_exc()
        print(l_cursor_read.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read.close()
