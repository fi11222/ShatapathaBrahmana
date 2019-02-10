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
<script src="jquery-3.3.1.min.js"></script>
<link rel="stylesheet" type="text/css" href="output.css" media="screen,tv">
<script>
{1}
</script>
</head>
<body>
"""

g_jquery_base = """
$(document).ready(function () {
__HOVER_FUNCTIONS__
});
"""

g_jquery_hover = """
    $('__ORIGIN_ELEMENT__').hover(function () {
        $('__TARGET_ELEMENT__').addClass('active_word');
    }, function () {
        $('__TARGET_ELEMENT__').removeClass('active_word');
    });
"""

g_jquery_hover_double = """
    $('__ORIGIN_ELEMENT__').hover(function () {
        $('__TARGET_ELEMENT0__').addClass('active_word');
        $('__TARGET_ELEMENT1__').addClass('active_word');
    }, function () {
        $('__TARGET_ELEMENT0__').removeClass('active_word');
        $('__TARGET_ELEMENT1__').removeClass('active_word');
    });
"""

g_jquery_hover_triple = """
    $('__ORIGIN_ELEMENT__').hover(function () {
        $('__TARGET_ELEMENT0__').addClass('active_word');
        $('__TARGET_ELEMENT1__').addClass('active_word');
        $('__TARGET_ELEMENT2__').addClass('active_word');
    }, function () {
        $('__TARGET_ELEMENT0__').removeClass('active_word');
        $('__TARGET_ELEMENT1__').removeClass('active_word');
        $('__TARGET_ELEMENT2__').removeClass('active_word');
    });
"""

g_html_bottom = """
</body></html>
"""

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

g_vowels = ['a', 'i', 'u', 'e', 'o', 'ऐ', 'औ', 'ī', 'ū', 'ā']


def get_words(p_db_connection, p_id):
    """
    Retrieve the word list for a given verse and generate the HTML output

    :param p_db_connection: live DB connection
    :param p_id: ID of the verse to retrieve
    :return: the HTML string
    """
    l_cursor_read0 = p_db_connection.cursor()
    l_html = '<table>\n'
    l_pos_list = []
    try:
        l_cursor_read0.execute("""
            select 
                "TX_WORD"
                , "TX_GRAMMAR"
                , "TX_LEXICON_SH"
                , "TX_LEMMA"
                , "N_BEGIN"
                , "N_END"
            from
                "TB_WORD"
            where
                "ID_VERSE" = %s
            order by "ID_WORD"
        """, (p_id, ))

        l_db_list = []
        for l_word, l_grammar_json, l_lex_json, l_lemma_json, l_begin, l_end in l_cursor_read0:
            l_grammar = json.loads(l_grammar_json)
            l_lexicon = json.loads(l_lex_json)
            l_lemma = json.loads(l_lemma_json)

            l_db_list.append((l_word, l_grammar, l_lexicon, l_lemma, l_begin, l_end))

        for i in range(len(l_db_list)):
            l_word, l_grammar, l_lexicon, l_lemma, l_begin, l_end = l_db_list[i]

            if i > 0:
                _, _, _, _, _, l_end_previous = l_db_list[i - 1]
            else:
                l_end_previous = None

            if i < len(l_db_list) - 1:
                _, _, _, _, l_begin_next, _ = l_db_list[i+1]
            else:
                l_begin_next = None

            # print('b', l_begin, l_end_previous)
            # print('a', l_end, l_begin_next)

            if l_begin == l_end:
                l_pos_record = (l_begin, l_begin, (l_begin, l_begin))
            elif l_end == l_begin_next:
                if l_begin == l_end_previous:
                    if l_end - l_begin >= 2:
                        l_pos_record = (l_begin, l_end, (l_begin, l_begin, l_begin+1, l_end-1, l_end, l_end))
                    else:
                        l_pos_record = (l_begin, l_end, (l_begin, l_begin, l_end, l_end))
                else:
                    l_pos_record = (l_begin, l_end, (l_begin, l_end-1, l_end, l_end))
            else:
                if l_begin == l_end_previous:
                    l_pos_record = (l_begin, l_end, (l_begin, l_begin, l_begin+1, l_end))
                else:
                    l_pos_record = (l_begin, l_end, (l_begin, l_end))

            l_pos_list.append(l_pos_record)
            # print(l_word, l_pos_record)

            # l_word = str(len(l_word)) + '/' + l_word
            l_word_new = ''
            if len(l_word) > 20:
                # l_word = str(len(l_word) // 20) + '!' + l_word
                for i in range(len(l_word) // 20 + 1):
                    l_word_new += l_word[i*20:min(len(l_word), (i+1)*20)] + '- '
                # l_word += '|' + l_word_new
                l_word = l_word_new[:-2]

            if len(l_lemma) > 0:
                l_word_class = ''
            else:
                l_word_class = ' class="unknown_string"'

            l_id = 'wt_{0}_{1}'.format(l_begin, l_end)
            l_html += (
                '<tr><td class="tooltip lex_cell"><span id="{3}"{1}>{0}</span>' +
                '<span class="tooltiptext">{2}</span></td>\n').format(
                    l_word,
                    l_word_class,
                    ' / '.join(l_grammar),
                    l_id)

            l_html += '<td class="lex_cell"><span class="lemma">{0}</span></td>\n'.format('-'.join(l_lemma))

            # if len(l_grammar) == 1:
            #     l_html += '<td>{0}</td>\n'.format(l_grammar[0])
            # else:
            #    l_html += '<td><ul>\n'
            #     for l_gr in l_grammar:
            #        l_html += '<li>{0}</li>\n'.format(l_gr)
            #    l_html += '</ul></td>\n'

            l_new_lex = []
            for l_lex in l_lexicon:
                if l_lex == 'Lexicon Entry Not Found':
                    l_new_lex.append('<span class="abbrev">Lexicon Entry Not Found</span>')
                else:
                    l_new_lex.append(re.sub(r'^([^\s]+)\s', r'<span class="lexword">\1</span> ', l_lex))
            l_lexicon = l_new_lex

            # v. [1] pr. (bhavati) imp. (bhava) opt. (bhavet) fut. (bhaviṣyati) fut. péri. (bhavitā) pft.
            # (babhūva) aor. [1] (abhūt) cond.
            l_new_lex = []
            l_abbrev = ['suppl.', 'pn.', 'syn.', 'adv.', 'pf.', 'a.', 'm.', 'n.', 'f.', 'var.', 'pn.', 'dém.', 'v.',
                        'lat.', 'conj.', 'act.', 'pl.', 'np.', 'véd.', 'fr.', 'cl.', 'part.', 'ifc.', 'pr.', 'pp.',
                        'aor.', 'pfp.', 'hi.', 'inf.', 'abs.', 'md.', 'pfu.', 'ca.', 'ppr.', 'ps.', 'ppft.',
                        'loc.', 'gr.', 'dés.', 'int.', 'myth.', 'math.', 'symb.', 'phil.', 'arch.', 'astr.', 'ang.',
                        'all.', 'compar.', 'péj.', 'pft.', 'gram.', 'cf.', 'opp.', 'zoo.', 'géo.', 'hist.', 'du.',
                        'épith.', 'red.']
            for l_lex in l_lexicon:
                # move conjugation at end
                l_re = r'v\.\s(?:(?:\[\d+]\s)?(?:[a-z]+\.\s)+(?:\[\d+]\s)?\([^)]+\)\s)+'
                l_all_matches = re.findall(l_re, l_lex)
                if len(l_all_matches) > 0:
                    l_lex = re.sub(l_re, 'v. ', l_lex)
                    l_lex = re.sub(r'(?:v\.\s)+', 'v. ', l_lex) + ' <span class="conjug">Conj.</span>: '
                    for m in l_all_matches:
                        l_lex += m
                    l_lex = re.sub(r'\s+', ' ', l_lex).strip()

                # grey abbreviations
                for l_abr in l_abbrev:
                    l_abr_re = r'\b' + re.sub(r'\.', r'\.', l_abr).strip()
                    l_lex = re.sub(l_abr_re, '<span class="abbrev">{0}</span>'.format(l_abr), l_lex).strip()

                l_new_lex.append(l_lex)
            l_lexicon = l_new_lex

            if len(l_lexicon) == 1:
                l_html += '<td class="lex_cell">{0}</td>\n'.format(l_lexicon[0])
            else:
                l_html += '<td class="lex_cell"><ul class="lex_options">\n'
                for l_lex in l_lexicon:
                    l_html += '<li class="lex_options">{0}</li>\n'.format(l_lex)
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
    return l_html, l_pos_list


def get_padapatha(p_db_connection, p_id_verse):
    """

    :param p_db_connection:
    :param p_id_verse:
    :return:
    """
    l_pada = ''
    l_seg_count = 0
    l_cursor_read0 = p_db_connection.cursor()
    try:
        l_cursor_read0.execute("""
                select 
                    "TX_PADAPATHA"
                    , "N_BEGIN"
                    , "N_END"
                    , "N_LENGTH"
                from
                    "TB_PARSING"
                where
                    "ID_VERSE" = %s
                order by "ID_PARSING"
            """, (p_id_verse,))

        # print(l_cursor_read0.query.decode('utf8'))
        for l_segment_txt, l_begin, l_end, l_length in l_cursor_read0:
            l_segment_txt = l_segment_txt if l_segment_txt is not None else ''
            if l_begin == 0:
                l_pada += l_segment_txt
            else:
                l_pada += ' ' + l_segment_txt
            l_seg_count += 1
    except Exception as e0:
        print('DB ERROR:', repr(e0))
        traceback.print_exc()
        print(l_cursor_read0.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read0.close()

    return l_pada, l_seg_count


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

    l_next_file = dict()
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
            -- where "S"."ID_VERSE" in (143413, 143414, 143415)
            order by "S"."N_BOOK", "S"."N_CHAPTER", "S"."N_SECTION", "S"."N_VERSE"
        """)

        l_previous = None
        for l_id_verse, l_book, l_chapter, l_section, l_verse, l_skt_txt, l_trans_txt in l_cursor_read:
            l_file_name = '{0:02}_{1:02}_{2:02}_{3:03}.html'.format(l_book, l_chapter, l_section, l_verse)
            if l_previous is not None:
                l_next_file[l_previous] = l_file_name
            l_previous = l_file_name
    except Exception as e:
        print('DB ERROR:', repr(e))
        traceback.print_exc()
        print(l_cursor_read.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read.close()

    # print(l_next_file)

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
                , "S"."TX_DEVA" 
                , "T"."TX_VERSE"
            from "TB_SANSKRIT" "S" left outer join "TB_TRANSLATION" "T" on 
                "T"."N_BOOK" = "S"."N_BOOK" 
                and "T"."N_CHAPTER" = "S"."N_CHAPTER" 
                and "T"."N_SECTION" = "S"."N_SECTION" 
                and "T"."N_VERSE" = "S"."N_VERSE"
            -- where "S"."ID_VERSE" in (143413, 143414, 143415)
            order by "S"."N_BOOK", "S"."N_CHAPTER", "S"."N_SECTION", "S"."N_VERSE"
        """)

        l_previous_file = None
        for l_id_verse, l_book, l_chapter, l_section, l_verse, l_skt_txt, l_deva_txt, l_trans_txt in l_cursor_read:
            print('------------------------', l_id_verse, '-------------------------------')
            print(l_skt_txt)
            print(l_trans_txt)

            l_file_name = '{0:02}_{1:02}_{2:02}_{3:03}.html'.format(l_book, l_chapter, l_section, l_verse)
            print(l_file_name)
            l_path = os.path.join(g_output_path, l_file_name)

            l_padapatha, l_segment_count = get_padapatha(l_db_connection, l_id_verse)
            print(l_padapatha)

            with open(l_path, 'w') as l_file_out:
                # l_jquery = g_jquery_hover
                # l_jquery = re.sub('__ORIGIN_ELEMENT__', '#trigger', l_jquery)
                # l_jquery = re.sub('__TARGET_ELEMENT__', '#target', l_jquery)
                l_jquery = ''

                l_words_table, l_positions_list = get_words(l_db_connection, l_id_verse)
                print(l_positions_list)

                l_skt_span = ''
                l_prev_right = -1
                l_previous_single = None
                for l_left, l_right, l_target_list in l_positions_list:
                    if l_left - l_prev_right > 1:
                        l_skt_span += ' '

                    if len(l_target_list) > 4:
                        l_t_left0, l_t_right0, l_t_left1, l_t_right1, l_t_left2, l_t_right2 = l_target_list
                        l_id_target0 = 'wd_{0}_{1}'.format(l_t_left0, l_t_right0)
                        l_id_target1 = 'wd_{0}_{1}'.format(l_t_left1, l_t_right1)
                        l_id_target2 = 'wd_{0}_{1}'.format(l_t_left2, l_t_right2)
                        l_id_trigger = 'wt_{0}_{1}'.format(l_left, l_right)
                        l_hover_function = g_jquery_hover_triple
                        l_hover_function = re.sub('__ORIGIN_ELEMENT__', '#' + l_id_trigger, l_hover_function)
                        l_hover_function = re.sub('__TARGET_ELEMENT0__', '#' + l_id_target0, l_hover_function)
                        l_hover_function = re.sub('__TARGET_ELEMENT1__', '#' + l_id_target1, l_hover_function)
                        l_hover_function = re.sub('__TARGET_ELEMENT2__', '#' + l_id_target1, l_hover_function)
                        l_jquery += l_hover_function

                        if l_previous_single != l_padapatha[l_t_left0:l_t_right0 + 1]:
                            l_skt_span += '<span id="{0}">{1}</span>'.format(
                                l_id_target0, l_padapatha[l_t_left0:l_t_right0 + 1]
                            )
                        l_skt_span += '<span id="{0}">{1}</span>'.format(
                            l_id_target1, l_padapatha[l_t_left1:l_t_right1 + 1]
                        )
                        l_skt_span += '<span id="{0}">{1}</span>'.format(
                            l_id_target2, l_padapatha[l_t_left2:l_t_right2 + 1]
                        )
                        l_previous_single = l_padapatha[l_t_left2:l_t_right2 + 1]
                    elif len(l_target_list) > 2:
                        l_t_left0, l_t_right0, l_t_left1, l_t_right1 = l_target_list
                        l_id_target0 = 'wd_{0}_{1}'.format(l_t_left0, l_t_right0)
                        l_id_target1 = 'wd_{0}_{1}'.format(l_t_left1, l_t_right1)
                        l_id_trigger = 'wt_{0}_{1}'.format(l_left, l_right)
                        l_hover_function = g_jquery_hover_double
                        l_hover_function = re.sub('__ORIGIN_ELEMENT__', '#' + l_id_trigger, l_hover_function)
                        l_hover_function = re.sub('__TARGET_ELEMENT0__', '#' + l_id_target0, l_hover_function)
                        l_hover_function = re.sub('__TARGET_ELEMENT1__', '#' + l_id_target1, l_hover_function)
                        l_jquery += l_hover_function

                        if l_previous_single != l_padapatha[l_t_left0:l_t_right0 + 1]:
                            l_skt_span += '<span id="{0}">{1}</span>'.format(
                                l_id_target0, l_padapatha[l_t_left0:l_t_right0 + 1]
                            )
                        l_skt_span += '<span id="{0}">{1}</span>'.format(
                            l_id_target1, l_padapatha[l_t_left1:l_t_right1 + 1]
                        )
                        l_previous_single = l_padapatha[l_t_left1:l_t_right1+1] if l_t_left1 == l_t_right1 else None
                    else:
                        l_id_target = 'wd_{0}_{1}'.format(l_left, l_right)
                        l_id_trigger = 'wt_{0}_{1}'.format(l_left, l_right)
                        l_hover_function = g_jquery_hover
                        l_hover_function = re.sub('__ORIGIN_ELEMENT__', '#' + l_id_trigger, l_hover_function)
                        l_hover_function = re.sub('__TARGET_ELEMENT__', '#' + l_id_target, l_hover_function)
                        l_jquery += l_hover_function

                        if l_previous_single != l_padapatha[l_left:l_right + 1]:
                            l_skt_span += '<span id="{0}">{1}</span>'.format(
                                l_id_target, l_padapatha[l_left:l_right+1]
                            )
                        l_previous_single = l_padapatha[l_left:l_right+1] if l_left == l_right else None

                    l_prev_right = l_right

                for l_deva, l_iast in g_conversion_mixed:
                    l_skt_span = re.sub(l_deva, l_iast, l_skt_span)

                l_jquery = re.sub('__HOVER_FUNCTIONS__', l_jquery, g_jquery_base)

                l_file_out.write(g_html_top.format(
                    '{0}.{1}.{2}.{3}'.format(l_book, l_chapter, l_section, l_verse),
                    l_jquery))
                l_file_out.write('<div class="header">\n')
                l_previous_link = \
                    '<a class="previous_verse" href="./{0}">Previous</a> '.format(l_previous_file) \
                    if l_previous_file is not None else ''
                l_next_link = \
                    ' <a class="next_verse" href="./{0}">Next</a>'.format(l_next_file[l_file_name]) \
                    if l_file_name in l_next_file.keys() is not None else ''
                l_file_out.write(
                    '<p class="verse_number">' + l_previous_link +
                    '<span class="verse_number">{0}:{1}:{2}:{3}</span>'.format(
                        l_book, l_chapter, l_section, l_verse) +
                    l_next_link + '</p>\n')

                # l_file_out.write('<p class="skt_iast">Test <span id="target">Text</span> bla</p>\n')
                # l_file_out.write('<p">This is the <span id="trigger">Trigger</span> bla</p>\n')
                # l_deva_txt
                l_file_out.write('<p class="skt_deva">{0}</p>\n'.format(l_deva_txt))
                l_file_out.write('<p class="skt_iast">{0}</p>\n'.format(l_skt_txt))
                if l_segment_count == 1:
                    l_file_out.write('<p class="skt_span">{0}</p>\n'.format(l_skt_span))
                l_file_out.write('<p id="translation">{0}</p>\n'.format(l_trans_txt))

                l_file_out.write('</div><div class="middle">\n')
                l_file_out.write(l_words_table)
                l_file_out.write('</div>')
                l_file_out.write(g_html_bottom)

            l_previous_file = l_file_name

    except Exception as e:
        print('DB ERROR:', repr(e))
        traceback.print_exc()
        print(l_cursor_read.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_read.close()
