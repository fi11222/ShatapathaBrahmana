#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os.path
import re
import psycopg2
import sys

__author__ = 'Nicolas Reimen'

g_dbServer = 'localhost'
g_dbDatabase = 'ShatapathaBrahmana'
g_dbUser = 'postgres'
g_dbPassword = 'murugan!'

g_root = '/home/fi11222/disk-partage/Dev/Shatapatha Brahmana/SB/'
g_root_12 = '/home/fi11222/disk-partage/Dev/Shatapatha Brahmana/SB/SB 12/'

g_do_full = True

g_consonants = [
    ('क', 'k'),
    ('ख', 'kh'),
    ('ग', 'g'),
    ('घ', 'gh'),
    ('ङ', 'ṅ'),
    ('च', 'c'),
    ('छ', 'ch'),
    ('ज', 'j'),
    ('झ', 'jh'),
    ('ञ', 'ñ'),
    ('ट', 'ṭ'),
    ('ठ', 'ṭh'),
    ('ड', 'ḍ'),
    ('ढ', 'ḍh'),
    ('ण', 'ṇ'),
    ('त', 't'),
    ('थ', 'th'),
    ('द', 'd'),
    ('ध', 'dh'),
    ('न', 'n'),
    ('प', 'p'),
    ('फ', 'ph'),
    ('ब', 'b'),
    ('भ', 'bh'),
    ('म', 'm'),
    ('य', 'y'),
    ('र', 'r'),
    ('ल', 'l'),
    ('व', 'v'),
    ('ह', 'h'),
    ('श', 'ś'),
    ('ष', 'ṣ'),
    ('स', 's'),
    ('ळ', 'ḷ')
]

g_vowels = [
    ('ऐ', 'ै', 'ai'),
    ('औ', 'ौ', 'au'),
    ('अ', '', 'a'),
    ('आ', 'ा', 'ā'),
    ('इ', 'ि', 'i'),
    ('ई', 'ी', 'ī'),
    ('उ', 'ु', 'u'),
    ('ऊ', 'ू', 'ū'),
    ('ऋ', 'ृ', 'ṛ'),
    ('ॠ', 'ॄ', 'ṝ'),
    ('ऌ', 'ॢ', 'ḷ'),
    ('ॡ', 'ॣ', 'ḹ'),
    ('ए', 'े', 'e'),
    ('ओ', 'ो', 'o')
]

g_remove_accents = [
    ('ḵ', 'k'),
    ('ṉ', 'n'),
    ('ṯ', 't'),
    ('ṟ', 'r'),
    ('̥', ''),  # separate diacritic
    ('́', ''),  # separate diacritic
    ('ù', 'u'),
    ('Ṛ', 'ṛ'),
    ('Ś', 'ś'),
    ('Ḷ', 'ḷ'),
    ('ē', 'e'),
    ('Ḍ', 'ḍ'),
    ('Ū', 'ū'),
    ('Ṭ', 'ṭ'),
    ('ō', 'o'),
    ('è', 'e'),
    ('ò', 'o'),
    ('̀', ''),  # separate diacritic
    ('Ṇ', 'ṇ'),
    ('à', 'a'),
    ('é', 'e'),
    ('ú', 'u'),
    ('í', 'i'),
    ('ā', 'ā'),  # separate diacritic
    ('ī', 'ī'),  # separate diacritic
    ('ū', 'ū'),  # separate diacritic
    ('̄', ''),  # separate diacritic
    ('Ā', 'ā'),
    ('Ṣ', 'ṣ'),
    ('ì', 'i'),
    ('Ī', 'ī'),
    ('ó', 'o'),
    ('á', 'a'),
    ('Ñ', 'ñ'),
]

g_visarga_d = 'ः'
g_visarga_l = 'ḥ'

g_anusvara_l = 'ṃ'
g_anusvara_d = 'ं'

g_virama = '्'


# ---------------------------------------------------- Functions -------------------------------------------------------
def remove_accents(p_string):
    """
    Remove all accents and other marks non directly translatable in Devanagari

    :param p_string: the input string
    :return: the string with accents removed
    """
    l_result = p_string.lower()

    for l_accent, l_no_accent in g_remove_accents:
        l_result = re.sub(l_accent, l_no_accent, l_result)

    return l_result


def iast_to_deva(p_string):
    """
    Transforms a string in IAST Sanskrit script into its Devanagari equivalent

    :param p_string: string in IAST alphabet
    :return: Devanagari equivalent
    """
    l_result = p_string.lower()

    for l_kd, l_kl in g_consonants:
        for _, l_vd, l_vl in g_vowels:
            l_from = l_kl + l_vl
            l_to = l_kd + l_vd
            l_result = re.sub(l_from, l_to, l_result)

    for l_kd, l_kl in g_consonants:
        l_result = re.sub(l_kl, l_kd + g_virama, l_result)

    for l_vd, _, l_vl in g_vowels:
        l_result = re.sub(l_vl, l_vd, l_result)

    l_result = re.sub(g_visarga_l, g_visarga_d, l_result)

    l_result = re.sub(" '", 'ऽ', l_result)

    return l_result


def clean_text(p_text):
    """
    Cleans a verse text string

    :param p_text: text to clean up
    :return: cleaned-up text. Removes HTML tags, \n, extra spaces
    """
    l_tex = p_text
    l_tex = re.sub(r'<BR>', ' ', l_tex)
    l_tex = re.sub(r'<[^>]+>', '', l_tex)

    # 11.7.4.2prajāpatirha vai svāṃ duhitaramabhidadhyau | divaṃ oṣasaṃ vā mithunyenayā syāmiti tāṃ sambabhūva
    l_tex = re.sub(r'\d+', '', l_tex)
    l_tex = re.sub(r'\.', '', l_tex)

    # Sentence: a
    l_tex = re.sub(r'Sentence:\s[ab]', '', l_tex)

    # &nbsp;
    l_tex = re.sub(r'&nbsp;', '', l_tex)

    l_tex = re.sub('\n', ' ', l_tex)
    l_tex = re.sub(r'\s+', ' ', l_tex)
    l_tex = l_tex.strip()

    # // /
    l_tex = re.sub(r'//$', '', l_tex)
    l_tex = re.sub(r'/$', '', l_tex)

    return l_tex


def save_verse(
        p_db_connection, p_book, p_chapter, p_section, p_verse_number, p_original_text, p_verse_text, p_verse_deva):
    """
    Saves a verse to the database

    :param p_db_connection: active database connexion
    :param p_book: Book number
    :param p_chapter: Chapter Number
    :param p_section: Section Number
    :param p_verse_number: Verse Number
    :param p_original_text: Raw Verse Text from the input file
    :param p_verse_text: Verse Text (IAST)
    :param p_verse_deva: Verse Text (Devanagari)
    :return:
    """

    l_cursor_w = p_db_connection.cursor()
    try:
        l_cursor_w.execute("""
                insert into 
                    "TB_SANSKRIT"(
                        "N_BOOK"
                        , "N_CHAPTER"
                        , "N_SECTION"
                        , "N_VERSE"
                        , "TX_VERSE"
                        , "TX_DEVA"
                        , "TX_VERSE_ORIGINAL"
                    )
                    values( %s, %s, %s, %s, %s, %s, %s );
            """, (p_book, p_chapter, p_section, p_verse_number, p_verse_text, p_verse_deva, p_original_text))

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
    print('| Load Shatapatha Brahmana Sanskrit Text                     |')
    print('|                                                            |')
    print('| load_sanskrit.py                                           |')
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
                delete from "TB_SANSKRIT"
            """)

    except Exception as e:
        print('DB ERROR:', repr(e))
        print(l_cursor_write.query)
        sys.exit(0)
    finally:
        # release DB objects once finished
        l_cursor_write.close()

    l_letter_set = set()

    # Load full chapters
    for l_file in os.listdir(g_root):
        l_path = os.path.join(g_root, l_file)

        if g_do_full and \
                os.path.isfile(l_path) and re.match(r'sb_\d\d_u.htm', os.path.basename(l_path)):
            print(l_path)
            with open(l_path, "r") as l_content:
                l_text = ''
                l_book = 0
                l_chapter = 0
                l_section = 0
                l_verse = 0

                # adding exd marker
                l_page = l_content.read() + '1.1.1.[10]<BR>'

                for l_line in l_page.split('\n'):
                    # 1.1.1.[10]
                    l_match = re.match(r'^(\d+)\.(\d+)\.(\d+)\.\[(\d+)\]<BR>', l_line)
                    if l_match:
                        if l_text != '' and l_book > 0:
                            # print(l_text)
                            l_text = clean_text(l_text)
                            # print(l_text)
                            l_letter_set = l_letter_set.union(set([c for c in l_text]))
                            l_original = l_text
                            l_text = remove_accents(l_text)
                            l_deva = iast_to_deva(l_text)
                            print(l_text)
                            print(l_deva)
                            # if (l_book, l_chapter, l_section, l_verse) == (4, 4, 5, 3):
                            #    sys.exit()
                            save_verse(
                                l_db_connection,
                                l_book, l_chapter, l_section, l_verse,
                                l_original, l_text, l_deva)

                        l_book = int(l_match.group(1))
                        l_chapter = int(l_match.group(2))
                        l_section = int(l_match.group(3))
                        l_verse = int(l_match.group(4))
                        print('{0}.{1}.{2}.[{3}]'.format(l_book, l_chapter, l_section, l_verse))
                        l_text = ''
                    else:
                        l_text += l_line

        print(l_letter_set)

    for l_file in os.listdir(g_root_12):
        l_path = os.path.join(g_root_12, l_file)

        if os.path.isfile(l_path) and re.match(r'SB 12-\d.htm', os.path.basename(l_path)):
            print(l_path)
            with open(l_path, "r") as l_content:
                l_text = ''
                l_book = 12
                l_chapter = 0
                l_section = 0
                l_verse = 0

                l_chapter_a = 0
                l_section_a = 0
                l_verse_a = 0

                l_text_a = None
                for l_line in l_content.readlines():
                    # YVW_SBM_12_1_1_1_b
                    l_match = re.search(r'YVW_SBM_12_(\d+)_(\d+)_(\d+)_([ab])', l_line)
                    if l_match:
                        l_chapter = int(l_match.group(1))
                        l_section = int(l_match.group(2))
                        l_verse = int(l_match.group(3))
                        l_ab = l_match.group(4)

                        l_text = clean_text(l_line)
                        l_letter_set = l_letter_set.union(set([c for c in l_text]))

                        print('{0}.{1}.{2}.[{3}]{4} {5}'.format(
                            l_book, l_chapter, l_section, l_verse, l_ab, l_text))

                        if l_ab == 'a':
                            if l_text_a is not None:
                                l_original = l_text_a
                                l_latin = remove_accents(l_original)
                                l_deva = iast_to_deva(l_latin)
                                print(l_latin)
                                print(l_deva)
                                save_verse(
                                    l_db_connection,
                                    l_book, l_chapter_a, l_section_a, l_verse_a,
                                    l_original, l_latin, l_deva)

                            l_text_a = l_text

                            l_chapter_a = l_chapter
                            l_section_a = l_section
                            l_verse_a = l_verse
                        else:
                            l_original = l_text_a + ' | ' + l_text
                            l_latin = remove_accents(l_original)
                            l_deva = iast_to_deva(l_latin)
                            print(l_latin)
                            print(l_deva)
                            save_verse(
                                l_db_connection,
                                l_book, l_chapter, l_section, l_verse,
                                l_original, l_latin, l_deva)
                            l_text_a = None

        print(l_letter_set)

    l_db_connection.close()
