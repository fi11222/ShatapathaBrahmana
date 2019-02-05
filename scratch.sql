select "A"."N_BOOK", "A"."COUNT_S", "B"."COUNT_T"
from (
	select "N_BOOK", count(1) "COUNT_T"
	from "TB_TRANSLATION"
	group by "N_BOOK"
	order by "N_BOOK"
) "B" join (
	select "N_BOOK", count(1) "COUNT_S"
	from "TB_SANSKRIT"
	group by "N_BOOK"
	order by "N_BOOK"
) "A" on "A"."N_BOOK" = "B"."N_BOOK"

select "ID_VERSE", "N_LENGTH", "TX_PARSING_HTML"
from "TB_PARSING"
order by "N_LENGTH"

select
	"S"."N_BOOK"
	, "S"."N_CHAPTER"
	, "S"."N_SECTION"
	, "S"."N_VERSE"
	, "S"."TX_VERSE"
	, "P"."N_LENGTH"
	, "P"."TX_PARSING_HTML"
from "TB_SANSKRIT" "S" join "TB_PARSING" "P" on "P"."ID_VERSE" = "S"."ID_VERSE"
where "P"."N_LENGTH" < 20000
order by "N_LENGTH"

delete from "TB_PARSING"
where "N_LENGTH" < 4000