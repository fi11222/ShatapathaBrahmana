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


