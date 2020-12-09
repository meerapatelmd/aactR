#' @title
#' Aggregate Interventions
#'
#' @description
#' Aggregate all the Interventions from the Interventions and Intervention Other Names Table to inspect whether they are valid synonyms.
#'
#'
#' @rdname aggregate_interventions
#' @export



aggregate_interventions <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE) {

                sql_statement <-
                "
                SELECT DISTINCT i.id AS intervention_id,
                        i.nct_id,
                        i.intervention_type,
                        i.name AS intervention_name,
                        i.description AS intervention_description,
                        io.name AS intervention_other_name
                FROM ctgov.interventions i
                LEFT JOIN ctgov.intervention_other_names io
                ON i.id = io.intervention_id
                        AND i.nct_id = io.nct_id
                "

                pg13::query(conn = conn,
                            sql_statement = sql_statement,
                            verbose = verbose,
                            render_sql = render_sql)

        }

