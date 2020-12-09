#' @title
#' Search Interventions
#'
#' @description
#' Search an aggregate of all the interventions in the AACT database for a specific term. To retrieve the full aggregate as a dataframe, see \code{\link{aggregate_interventions}}.
#'
#' @export
#' @rdname search_interventions


search_interventions <-
        function(search_term,
                 ignore.case = TRUE,
                 perl = FALSE,
                 fixed = FALSE,
                 useBytes = FALSE,
                 conn,
                 verbose = TRUE,
                 render_sql = TRUE) {


                all_interventions <<-
                        aggregate_interventions(conn = conn,
                                                verbose = verbose,
                                                render_sql = render_sql)

                all_interventions %>%
                        dplyr::filter_at(dplyr::vars(c(intervention_name, intervention_other_name)),
                                         dplyr::any_vars(grepl(pattern = search_term,
                                                                           x = .,
                                                                           ignore.case = ignore.case,
                                                                           perl = perl,
                                                                           fixed = fixed,
                                                                           useBytes = useBytes
                                                                           ) == TRUE))

        }
