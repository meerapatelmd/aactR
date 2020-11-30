#' @title
#' Setup and Maintain ClinicalTrial.gov `aact` Postgres Database
#'
#' @inherit clinicaltrialsgov_functions description
#' @inheritSection clinicaltrialsgov_functions AACT Database
#'
#' @return
#' A `aact` Postgres Database if one did not previously exist with a populated `ctgov` schema and a log of the source file and timestamp in the `public.update_log` Table.
#'
#' @seealso
#'  \code{\link[police]{try_catch_error_as_null}}
#'  \code{\link[httr]{with_config}},\code{\link[httr]{c("add_headers", "authenticate", "config", "config", "set_cookies", "timeout", "use_proxy", "user_agent", "verbose")}},\code{\link[httr]{GET}},\code{\link[httr]{content}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[curl]{handle}},\code{\link[curl]{curl_download}}
#'  \code{\link[pg13]{query}},\code{\link[pg13]{createDB}},\code{\link[pg13]{localConnect}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}},\code{\link[pg13]{dc}}
#' @rdname run_aact_db
#' @export
#' @importFrom httr with_config config GET content
#' @importFrom rvest html_nodes html_children html_text
#' @importFrom curl new_handle curl_download
#' @importFrom pg13 query createDB localConnect lsTables appendTable writeTable dc brake_closed_conn


run_aact_db <-
        function(conn,
                 conn_fun,
                 verbose = TRUE,
                 render_sql = TRUE) {


                if (!missing(conn_fun)) {

                        if (verbose) {
                                cli::cat_line()
                                cli::cat_rule("Make connection")
                        }

                        conn <- eval(rlang::parse_expr(conn_fun))
                        on.exit(pg13::dc(conn = conn,
                                         verbose = verbose))

                }

                if (verbose) {
                        cli::cat_line()
                        cli::cat_rule("Check Connection")
                }


                pg13::brake_closed_conn(conn = conn)


                if (verbose) {
                        cli::cat_line()
                        cli::cat_rule("Read AACT Page")
                }

                # Reading ACCT Page with crawl delay
                Sys.sleep(5)
                aact_page <-
                        tryCatch(
                                httr::with_config(
                                        config = httr::config(ssl_verifypeer = FALSE),
                                        httr::GET("https://aact.ctti-clinicaltrials.org/snapshots")
                                ) %>%
                                        httr::content(),
                                error = function(error) NULL
                        )


                # Parse Most Recent Filename
                file_archive <-
                        aact_page %>%
                        rvest::html_nodes(".file-archive td") %>%
                        rvest::html_children() %>%
                        rvest::html_text()
                file_archive <- file_archive[1]

                if (verbose) {
                        secretary::typewrite(secretary::enbold("File Archive:"), file_archive)
                }


                # Download most recent archived file if it already isn't in the working directory
                if (!file.exists(file_archive)) {

                        if (verbose) {
                                cli::cat_line()
                                cli::cat_rule("Download File Archive")
                        }

                        Sys.sleep(3)
                        handle <- curl::new_handle(ssl_verifypeer=FALSE)
                        curl::curl_download(
                                paste0("https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/", file_archive),
                                destfile = file_archive,
                                quiet = FALSE,
                                handle = handle)

                }


                if (verbose) {
                        cli::cat_line()
                        cli::cat_rule("Unzip File Archive")
                }


                # Unzip
                files <- unzip(file_archive)


                # Check for a fresh database by schema count and tables
                if (!("ctgov" %in% pg13::lsSchema(conn = conn))) {

                        if (verbose) {
                                cli::cat_line()
                                cli::cat_rule("Instantiate ctgov Schema")
                                command <- paste0("pg_restore -e -v -O -x -d aact --no-owner ", getwd(), "/postgres_data.dmp")
                                secretary::typewrite(secretary::enbold("Command:"), command)
                        }

                } else {

                        if (verbose) {
                                cli::cat_line()
                                cli::cat_rule("Update ctgov Schema")
                                command <- paste0("pg_restore -e -v -O -x -d aact --clean --no-owner ", getwd(), "/postgres_data.dmp")
                                secretary::typewrite(secretary::enbold("Command:"), command)

                        }
                }

                system(command = command)


                if (verbose) {
                        cli::cat_line()
                        cli::cat_rule("Log Activity")
                }


                log_exists <- pg13::table_exists(conn = conn,
                                   schema = "public",
                                   tableName = "aact_log")


                if (!log_exists) {

                        pg13::send(
                                conn = conn,
                                sql_statement =
                                        "
                                        CREATE TABLE public.aact_log (
                                                update_datetime TIMESTAMP without TIME ZONE,
                                                file_archive_zip VARCHAR(255)
                                        );
                                        ",
                                verbose = verbose,
                                render_sql = render_sql
                        )
                }

                pg13::appendTable(conn = conn,
                                  schema = "public",
                                  tableName = "aact_log",
                                  data = tibble::tibble(
                                                update_datetime = Sys.time(),
                                                file_archive_zip = file_archive
                                  ))

                if (verbose) {
                        cli::cat_line()
                        cli::cat_rule("Remove Files")
                }

                # Remove all files
                file.remove(files,
                            file_archive)


        }
