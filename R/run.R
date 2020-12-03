#' @title
#' Setup and Maintain ClinicalTrial.gov `aact` Postgres Database
#'
#' @description
#' The `aact` Postgres database instance is available for download at \url{https://aact.ctti-clinicaltrials.org/}. Technical documentation on the AACT Database instantiation can be found at \url{https://aact.ctti-clinicaltrials.org/snapshots}. Documentation on database structure and naming conventions can be found at \url{https://aact.ctti-clinicaltrials.org/schema}.
#'
#' @seealso
#'  \code{\link[cli]{cat_line}}
#'  \code{\link[rlang]{parse_expr}}
#'  \code{\link[pg13]{dc}},\code{\link[pg13]{brake_closed_conn}},\code{\link[pg13]{lsSchema}},\code{\link[pg13]{table_exists}},\code{\link[pg13]{send}},\code{\link[pg13]{appendTable}}
#'  \code{\link[httr]{with_config}},\code{\link[httr]{c("add_headers", "authenticate", "config", "config", "set_cookies", "timeout", "use_proxy", "user_agent", "verbose")}},\code{\link[httr]{GET}},\code{\link[httr]{content}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[secretary]{c("typewrite", "typewrite")}},\code{\link[secretary]{character(0)}}
#'  \code{\link[curl]{handle}},\code{\link[curl]{curl_download}}
#'  \code{\link[tibble]{tibble}}
#' @rdname run_aact_db
#' @export
#' @importFrom cli cat_line cat_rule
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc brake_closed_conn lsSchema table_exists send appendTable
#' @importFrom httr with_config config GET content
#' @importFrom rvest html_nodes html_children html_text
#' @importFrom secretary typewrite enbold
#' @importFrom curl new_handle curl_download
#' @importFrom tibble tibble



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

                if (pg13::is_conn_open(conn = conn)) {

                        cli::cli_alert_success("Valid connection")

                } else {

                        cli::cli_alert_danger("Invalid connection")
                        stop(call. = FALSE)

                }


                if (verbose) {
                        cli::cat_line()
                        cli::cat_rule("Read AACT Page")
                }

                # Reading ACCT Page with crawl delay
                sp <- cli::make_spinner()
                lapply(1:5, function(x) { sp$spin(); Sys.sleep(1) })
                sp$finish()

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



                # Remove files on exit
                on.exit(
                        if (verbose) {
                                cli::cat_line()
                                cli::cat_rule("Remove Files")
                        },
                        add = TRUE)

                on.exit(# Remove all files
                        file.remove(files,
                                    file_archive),
                        after = TRUE,
                        add = TRUE)


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


        }
