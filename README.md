# utl-nearest-sales-date-on-or-before-a-commercial-date-using-r-roll-join-and-wps-r-and-python-sql
Nearest sales date on or before a commercial date using r roll join and wps r and python sql 
    %let pgm=utl-nearest-sales-date-on-or-before-a-commercial-date-using-r-roll-join-and-wps-r-and-python-sql;

    Nearest sales date on or before a commercial date using r roll join and wps r and python sql

    github
    https://tinyurl.com/2d58c84a
    https://github.com/rogerjdeangelis/utl-nearest-sales-date-on-or-before-a-commercial-date-using-r-roll-join-and-wps-r-and-python-sql


       Solutions

            1. wps sql
            2. native r rolling joins
               https://www.gormanalysis.com/blog/r-data-table-rolling-joins/
            3. r sql
            4. python sql

    https://www.gormanalysis.com/blog/r-data-table-rolling-joins/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */
    options validvarname=upcase;
    libname sd1 "d:/sd1";

    data sd1.sales;
     informat SaleId $2.  SaleDate yymmdd10.;
     format SaleDate yymmdd10.;
     input SaleId   SaleDate;
    cards4;
    S1 2014-02-20
    S2 2014-05-01
    S3 2014-06-15
    S4 2014-07-01
    S5 2014-12-31
    ;;;;
    run;quit;

    data sd1.commercials;
     informat CommercialId $2. CommercialDate yymmdd10.;
     format CommercialDate yymmdd10.;
     input CommercialId CommercialDate;
    cards4;
    C1 2014-01-01
    C2 2014-04-01
    C3 2014-07-01
    C4 2014-09-15
    ;;;;
    run;quit;

    /**************************************************************************************************************************************/
    /*                                                      |                                                                             */
    /*SD1.COMMERCIALS                  SD1.SALES            | RULES (ASSOCIATE each commercial with the most                              */
    /*                                                      | recent sale prior to the commercial date                                    */
    /*                                                      |                                                                             */
    /* COMMERCIALID  COMMERCIALDATE  SALEID     SALEDATE    | COMMERCIAL COMMERCIAL  SALE                                                 */
    /*                                                      |    ID           DATE   ID                                                   */
    /*      C1         2014-01-01      S1      2014-02-20   |                                                                             */
    /*      C2         2014-04-01      S2      2014-05-01   |    C1      2014-01-01  no saledate <= commercial date 2014-01-01            */
    /*      C3         2014-07-01      S3      2014-06-15   |    C2      2014-04-01  S1 saledate 2014-02-20 <= commecial date 2014-04-01  */
    /*      C4         2014-09-15      S4      2014-07-01   |    C3      2014-07-01  S4 saledate 2014-07-01 =  commecial date 2014-07-01  */
    /*                                                      |    C4      2014-09-15  S4 saledate 2014-07-01 <= commecial date 2014-09-15  */
    /*                                                                                                                                    */
    /* OUTPUT                                                                                                                             */
    /* ======                                                                                                                             */
    /*                                                                                                                                    */
    /* The WPS System                                                                                                                     */
    /*                                                   NEAREST_BEFORE        DAYS                                                       */
    /* Obs    COMMERCIALID    COMMERCIALDATE    SALEID      SALEDATE    NEAREST_ON_OR_BEFORE                                              */
    /*                                                                                                                                    */
    /*  1          C1           2014-01-01                         .             .                                                        */
    /*  2          C2           2014-04-01        S1      2014-02-20           -40                                                        */
    /*  3          C3           2014-07-01        S4      2014-07-01             0                                                        */
    /*  4          C4           2014-09-15        S4      2014-07-01           -76                                                        */
    /*                                                                                                                                    */
    /**************************************************************************************************************************************/

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    options validvarname=any;

    proc sql;
       create
          table sd1.want as
       select
          commercialid
         ,commercialdate
         ,saleid
         ,saledate
         ,dif      as nearest_on_before
       from
          (
           select
              l.commercialid
             ,l.commercialdate
             ,r.saleid
             ,r.saledate
             ,(r.saledate - l.commercialdate) as dif
           from
             sd1.commercials as l left join sd1.sales as r
           on
             r.saledate <= l.commercialdate
          )
       group
          by commercialid
       having
          max(dif) = dif
       order
          by commercialid
    ;quit;

    proc print data=sd1.want;
    run;quit;

    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs    COMMERCIALID    COMMERCIALDATE    SALEID      SALEDATE    nearest_on_before                                     */
    /*                                                                                                                        */
    /*  1          C1           2014-01-01                         .             .                                            */
    /*  2          C2           2014-04-01        S1      2014-02-20           -40                                            */
    /*  3          C3           2014-07-01        S4      2014-07-01             0                                            */
    /*  4          C4           2014-09-15        S4      2014-07-01           -76                                            */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    In other words, the most recent sale prior to each commercial is said to “roll forward”, and the SaleDate is mapped to the CommercialDate.

    /*___                _   _                              _ _ _                _       _
    |___ \   _ __   __ _| |_(_)_   _____   _ __   _ __ ___ | | (_)_ __   __ _   (_) ___ (_)_ __
      __) | | `_ \ / _` | __| \ \ / / _ \ | `__| | `__/ _ \| | | | `_ \ / _` |  | |/ _ \| | `_ \
     / __/  | | | | (_| | |_| |\ V /  __/ | |    | | | (_) | | | | | | | (_| |  | | (_) | | | | |
    |_____| |_| |_|\__,_|\__|_| \_/ \___| |_|    |_|  \___/|_|_|_|_| |_|\__, | _/ |\___/|_|_| |_|
                                                                        |___/ |__/
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.commercials r=commercials;
    export data=sd1.sales       r=sales;
    submit;
    library(data.table);
    setDT(sales);
    setDT(commercials);
    setkey(sales, "SALEDATE");
    setkey(commercials, "COMMERCIALDATE");
    want<-sales[commercials,roll = TRUE];
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    proc print data=sd1.want;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /*    SALEID   SALEDATE COMMERCIALID                                                                                      */
    /* 1:   <NA> 2014-01-01           C1                                                                                      */
    /* 2:     S1 2014-04-01           C2                                                                                      */
    /* 3:     S4 2014-07-01           C3                                                                                      */
    /* 4:     S4 2014-09-15           C4                                                                                      */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* Obs    SALEID     SALEDATE    COMMERCIALID                                                                             */
    /*                                                                                                                        */
    /*  1               01JAN2014         C1                                                                                  */
    /*  2       S1      01APR2014         C2                                                                                  */
    /*  3       S4      01JUL2014         C3                                                                                  */
    /*  4       S4      15SEP2014         C4                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                  _
    |___ /   _ __ ___  __ _| |
      |_ \  | `__/ __|/ _` | |
     ___) | | |  \__ \ (_| | |
    |____/  |_|  |___/\__, |_|
                         |_|
    */

    proc datasets lib=sd1 ;
      modify sales;
      format _all_;
      informat _all_;
      modify commercials;
      format _all_;
      informat _all_;
    run;quit;

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc print data=sd1.sales;
    run;quit;
    proc r;
    export data=sd1.commercials r=commercials;
    export data=sd1.sales       r=sales;
    submit;
    library(sqldf);
    want<- sqldf("
       select
          commercialid
         ,commercialdate
         ,saledate
         ,dif      as nearest_on_before
       from
          (
           select
              l.commercialid
             ,l.commercialdate
             ,r.saleid
             ,r.saledate
             ,(r.saledate - l.commercialdate) as dif
           from
             commercials as l left join sales as r
           on
             r.saledate <= l.commercialdate
          )
       group
          by commercialid
       having
          max(dif) = dif or dif is NULL
       order
          by commercialid
    ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    proc print data=sd1.want;
    format  COMMERCIALDATE SALEDATE yymmdd10.;
    run;quit;
    ');


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*   commercialid commercialdate saledate nearest_on_before                                                               */
    /* 1           C1          19724       NA                NA                                                               */
    /* 2           C2          19814    19774               -40                                                               */
    /* 3           C3          19905    19905                 0                                                               */
    /* 4           C4          19981    19905               -76                                                               */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* Obs    COMMERCIALID    COMMERCIALDATE      SALEDATE    NEAREST_ON_BEFORE                                               */
    /*                                                                                                                        */
    /*  1          C1           2014-01-01               .             .                                                      */
    /*  2          C2           2014-04-01      2014-02-20           -40                                                      */
    /*  3          C3           2014-07-01      2014-07-01             0                                                      */
    /*  4          C4           2014-09-15      2014-07-01           -76                                                      */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*----  seems to work better without formats                             ----*/
    proc datasets lib=sd1 ;
      modify sales;
      format _all_;
      informat _all_;
      modify commercials;
      format _all_;
      informat _all_;
    run;quit;

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";
    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    proc python;
    export data=sd1.commercials python=commercials;
    export data=sd1.sales       python=sales;
    submit;
     from os import path;
     import pandas as pd;
     import numpy as np;
     import pandas as pd;
     from pandasql import sqldf;
     mysql = lambda q: sqldf(q, globals());
     from pandasql import PandaSQL;
     pdsql = PandaSQL(persist=True);
     sqlite3conn = next(pdsql.conn.gen).connection.connection;
     sqlite3conn.enable_load_extension(True);
     sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
     mysql = lambda q: sqldf(q, globals());
     print(sales);
     want=pdsql("""
       select
          commercialid
         ,commercialdate
         ,saleid
         ,saledate
         ,dif        as nearest_on_before
       from
          (
           select
              l.commercialid
             ,l.commercialdate
             ,r.saleid
             ,r.saledate
             ,(r.saledate - l.commercialdate) as dif
           from
             commercials as l left join sales as r
           on
             r.saledate <= l.commercialdate
          )
       group
          by commercialid
       having
          max(dif) = dif or dif is NULL
       order
          by commercialid
     """);
    print(want);
    endsubmit;
    import data=sd1.want python=want;
    run;quit;
    proc print data=sd1.want;
    format format SaleDate commercialdate yymmdd10.;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* Obs    COMMERCIALID    COMMERCIALDATE    SALEID      SALEDATE    NEAREST_ON_BEFORE                                     */
    /*                                                                                                                        */
    /*  1          C1           2014-01-01       None              .             .                                            */
    /*  2          C2           2014-04-01       S1       2014-02-20           -40                                            */
    /*  3          C3           2014-07-01       S4       2014-07-01             0                                            */
    /*  4          C4           2014-09-15       S4       2014-07-01           -76                                            */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
