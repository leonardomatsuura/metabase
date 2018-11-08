(ns metabase.driver.vertica
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]
             [util :as u]]

            [metabase.util
             [date :as du]
             [honeysql-extensions :as hx]
             [ssh :as ssh]]))

(def ^:private database-type->base-type
  "Map of Vertica column types -> Field base types. Add more mappings here as you come across them."
  {:Boolean        :type/Boolean
   :Integer        :type/Integer
   :Bigint         :type/BigInteger
   :Varbinary      :type/*
   :Binary         :type/*
   :Char           :type/Text
   :Varchar        :type/Text
   :Money          :type/Decimal
   :Numeric        :type/Decimal
   :Double         :type/Decimal
   :Float          :type/Float
   :Date           :type/Date
   :Time           :type/Time
   :TimeTz         :type/Time
   :Timestamp      :type/DateTime
   :TimestampTz    :type/DateTime
   :AUTO_INCREMENT :type/Integer
   (keyword "Long Varchar")   :type/Text
   (keyword "Long Varbinary") :type/*})

(defmethod sql-jdbc.conn/connection-details->spec :vertica [_ {:keys [host port db dbname]
                                                           :or   {host "localhost", port 5433, db ""}
                                                           :as   details}]
  (-> (merge {:classname   "com.vertica.jdbc.Driver"
              :subprotocol "vertica"
              :subname     (str "//" host ":" port "/" (or dbname db))}
             (dissoc details :host :port :dbname :db :ssl))
      (sql-jdbc.common/handle-additional-options details)))

(defmethod sql.qp/unix-timestamp->timestamp [:vertica :seconds] [_ _ expr]
  (hsql/call :to_timestamp expr))

(defn- cast-timestamp
  "Vertica requires stringified timestamps (what Date/DateTime/Timestamps are converted to) to be cast as timestamps
  before date operations can be performed. This function will add that cast if it is a timestamp, otherwise this is a
  noop."
  [expr]
  (if (du/is-temporal? expr)
    (hx/cast :timestamp expr)
    expr))

(defn- date-trunc [unit expr] (hsql/call :date_trunc (hx/literal unit) (cast-timestamp expr)))
(defn- extract    [unit expr] (hsql/call :extract    unit              expr))

(def ^:private extract-integer (comp hx/->integer extract))

(def ^:private ^:const one-day (hsql/raw "INTERVAL '1 day'"))

(defn- date [unit expr]
  (case unit
    :default         expr
    :minute          (date-trunc :minute expr)
    :minute-of-hour  (extract-integer :minute expr)
    :hour            (date-trunc :hour expr)
    :hour-of-day     (extract-integer :hour expr)
    :day             (hx/->date expr)
    :day-of-week     (hx/inc (extract-integer :dow expr))
    :day-of-month    (extract-integer :day expr)
    :day-of-year     (extract-integer :doy expr)
    :week            (hx/- (date-trunc :week (hx/+ (cast-timestamp expr)
                                                   one-day))
                           one-day)
    ;:week-of-year    (extract-integer :week (hx/+ expr one-day))
    :week-of-year    (hx/week expr)
    :month           (date-trunc :month expr)
    :month-of-year   (extract-integer :month expr)
    :quarter         (date-trunc :quarter expr)
    :quarter-of-year (extract-integer :quarter expr)
    :year            (extract-integer :year expr)))

(defmethod driver/date-interval [unit amount]
  (hsql/raw (format "(NOW() + INTERVAL '%d %s')" (int amount) (name unit))))

(defn- materialized-views
  "Fetch the Materialized Views for a Vertica DATABASE.
   These are returned as a set of maps, the same format as `:tables` returned by `describe-database`."
  [database]
  (try (set (jdbc/query (sql-jdbc.conn/db->jdbc-connection-spec database)
                        ["SELECT TABLE_SCHEMA AS \"schema\", TABLE_NAME AS \"name\" FROM V_CATALOG.VIEWS;"]))
       (catch Throwable e
         (log/error "Failed to fetch materialized views for this database:" (.getMessage e)))))

(defn- describe-database
  "Custom implementation of `describe-database` for Vertica."
  [driver database]
  (update (sql/describe-database driver database) :tables (u/rpartial set/union (materialized-views database))))

(defmethod sql.qp/string-length-fn [field-key]
  (hsql/call :char_length (hx/cast :Varchar field-key)))


(defrecord VerticaDriver []
  :load-ns true
  clojure.lang.Named
  (getName [_] "Vertica"))

(def ^:private vertica-date-formatters (driver.common/create-db-time-formatters "yyyy-MM-dd HH:mm:ss z"))
(def ^:private vertica-db-time-query "select to_char(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZ')")

(u/strict-extend VerticaDriver
  driver/IDriver
  (merge (sql/IDriverSQLDefaultsMixin)
         {:date-interval     (u/drop-first-arg date-interval)
          :describe-database describe-database
          :details-fields    (constantly (ssh/with-tunnel-config
                                           [driver.common/default-host-details
                                            (assoc driver.common/default-port-details :default 5433)
                                            driver.common/default-dbname-details
                                            driver.common/default-user-details
                                            driver.common/default-password-details
                                            (assoc driver.common/default-additional-options-details
                                              :placeholder "ConnectionLoadBalance=1")]))
          :current-db-time   (driver.common/current-db-time vertica-db-time-query vertica-date-formatters)})
  sql/ISQLDriver
  (merge (sql/ISQLDriverDefaultsMixin)
         {:database-type->base-type         (u/drop-first-arg database-type->base-type)
          :connection-details->spec  (u/drop-first-arg connection-details->spec)
          :date                      (u/drop-first-arg date)
          :set-timezone-sql          (constantly "SET TIME ZONE TO %s;")
          :string-length-fn          (u/drop-first-arg string-length-fn)
          :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)}))

(defn -init-driver
  "Register the Vertica driver when found on the classpath"
  []
  ;; only register the Vertica driver if the JDBC driver is available
  (when (u/ignore-exceptions
         (Class/forName "com.vertica.jdbc.Driver"))
    (driver/register-driver! :vertica :vertica)))
