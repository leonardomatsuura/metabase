(ns metabase.driver.common
  "Shared definitions and helper functions for use across different drivers."
  (:require [clj-time
             [coerce :as tcoerce]
             [core :as time]
             [format :as tformat]]
            [clojure.tools.logging :as log]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver.util :as driver.u]
            [metabase.query-processor.store :as qp.store]
            [metabase.util.i18n :refer [trs tru]])
  (:import java.text.SimpleDateFormat
           org.joda.time.DateTime
           org.joda.time.format.DateTimeFormatter))

(def connection-error-messages
  "Generic error messages that drivers should return in their implementation of `humanize-connection-error-message`."
  {:cannot-connect-check-host-and-port (str (tru "Hmm, we couldn''t connect to the database.")
                                            " "
                                            (tru "Make sure your host and port settings are correct"))
   :ssh-tunnel-auth-fail               (str (tru "We couldn''t connect to the ssh tunnel host.")
                                            " "
                                            (tru "Check the username, password."))
   :ssh-tunnel-connection-fail         (str (tru "We couldn''t connect to the ssh tunnel host.")
                                            " "
                                            (tru "Check the hostname and port."))
   :database-name-incorrect            (tru "Looks like the database name is incorrect.")
   :invalid-hostname                   (str (tru "It looks like your host is invalid.")
                                            " "
                                            (tru "Please double-check it and try again."))
   :password-incorrect                 (tru "Looks like your password is incorrect.")
   :password-required                  (tru "Looks like you forgot to enter your password.")
   :username-incorrect                 (tru "Looks like your username is incorrect.")
   :username-or-password-incorrect     (tru "Looks like the username or password is incorrect.")})

(def default-host-details
  "Map of the db host details field, useful for `details-fields` implementations"
  {:name         "host"
   :display-name (tru "Host")
   :default      "localhost"})

(def default-port-details
  "Map of the db port details field, useful for `details-fields` implementations. Implementations should assoc a
  `:default` key."
  {:name         "port"
   :display-name (tru "Port")
   :type         :integer})

(def default-user-details
  "Map of the db user details field, useful for `details-fields` implementations"
  {:name         "user"
   :display-name (tru "Database username")
   :placeholder  (tru "What username do you use to login to the database?")
   :required     true})

(def default-password-details
  "Map of the db password details field, useful for `details-fields` implementations"
  {:name         "password"
   :display-name (tru "Database password")
   :type         :password
   :placeholder  "*******"})

(def default-dbname-details
  "Map of the db name details field, useful for `details-fields` implementations"
  {:name         "dbname"
   :display-name (tru "Database name")
   :placeholder  (tru "birds_of_the_world")
   :required     true})

(def default-ssl-details
  "Map of the db ssl details field, useful for `details-fields` implementations"
  {:name         "ssl"
   :display-name (tru "Use a secure connection (SSL)?")
   :type         :boolean
   :default      false})

(def default-additional-options-details
  "Map of the db `additional-options` details field, useful for `details-fields` implementations. Should assoc a
  `:placeholder` key"
  {:name         "additional-options"
   :display-name (tru "Additional JDBC connection string options")})


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           Fetching Current Timezone                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defprotocol ^:private ParseDateTimeString
  (^:private parse [this date-time-str] "Parse the `date-time-str` and return a `DateTime` instance"))

(extend-protocol ParseDateTimeString
  DateTimeFormatter
  (parse [formatter date-time-str]
    (tformat/parse formatter date-time-str)))

;; Java's SimpleDateFormat is more flexible on what it accepts for a time zone identifier. As an example, CEST is not
;; recognized by Joda's DateTimeFormatter but is recognized by Java's SimpleDateFormat. This defrecord is used to
;; dispatch parsing for SimpleDateFormat instances. Dispatching off of the SimpleDateFormat directly wouldn't be good
;; as it's not threadsafe. This will always create a new SimpleDateFormat instance and discard it after parsing the
;; date
(defrecord ^:private ThreadSafeSimpleDateFormat [format-str]
  :load-ns true
  ParseDateTimeString
  (parse [_ date-time-str]
    (let [sdf         (SimpleDateFormat. format-str)
          parsed-date (.parse sdf date-time-str)
          joda-tz     (-> sdf .getTimeZone .getID time/time-zone-for-id)]
      (time/to-time-zone (tcoerce/from-date parsed-date) joda-tz))))

(defn create-db-time-formatters
  "Creates date formatters from `DATE-FORMAT-STR` that will preserve the offset/timezone information. Will return a
  JodaTime date formatter and a core Java SimpleDateFormat. Results of this are threadsafe and can safely be def'd."
  [date-format-str]
  [(.withOffsetParsed ^DateTimeFormatter (tformat/formatter date-format-str))
   (ThreadSafeSimpleDateFormat. date-format-str)])

(defn- first-successful-parse
  "Attempt to parse `time-str` with each of `date-formatters`, returning the first successful parse. If there are no
  successful parses throws the exception that the last formatter threw."
  [date-formatters time-str]
  (or (some #(u/ignore-exceptions (parse % time-str)) date-formatters)
      (doseq [formatter (reverse date-formatters)]
        (parse formatter time-str))))

(defn current-db-time [native-query date-formatters driver database]
  {:pre [(map? database)]}
  (let [settings (when-let [report-tz (driver.u/report-timezone-if-supported driver)]
                   {:settings {:report-timezone report-tz}})
        time-str (try
                   (qp.store/with-store
                     (qp.store/store-database! database)
                     (->> (merge settings {:database database, :native {:query native-query}})
                          (driver/execute-query driver)
                          :rows
                          ffirst))
                   (catch Exception e
                     (throw
                      (Exception.
                       (format "Error querying database '%s' for current time" (:name database)) e))))]
    (try
      (when time-str
        (first-successful-parse date-formatters time-str))
      (catch Exception e
        (throw
         (Exception.
          (str
           (tru "Unable to parse date string ''{0}'' for database engine ''{1}''"
                time-str (-> database :engine name))) e))))))

(defn ^:deprecated make-current-db-time-fn
  "Takes a clj-time date formatter `date-formatter` and a native query for the current time. Returns a function that
  executes the query and parses the date returned preserving it's timezone"
  [native-query date-formatters]
  (partial current-db-time native-query date-formatters))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                               Class -> Base Type                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn class->base-type
  "Return the `Field.base_type` that corresponds to a given class returned by the DB.
   This is used to infer the types of results that come back from native queries."
  [klass]
  (or (some (fn [[mapped-class mapped-type]]
              (when (isa? klass mapped-class)
                mapped-type))
            [[Boolean                        :type/Boolean]
             [Double                         :type/Float]
             [Float                          :type/Float]
             [Integer                        :type/Integer]
             [Long                           :type/Integer]
             [java.math.BigDecimal           :type/Decimal]
             [java.math.BigInteger           :type/BigInteger]
             [Number                         :type/Number]
             [String                         :type/Text]
             [java.sql.Date                  :type/Date]
             [java.sql.Timestamp             :type/DateTime]
             [java.util.Date                 :type/DateTime]
             [DateTime                       :type/DateTime]
             [java.util.UUID                 :type/Text]       ; shouldn't this be :type/UUID ?
             [clojure.lang.IPersistentMap    :type/Dictionary]
             [clojure.lang.IPersistentVector :type/Array]
             [org.bson.types.ObjectId        :type/MongoBSONID]
             [org.postgresql.util.PGobject   :type/*]
             [nil                            :type/*]]) ; all-NULL columns in DBs like Mongo w/o explicit types
      (log/warn (trs "Don''t know how to map class ''{0}'' to a Field base_type, falling back to :type/*." klass))
      :type/*))

(defn values->base-type
  "Given a sequence of VALUES, return the most common base type."
  [values]
  (->> values
       (take 100)                                   ; take up to 100 values
       (remove nil?)                                ; filter out `nil` values
       (group-by (comp class->base-type class))     ; now group by their base-type
       (sort-by (comp (partial * -1) count second)) ; sort the map into pairs of [base-type count] with highest count as first pair
       ffirst))                                     ; take the base-type from the first pair
