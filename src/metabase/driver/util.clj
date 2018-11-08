(ns metabase.driver.util
  "Utility functions for common operations on drivers."
  (:require [clojure.tools.logging :as log]
            [metabase
             [config :as config]
             [driver :as driver]
             [util :as u]]
            [metabase.models.database :refer [Database]]
            [metabase.util.i18n :refer [trs]]
            [toucan.db :as db]))

(def ^:private can-connect-timeout-ms
  "Consider `can-connect?`/`can-connect-with-details?` to have failed after this many milliseconds.
   By default, this is 5 seconds. You can configure this value by setting the env var `MB_DB_CONNECTION_TIMEOUT_MS`."
  (or (config/config-int :mb-db-connection-timeout-ms)
      5000))

(defn can-connect-with-details?
  "Check whether we can connect to a database with ENGINE and DETAILS-MAP and perform a basic query
   such as `SELECT 1`. Specify optional param RETHROW-EXCEPTIONS if you want to handle any exceptions
   thrown yourself (e.g., so you can pass the exception message along to the user).

     (can-connect-with-details? :postgres {:host \"localhost\", :port 5432, ...})"
  ^Boolean [driver details-map & [rethrow-exceptions]]
  {:pre [(keyword? driver) (map? details-map)]}
  (try
    (u/with-timeout can-connect-timeout-ms
      (driver/can-connect? driver details-map))
    (catch Throwable e
      (log/error (trs "Failed to connect to database: {0}" (.getMessage e)))
      (when rethrow-exceptions
        (throw (Exception. (driver/humanize-connection-error-message driver (.getMessage e)))))
      false)))


(defn report-timezone-if-supported
  "Returns the report-timezone if `driver` supports setting it's timezone and a report-timezone has been specified by
  the user."
  [driver]
  (when (driver/supports? driver :set-timezone)
    (let [report-tz (driver/report-timezone)]
      (when-not (empty? report-tz)
        report-tz))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                               Driver Resolution                                                |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- database->driver* [database-or-id]
  (db/select-one-field :engine Database, :id (u/get-id database-or-id)))

(def ^{:arglists '([database-or-id])} database->driver
  "Memoized function that returns the driver instance that should be used for `Database` with ID. (Databases aren't
  expected to change their types, and this optimization makes things a lot faster)."
  (memoize database->driver*))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                              Loading all Drivers                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn find-and-load-all-drivers!
  "Search classpath for namespaces that start with `metabase.driver.`, then `require` them, which should register them
  as a side-effect. Note that this will not load drivers added by 3rd-party plugins; they must register themselves
  appropriately when initialized by `load-plugins!`.

  This really only needs to be done by the public settings API endpoint to populate the list of available drivers.
  Please avoid using this function elsewhere, as loading all of these namespaces can be quite expensive!"
  []
  (doseq [ns-symb @u/metabase-namespace-symbols
          :when   (re-matches #"^metabase\.driver\.[a-z0-9_]+$" (name ns-symb))]
    (u/ignore-exceptions
      (#'driver/load-driver-namespace-if-needed ns-symb))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             Available Drivers Info                                             |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn features
  "Return a set of all features supported by `driver`."
  [driver]
  (set (for [feature driver/driver-features
             :when (driver/supports? driver feature)]
         feature)))

(defn available-drivers []
  (set (for [driver (descendants driver/hierarchy ::driver)
             :when  (driver/available? driver)]
         driver)))

(defn available-drivers-info
  "Info about available drivers."
  []
  (into {} (for [driver (available-drivers)]
             [driver {:details-fields (driver/details-fields driver)
                      :driver-name    (driver/display-name driver)
                      :features       (features driver)}])))
