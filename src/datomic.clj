(ns datomic
  (:require
    [datomic.api :as d]
    [clojure.edn :as edn]
    [clojure.java.io :as io]))

(def db-url "datomic:free://localhost:4334/imdb")

(d/create-database db-url)
(def conn (d/connect db-url))
(defn db [] (d/db conn))

;; Hint: сделайте extenal id из feature :id
(def schema [
  [:db/add (d/tempid :db.part/user) :db/ident :series] ;; ? or better :feature.type/series
  [:db/add (d/tempid :db.part/user) :db/ident :episode]
  [:db/add (d/tempid :db.part/user) :db/ident :movie]
  [:db/add (d/tempid :db.part/user) :db/ident :video]
  [:db/add (d/tempid :db.part/user) :db/ident :tv-movie]
  [:db/add (d/tempid :db.part/user) :db/ident :videogame]

  {:db/ident :feature/type
   :db/valueType :db.type/ref
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}

  {:db/ident :feature/id
   :db/valueType :db.type/string
   :db/unique :db.unique/identity ;; I think identity here is better than value, because there is no need to have whole db unique values; This needs to use lookup ref feature
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}

  {:db/ident :feature/title
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}

  {:db/ident :feature/year
   :db/valueType :db.type/long
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}

  {:db/ident :feature/endyear
   :db/valueType :db.type/long
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}

  {:db/ident :feature/series
   :db/valueType :db.type/string
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}

  {:db/ident :feature/season
   :db/valueType :db.type/long
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}

  {:db/ident :feature/episode
   :db/valueType :db.type/long
   :db/cardinality :db.cardinality/one
   :db/id (d/tempid :db.part/db)
   :db.install/_attribute :db.part/db}
])

(defn reset []
  (d/release conn)
  (d/delete-database db-url)
  (d/create-database db-url)
  (alter-var-root #'conn (constantly (d/connect db-url)))
  @(d/transact conn schema))
;; (reset)
;; Формат файла:
;; { :type  =>   :series | :episode | :movie | :video | :tv-movie | :videogame
;;   :id    =>   str,  unique feature id
;;   :title =>   str,  feature title. Optional for episodes
;;   :year  =>   long, year of release. For series, this is starting year of the series
;;   :endyear => long, only for series, optional. Ending year of the series. If omitted, series still airs
;;   :series  => str,  only for episode. Id of enclosing series
;;   :season  => long, only for episode, optional. Season number
;;   :episode => long, only for episode, optional. Episode number
;; }
;; hint: воспользуйтесь lookup refs чтобы ссылаться на features по внешнему :id
(defn import []
  (with-open [rdr (io/reader "features.2014.edn")]
    (doseq [line (line-seq rdr)
            :let [feature (edn/read-string line)]]
      (d/transact conn [{:db/id (d/tempid :db.part/user)
                         :feature/type (:type feature)
                         :feature/id (:id feature)
                         :feature/title (:title feature)
                       ;;  :feature/year (:year feature)
                         }]))))
;;(import)
;; Найти все пары entity указанных типов с совпадающими названиями
;; Например, фильм + игра с одинаковым title
;; Вернуть #{[id1 id2], ...}
;; hint: data patterns
;; (println (d/q '[:find ?n ?id
;;        :where
;;        [?n :feature/id ?id]]
;;      (d/db conn)))
;; (d/q '[:find ?n ?id
;;        :where
;;        [?n :feature/id ?id]]
;;      (d/db conn))
;; (println "hello datomic")
(defn siblings [db type1 type2]
  :TODO)

;; Найти сериал(ы) с самым ранним годом начала
;; Вернуть #{[id year], ...}
;; hint: aggregates

(defn oldest-series [db]
  :TODO)

;; Найти 3 сериала с наибольшим количеством серий в сезоне
;; Вернуть [[id season series-count], ...]
;; hint: aggregates, grouping

(defn longest-season [db]
  :TODO)

;; Найти 5 самых популярных названий (:title). Названия эпизодов не учитываются
;; Вернуть [[count title], ...]
;; hint: aggregation, grouping, predicates

(defn popular-titles [db]
  :TODO)
