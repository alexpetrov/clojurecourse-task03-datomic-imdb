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
   :db/unique :db.unique/identity ;; I think identity here is better than value, because there is no need to have whole db unique values; This needs to use lookup ref feature. Also it make entity upsertable
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

(defn set-year [feature entity]
  (if (contains? feature :year)
    (assoc entity :feature/year (:year feature))
    entity))

(defn set-series [feature entity]
  (if (contains? feature :series)
    (assoc entity :feature/series (:series feature))
    entity))

(defn set-endyear [feature entity]
  (if (contains? feature :endyear)
    (assoc entity :feature/endyear (:endyear feature))
    entity))

(defn set-episode [feature entity]
  (if (contains? feature :episode)
    (assoc entity :feature/episode (:episode feature))
    entity))

(defn set-season [feature entity]
  (if (contains? feature :season)
    (assoc entity :feature/season (:season feature))
    entity))


(defn construct-feature [feature]
  [(->> {:db/id (d/tempid :db.part/user)
        :feature/type (:type feature)
        :feature/id (:id feature)
        :feature/title (:title feature)}
        (set-year feature)
        (set-series feature)
        (set-endyear feature)
        (set-episode feature)
        (set-season feature))])

;; (construct-feature {:type :series, :id "\"#JustDating\" (2014)", :title "#JustDating", :year 2014})

;; (construct-feature {:type :series, :id "\"30 for 30\" (2009)", :title "30 for 30", :year 2009, :endyear 2014})

;; (construct-feature {:type :episode, :id "\"#JustDating\" (2014) {(#1.1)}", :series "\"#JustDating\" (2014)", :season 1, :episode 1, :year 2014})


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
;; TODO: Сделать функцию, которая будет формировать entity для feature
(defn import []
  (with-open [rdr (io/reader "features.2014.edn")]
    (doseq [line (line-seq rdr)
            :let [feature (edn/read-string line)]]
      (d/transact conn (construct-feature feature)))))
;;(reset)
;;(import)
;; Найти все пары entity указанных типов с совпадающими названиями
;; Например, фильм + игра с одинаковым title
;; Вернуть #{[id1 id2], ...}
;; hint: data patterns
;; (println (d/q '[:find (first ?n) :where [?n :feature/id "\"#JustDating\" (2014)"]] (d/db conn)))


;; (println (d/q '[:find ?year :where [?n :feature/year ?year]] (d/db conn)))
;; (println (d/q '[:find ?title :where [?n :feature/title ?title]] (d/db conn)))

;; (println (d/q '[:find ?endyear :where [?n :feature/endyear ?endyear]] (d/db conn)))
;; (println (d/q '[:find ?n ?series :where [?n :feature/series ?series]] (d/db conn)))
;; (println (d/q '[:find (first ?season) :where [?n :feature/season ?season]] (d/db conn)))
;; (println (d/q '[:find (first ?n) ?episode :where [?n :feature/episode ?episode]] (d/db conn)))
;; (println (d/q '[:find ?n ?type ?dbident :where [?n :feature/type ?type][?type :db/ident ?dbident]] (d/db conn)))


;; (d/q '[:find ?n ?id :where [?n :feature/id ?id]] (d/db conn))
;; (println "hello datomic")
;; (siblings (d/db conn) :movie :videogame)
(defn siblings [db type1 type2]
  (d/q '[:find ?id1 ?id2
         :in $ ?type1 ?type2
         :where
         [?f1 :feature/title ?t]
         [?f2 :feature/title ?t]
         [(not= ?f1 ?f2)]
         [?f1 :feature/type ?type1]
         [?f2 :feature/type ?type2]
         [?f1 :feature/id ?id1]
         [?f2 :feature/id ?id2]]
       db type1 type2))

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
