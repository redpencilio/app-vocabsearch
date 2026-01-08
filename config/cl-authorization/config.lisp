;;;;;;;;;;;;;;;;;;;
;;; delta messenger
(in-package :delta-messenger)

(add-delta-logger)
(add-delta-messenger "http://delta-notifier/")

;;;;;;;;;;;;;;;;;
;;; configuration
(in-package :client)
(setf *log-sparql-query-roundtrip* t)
(setf *backend* "http://triplestore:8890/sparql")

(in-package :server)
(setf *log-incoming-requests-p* nil)

;;;;;;;;;;;;;;;;;
;;; access rights
(in-package :acl)

(defparameter *access-specifications* nil
  "All known ACCESS specifications.")

(defparameter *graphs* nil
  "All known GRAPH-SPECIFICATION instances.")

(defparameter *rights* nil
  "All known GRANT instances connecting ACCESS-SPECIFICATION to GRAPH.")

(type-cache::add-type-for-prefix "http://mu.semte.ch/sessions/" "http://mu.semte.ch/vocabularies/session/Session")

(define-graph public ("http://mu.semte.ch/graphs/public")
    ("http://mu.semte.ch/vocabularies/ext/VocabularyMeta" -> _)
    ("http://mu.semte.ch/vocabularies/ext/DatasetType" -> _)
    ("http://www.w3.org/ns/shacl#NodeShape" -> _)
    ("http://www.w3.org/ns/shacl#PropertyShape" -> _)
    ("http://mu.semte.ch/vocabularies/ext/VocabularyMeta" -> _)
    ("http://rdfs.org/ns/void#Dataset" -> _)
    ("http://mu.semte.ch/vocabularies/ext/VocabDownloadJob" -> _)
    ("http://mu.semte.ch/vocabularies/ext/MetadataExtractionJob" -> _)
    ("http://mu.semte.ch/vocabularies/ext/ContentUnificationJob" -> _)
    ("http://mu.semte.ch/vocabularies/ext/VocabDeleteJob" -> _)
    ("http://mu.semte.ch/vocabularies/ext/VocabsExportJob" -> _)
    ("http://mu.semte.ch/vocabularies/ext/VocabsImportJob" -> _)
    ("http://vocab.deri.ie/cogs#Job" -> _)
    ("http://redpencil.data.gift/vocabularies/tasks/Task" -> _)
    ("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#DataContainer" -> _)
    ("http://open-services.net/ns/core#Error" -> _)
    ("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject" -> _)
)

(supply-allowed-group "public")

(grant (read write)
  :to-graph (public)
  :for-allowed-group "public")
