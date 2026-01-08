alias Acl.Accessibility.Always, as: AlwaysAccessible
alias Acl.GraphSpec.Constraint.Resource, as: ResourceConstraint
alias Acl.GraphSpec, as: GraphSpec
alias Acl.GroupSpec, as: GroupSpec
alias Acl.GroupSpec.GraphCleanup, as: GraphCleanup

defmodule Acl.UserGroups.Config do
  def user_groups do
    # These elements are walked from top to bottom.  Each of them may
    # alter the quads to which the current query applies.  Quads are
    # represented in three sections: current_source_quads,
    # removed_source_quads, new_quads.  The quads may be calculated in
    # many ways.  The useage of a GroupSpec and GraphCleanup are
    # common.
    [
      # // PUBLIC
      %GroupSpec{
        name: "public",
        useage: [:read, :write, :read_for_write],
        access: %AlwaysAccessible{},
        graphs: [ %GraphSpec{
                    graph: "http://mu.semte.ch/graphs/public",
                    constraint: %ResourceConstraint{
                      resource_types: [
                        "http://mu.semte.ch/vocabularies/ext/VocabularyMeta",
                        "http://mu.semte.ch/vocabularies/ext/DatasetType",
                        "http://www.w3.org/ns/shacl#NodeShape",
                        "http://www.w3.org/ns/shacl#PropertyShape",
                        "http://mu.semte.ch/vocabularies/ext/VocabularyMeta",
                        "http://rdfs.org/ns/void#Dataset",
                        "http://mu.semte.ch/vocabularies/ext/VocabDownloadJob",
                        "http://mu.semte.ch/vocabularies/ext/MetadataExtractionJob",
                        "http://mu.semte.ch/vocabularies/ext/ContentUnificationJob",
                        "http://mu.semte.ch/vocabularies/ext/VocabDeleteJob",
                        "http://mu.semte.ch/vocabularies/ext/VocabsExportJob",
                        "http://mu.semte.ch/vocabularies/ext/VocabsImportJob",
                        "http://vocab.deri.ie/cogs#Job",
                        "http://redpencil.data.gift/vocabularies/tasks/Task",
                        "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#DataContainer",
                        "http://open-services.net/ns/core#Error",
                        "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject",
                      ]
                    } } ] },

      # // CLEANUP
      #
      %GraphCleanup{
        originating_graph: "http://mu.semte.ch/application",
        useage: [:write],
        name: "clean"
      }
    ]
  end
end
