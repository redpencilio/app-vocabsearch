defmodule Dispatcher do
  use Matcher
  define_accept_types [
    html: [ "text/html", "application/xhtml+html" ],
    json: [ "application/json", "application/vnd.api+json" ],
    any: [ "*/*" ]
  ]

  @html %{ accept: %{ html: true } }
  @json %{ accept: %{ json: true } }
  @any %{ accept: %{ any: true } }

  # In order to forward the 'themes' resource to the
  # resource service, use the following forward rule:
  #
  # match "/themes/*path", @json do
  #   Proxy.forward conn, path, "http://resource/themes/"
  # end
  #
  # Run `docker-compose restart dispatcher` after updating
  # this file.

  get "/webcomponent/main.js", @any do
    forward conn, [], "http://webcomponent/vocab-search-bar.js"
  end

  post "/vocab-download-jobs/:id/run", @json do
    forward conn, [], "http://vocab-fetch/" <> id
  end

  post "/dataset-generation-jobs/:id/run", @json do
    forward conn, [], "http://content-unification/generate_void/" <> id
  end

  post "/content-unification-jobs/:id/run", @json do
    forward conn, [], "http://content-unification/" <> id
  end

  delete "/vocabularies/:id", @any do
    forward conn, [], "http://content-unification/delete-vocabulary/" <> id
  end

  match "/dataset-types/*path", @json do
    forward conn, path, "http://resource/dataset-types/"
  end

  match "/vocabularies/*path", @json do
    forward conn, path, "http://resource/vocabularies/"
  end

  match "/shacl-property-shapes/*path", @json do
    forward conn, path, "http://resource/shacl-property-shapes/"
  end

  match "/shacl-node-shapes/*path", @json do
    forward conn, path, "http://resource/shacl-node-shapes/"
  end

  match "/datasets/*path", @json do
    forward conn, path, "http://resource/datasets/"
  end

  match "/jobs/*path", @json do
    forward conn, path, "http://resource/jobs/"
  end

  match "/tasks/*path", @json do
    forward conn, path, "http://resource/tasks/"
  end

  get "/files/:id/download", @any do
    Proxy.forward conn, [], "http://file/files/" <> id <> "/download"
  end

  post "/files/*path", @any do
    Proxy.forward conn, path, "http://file/files/"
  end

  delete "/files/*path", %{ accept: [ :json ] } do
    Proxy.forward conn, path, "http://file/files/"
  end

  get "/files/*path", @json do
    Proxy.forward conn, path, "http://resource/files/"
  end

  match "/data-containers/*path", @json do
    forward conn, path, "http://resource/data-containers/"
  end

  match "/content-unification-jobs/*path", @json do
    forward conn, path, "http://resource/content-unification-jobs/"
  end

  match "/metadata-extraction-jobs/*path", @json do
    forward conn, path, "http://resource/metadata-extraction-jobs/"
  end

  match "/concepts/search", @any do
    forward conn, [], "http://search/concepts/search"
  end

  get "/concepts/*path", @json do
    forward conn, path, "http://resource/concepts/"
  end

  match "/assets/*path", @any do
    forward conn, path, "http://frontend/assets/"
  end

  match "/webcomponent/*path", @any do
    forward conn, path, "http://webcomponent/"
  end

  match "/*_path", @html do
    # *_path allows a path to be supplied, but will not yield
    # an error that we don't use the path variable.
    forward conn, [], "http://frontend/index.html"
  end

  match "/*_", %{ last_call: true, accept: %{ json: true } } do
    send_resp( conn, 404, "{ \"error\": { \"code\": 404, \"message\": \"Route not found.  See config/dispatcher.ex\" } }" )
  end

  match "/*_", %{ last_call: true } do
    send_resp( conn, 404, "Route not found.  See config/dispatcher.ex" )
  end
end
