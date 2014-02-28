# npm-ev-source

The role of this module is to be a service that takes the standard npm data
structure and splits and transforms it into the various event-sourced documents
it should be composed of.

The main idea here is to take each current document, and divide it into `n` number
of documents where `n` is the number of versions for the package. Each document
will have an `_id` of `package@version` and a `name` property of `package`.

## Algorithm

```

Listen on changes of SkimDB

On each change
  pause feed

  get versions
  query view to get list of versions that exist in ev db
  filter versions based on this view
  iterate through only the needed versions

  for v in versions
    fetch attachment/tarball from whichever source
    **Note**No need to do any diffs here as we are assuming each version
    is an atomic unit that will not be changed again

Insert each new version that was fetched along with the tarball as
`multipart/related` or read the whole file and do one atomic PUT.
Whichever seems more sane.

```

# Questions

Metadata that CAN be changed and is a generic property of the package.

1. README contents + readmeFilename
  This either needs to be in its own separate document or be attached to each
  document with the latest one winning in the merged document. I'm thinking we
  should just make sure it is trimmed and just put it in with the version
  because it does make sense to be in there

1. Stars
  These need to be their own document as well, we will make creating these
  secondary for now as it is not crucial and can easily be added.

1. Dist-tags
  These will need to be resolved in the view on install as we will need
  a mapping to tag->version.
