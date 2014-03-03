# npm-ev-source

**work in progress**

The role of this module is to be a service, or a module used for a service takes
the standard npm data structure and splits and transforms it into the various
`event-sourced` documents it should be composed of.

This is very similar to
[`npm-fullfat-registry`](https://github.com/npm/npm-fullfat-registry) except
forms the data based on a new model.

The main idea here is to take each current document, and divide it into `n` number
of documents where `n` is the number of versions for the package. Each document
will have an `_id` of `package@version` and a `name` property of `package`.

## Algorithm

```

Listen on changes of SkimDB

On each change
  pause feed

  get versions
  (query view to get list of versions that exist in ev db)
  (filter versions based on this view)

  for v in versions
    fetch attachment/tarball from whichever source

    **Note**No need to do any diffs here as we are assuming each version
    is an atomic unit that will not be changed again

    Insert each new version that was fetched along with the tarball as
    `multipart/related` so we can stream all the things.

  resume feed

```
**note** what is in parens is currently not implemented in the present version.

# TODO:

1. Stars
  These need to be their own document as well, we will make creating these
  secondary for now as it is not crucial and can easily be added.

1. Dist-tags
  These will need to be resolved in the view on install as we will need
  a mapping to tag->version.
