use anyhow::Result;

/// An iterator over a consistent set of keys and values.
///
/// Iterators are implemented for `KvEngine`s and for `Snapshot`s. They see a
/// consistent view of the database; an iterator created by an engine behaves as
/// if a snapshot was created first, and the iterator created from the snapshot.
///
/// Most methods on iterators will panic if they are not "valid",
/// as determined by the `valid` method.
/// An iterator is valid if it is currently "pointing" to a key/value pair.
///
/// Iterators begin in an invalid state; one of the `seek` methods
/// must be called before beginning iteration.
/// Iterators may become invalid after a failed `seek`,
/// or after iteration has ended after calling `next` or `prev`,
/// and they return `false`.
pub trait Iterator {
    /// Move the iterator to a specific key.
    ///
    /// When an exact match is not found, `seek` sets the iterator to the next
    /// key greater than that specified as `key`, if such a key exists;
    /// `seek_for_prev` sets the iterator to the previous key less than
    /// that specified as `key`, if such a key exists.
    ///
    /// # Returns
    ///
    /// `true` if seeking succeeded and the iterator is valid,
    /// `false` if seeking failed and the iterator is invalid.
    fn seek(&mut self, key: &[u8]) -> Result<bool>;

    /// Move the iterator to a specific key.
    ///
    /// For the difference between this method and `seek`,
    /// see the documentation for `seek`.
    ///
    /// # Returns
    ///
    /// `true` if seeking succeeded and the iterator is valid,
    /// `false` if seeking failed and the iterator is invalid.
    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool>;

    /// Seek to the first key in the engine.
    fn seek_to_first(&mut self) -> Result<bool>;

    /// Seek to the last key in the database.
    fn seek_to_last(&mut self) -> Result<bool>;

    /// Move a valid iterator to the previous key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn prev(&mut self) -> Result<bool>;

    /// Move a valid iterator to the next key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn next(&mut self) -> Result<bool>;

    /// Retrieve the current key.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn key(&self) -> &[u8];

    /// Retrieve the current value.
    ///
    /// # Panics
    ///
    /// If the iterator is invalid, iterator may panic or aborted.
    fn value(&self) -> &[u8];

    /// Returns `true` if the iterator points to a `key`/`value` pair.
    fn valid(&self) -> Result<bool>;
}
