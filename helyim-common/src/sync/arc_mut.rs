/// copied from https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq/src/arc_mut.rs
use core::fmt;
use std::{
    cell::SyncUnsafeCell,
    fmt::{Debug, Formatter},
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

pub struct WeakArcMut<T: ?Sized> {
    inner: Weak<SyncUnsafeCell<T>>,
}

// Implementation of PartialEq for WeakArcMut<T>
impl<T: PartialEq> PartialEq for WeakArcMut<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // Upgrade the Weak references to Arc, then compare the inner values
        if let (Some(self_arc), Some(other_arc)) = (self.inner.upgrade(), other.inner.upgrade()) {
            unsafe { *self_arc.get() == *other_arc.get() }
        } else {
            false
        }
    }
}

// Implementation of Eq for WeakArcMut<T>
impl<T: PartialEq> Eq for WeakArcMut<T> {}

// Implementation of Hash for WeakArcMut<T>
impl<T: Hash> Hash for WeakArcMut<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(arc) = self.inner.upgrade() {
            unsafe { (*arc.get()).hash(state) }
        }
    }
}

impl<T: ?Sized> Clone for WeakArcMut<T> {
    #[inline]
    fn clone(&self) -> Self {
        WeakArcMut {
            inner: self.inner.clone(),
        }
    }
}

impl<T> WeakArcMut<T> {
    #[inline]
    pub fn upgrade(&self) -> Option<ArcMut<T>> {
        self.inner.upgrade().map(|value| ArcMut { inner: value })
    }
}

#[derive(Default)]
pub struct ArcMut<T: ?Sized> {
    inner: Arc<SyncUnsafeCell<T>>,
}

// Implementation of PartialEq for ArcMut<T>
impl<T: PartialEq> PartialEq for ArcMut<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // Compare the inner values by borrowing them unsafely
        unsafe { *self.inner.get() == *other.inner.get() }
    }
}

impl<T: Hash> Hash for ArcMut<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Compute the hash of the inner value
        unsafe { (*self.inner.get()).hash(state) }
    }
}

// Implementation of Eq for ArcMut<T>
// Eq implies PartialEq, so we don't need to add any methods here
impl<T: PartialEq> Eq for ArcMut<T> {}

impl<T> ArcMut<T> {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn mut_from_ref(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }

    #[inline]
    pub fn downgrade(this: &Self) -> WeakArcMut<T> {
        WeakArcMut {
            inner: Arc::downgrade(&this.inner),
        }
    }

    #[inline]
    pub fn get_inner(&self) -> &Arc<SyncUnsafeCell<T>> {
        &self.inner
    }
}

impl<T> ArcMut<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(SyncUnsafeCell::new(value)),
        }
    }
}

impl<T: ?Sized> Clone for ArcMut<T> {
    #[inline]
    fn clone(&self) -> Self {
        ArcMut {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: ?Sized> AsRef<T> for ArcMut<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        unsafe { &*self.inner.get() }
    }
}

impl<T: ?Sized> AsMut<T> for ArcMut<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

impl<T: ?Sized> Deref for ArcMut<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T: ?Sized> DerefMut for ArcMut<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

pub struct SyncUnsafeCellWrapper<T: ?Sized> {
    inner: SyncUnsafeCell<T>,
}

impl<T> SyncUnsafeCellWrapper<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: SyncUnsafeCell::new(value),
        }
    }
}

impl<T> SyncUnsafeCellWrapper<T> {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn mut_from_ref(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

impl<T> AsRef<T> for SyncUnsafeCellWrapper<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        unsafe { &*self.inner.get() }
    }
}

impl<T> AsMut<T> for SyncUnsafeCellWrapper<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        &mut *self.inner.get_mut()
    }
}

impl<T> Deref for SyncUnsafeCellWrapper<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for SyncUnsafeCellWrapper<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl<T: ?Sized + Debug> std::fmt::Debug for ArcMut<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(test)]
mod arc_cell_wrapper_tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn new_creates_arc_cell_wrapper_with_provided_value() {
        let wrapper = ArcMut::new(10);
        assert_eq!(*wrapper.as_ref(), 10);
    }

    #[test]
    fn clone_creates_a_new_instance_with_same_value() {
        let wrapper = ArcMut::new(20);
        let cloned_wrapper = wrapper.clone();
        assert_eq!(*cloned_wrapper.as_ref(), 20);
    }

    #[test]
    fn as_ref_returns_immutable_reference_to_value() {
        let wrapper = ArcMut::new(30);
        assert_eq!(*wrapper.as_ref(), 30);
    }

    #[test]
    fn as_mut_returns_mutable_reference_to_value() {
        let mut wrapper = ArcMut::new(40);
        *wrapper.as_mut() = 50;
        assert_eq!(*wrapper.as_ref(), 50);
    }

    #[test]
    fn deref_returns_reference_to_inner_value() {
        let wrapper = ArcMut::new(60);
        assert_eq!(*wrapper, 60);
    }

    #[test]
    fn deref_mut_allows_modification_of_inner_value() {
        let mut wrapper = ArcMut::new(70);
        *wrapper = 80;
        assert_eq!(*wrapper, 80);
    }

    #[test]
    fn multiple_clones_share_the_same_underlying_data() {
        let wrapper = ArcMut::new(Arc::new(90));
        let cloned_wrapper1 = wrapper.clone();
        let cloned_wrapper2 = wrapper.clone();

        assert_eq!(Arc::strong_count(wrapper.as_ref()), 1);
        assert_eq!(Arc::strong_count(cloned_wrapper1.as_ref()), 1);
        assert_eq!(Arc::strong_count(cloned_wrapper2.as_ref()), 1);
    }
}
