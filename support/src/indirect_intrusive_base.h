// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once

#include <memory>
#include <vector>
#include <string>
#include <iostream>
#include <functional>

#include "assert.h"


namespace crimson {
  using IndIntruHeapData = size_t;

  /* T is the ultimate data that's being stored in the heap, although
   *   through indirection.
   *
   * I is the indirect type that will actually be stored in the heap
   *   and that must allow dereferencing (via operator*) to yield a
   *   T&.
   *
   * C is functor when given two T&'s will return true if the first
   *   must precede the second.
   *
   * heap_info is a data member pointer as to where the heap data in T
   * is stored.
   */
  template<typename I, typename T, IndIntruHeapData T::*heap_info,  typename C>
  class IndIntruBase {

    static_assert(
      std::is_same<T,typename std::pointer_traits<I>::element_type>::value,
      "class I must resolve to class T by indirection (pointer dereference)");

    static_assert(
      std::is_same<bool,
      typename std::result_of<C(const T&,const T&)>::type>::value,
      "class C must define operator() to take two const T& and return a bool");

  protected:

    class Iterator {
      friend IndIntruBase<I, T, heap_info, C>;

      IndIntruBase<I, T, heap_info, C>& heap;
      size_t                            index;

      Iterator(IndIntruBase<I, T, heap_info, C>& _heap, size_t _index) :
	heap(_heap),
	index(_index)
      {
	// empty
      }

    public:

      Iterator(Iterator&& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      Iterator(const Iterator& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      Iterator& operator=(Iterator&& other) {
	std::swap(heap, other.heap);
	std::swap(index, other.index);
	return *this;
      }

      Iterator& operator=(const Iterator& other) {
	heap = other.heap;
	index = other.index;
      }

      Iterator& operator++() {
	if (index <= heap.count) {
	  ++index;
	}
	return *this;
      }

      bool operator==(const Iterator& other) const {
	return index == other.index;
      }

      bool operator!=(const Iterator& other) const {
	return !(*this == other);
      }

      T& operator*() {
	return *heap.data[index];
      }

      T* operator->() {
	return &(*heap.data[index]);
      }

#if 0
      // the item this iterator refers to
      void increase() {
	heap.siftUp(index);
      }
#endif
    }; // class Iterator


    class ConstIterator {
      friend IndIntruBase<I, T, heap_info, C>;

      const IndIntruBase<I, T, heap_info, C>& heap;
      size_t                                  index;

      ConstIterator(const IndIntruBase<I, T, heap_info, C>& _heap, size_t _index) :
	heap(_heap),
	index(_index)
      {
	// empty
      }

    public:

      ConstIterator(ConstIterator&& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      ConstIterator(const ConstIterator& other) :
	heap(other.heap),
	index(other.index)
      {
	// empty
      }

      ConstIterator& operator=(ConstIterator&& other) {
	std::swap(heap, other.heap);
	std::swap(index, other.index);
	return *this;
      }

      ConstIterator& operator=(const ConstIterator& other) {
	heap = other.heap;
	index = other.index;
      }

      ConstIterator& operator++() {
	if (index <= heap.count) {
	  ++index;
	}
	return *this;
      }

      bool operator==(const ConstIterator& other) const {
	return &heap == &other.heap && index == other.index;
      }

      bool operator!=(const ConstIterator& other) const {
	return !(*this == other);
      }

      const T& operator*() {
	return *heap.data[index];
      }

      const T* operator->() {
	return &(*heap.data[index]);
      }

    }; // class ConstIterator


  protected:
    using index_t = IndIntruHeapData;

    std::vector<I> data;
    index_t        count;


  public:

    IndIntruBase() :
      count(0)
    {
      // empty
    }

    IndIntruBase(const IndIntruBase<I,T,heap_info, C>& other) :
      count(other.count)
    {
      for (uint i = 0; i < other.count; ++i) {
	data.push_back(other.data[i]);
      }
    }

    bool empty() const { return 0 == count; }

    size_t size() const { return count; }

    T& top() { return *data[0]; }

    const T& top() const { return *data[0]; }

    I& top_ind() { return data[0]; }

    const I& top_ind() const { return data[0]; }

    Iterator find(const I& item) {
      for (index_t i = 0; i < count; ++i) {
	if (data[i] == item) {
	  return Iterator(*this, i);
	}
      }
      return end();
    }

    // NB: should we be using operator== instead of address check?
    Iterator find(const T& item) {
      for (index_t i = 0; i < count; ++i) {
	if (data[i].get() == &item) {
	  return Iterator(*this, i);
	}
      }
      return end();
    }

    // reverse find -- start looking from bottom of heap
    Iterator rfind(const I& item) {
      // index_t is unsigned, so we can't allow to go negative; so
      // we'll keep it one more than actual index
      for (index_t i = count; i > 0; --i) {
	if (data[i-1] == item) {
	  return Iterator(*this, i-1);
	}
      }
      return end();
    }

    // reverse find -- start looking from bottom of heap
    Iterator rfind(const T& item) {
      // index_t is unsigned, so we can't allow to go negative; so
      // we'll keep it one more than actual index
      for (index_t i = count; i > 0; --i) {
	if (data[i-1].get() == &item) {
	  return Iterator(*this, i-1);
	}
      }
      return end();
    }

    Iterator begin() {
      return Iterator(*this, 0);
    }

    Iterator end() {
      return Iterator(*this, count);
    }

    ConstIterator cbegin() const {
      return ConstIterator(*this, 0);
    }

    ConstIterator cend() const {
      return ConstIterator(*this, count);
    }

    // queue methods
    void push(I&& item) {
      index_t i = count++;
      intru_data_of(item) = i;
      data.emplace_back(std::move(item));
    }

    void push(const I& item) {
      I copy(item);
      push(std::move(copy));
    }

    void pop() {
      remove((index_t)0);
    }

    size_t remove(Iterator& i) {
      size_t _index = i.index;
      remove(_index);
      i = end();
      return _index;
    }

    size_t remove(const I& item) {
      index_t i = (*item).*heap_info;
      remove(i);
      return i;
    }

    friend std::ostream& operator<<(std::ostream& out, const IndIntruBase& h) {
      auto i = h.data.cbegin();
      if (i != h.data.cend()) {
	out << **i;
	++i;
	while (i != h.data.cend()) {
	  out << ", " << **i;
	}
      }
      return out;
    }

    // default value of filter parameter to display_sorted
    static bool all_filter(const T& data) { return true; }

    // can only be called if I is copyable
    std::ostream&
    display_sorted(std::ostream& out,
		   std::function<bool(const T&)> filter = all_filter) const {
      static_assert(std::is_copy_constructible<I>::value,
		    "cannot call display_sorted when class I is not copy"
		    " constructible");

      IndIntruBase<I,T,heap_info, C> copy = *this;

      bool first = true;
      while(!copy.empty()) {
	const T& top = copy.top();
	if (filter(top)) {
	  if (!first) {
	    out << ", ";
	  }
	  out << copy.top();
	  first = false;
	}
	copy.pop();
      }

      return out;
    }

    ~IndIntruBase() {
      // empty
    };


  protected:

    static index_t& intru_data_of(I& item) {
      return (*item).*heap_info;
    }

    void remove(index_t i) {
      std::swap(data[i], data[--count]);
      intru_data_of(data[i]) = i;
      data.pop_back();
    }

  }; // class IndIntruBase
} // namespace crimson
