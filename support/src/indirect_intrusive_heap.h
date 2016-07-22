// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once

#include "indirect_intrusive_base.h"


namespace crimson {

  template<typename I, typename T, IndIntruHeapData T::*heap_info, typename C>
  class IndIntruHeap : public IndIntruBase <I, T, heap_info, C> {
    using super = IndIntruBase <I, T, heap_info, C>;

  protected:
    using index_t = IndIntruHeapData;

    C              comparator;

  public:

    IndIntruHeap() :
      super()
    {
      // empty
    }

    IndIntruHeap(const IndIntruHeap < I, T, heap_info, C > &other) :
      super (other)
    {
      // empty
    }

    void pop() {
      remove((index_t)0);
    }

    void push(I&& item) {
      super::push(std::move(item));
      sift_up(super::count - 1);
    }

    void push(const I& item) {
      I copy(item);
      push(std::move(copy));
    }

    void promote(T& item) {
      sift_up(item.*heap_info);
    }

    void demote(T& item) {
      sift_down(item.*heap_info);
    }

    void adjust(T& item) {
      sift(item.*heap_info);
    }

    void remove(typename super::Iterator& i) {
      index_t _i = super::remove(i);
      sift_down(_i);
    }

    void remove(const I& item) {
      size_t i = super::remove(item);
      sift_down(i);
    }

    ~IndIntruHeap() {
       // empty
    }

  protected:

    void remove(index_t i) {
      super::remove(i);
      sift_down(i);
    }

    // when i is negative?
    static inline index_t parent(index_t i) {
      assert(0 != i);
      return (i - 1) / 2;
    }

    static inline index_t lhs(index_t i) { return 2*i + 1; }

    static inline index_t rhs(index_t i) { return 2*i + 2; }

    void sift_up(index_t i) {
      while (i > 0) {
	index_t pi = parent(i);
	if (!comparator(*super::data[i], *super::data[pi])) {
	  break;
	}

	std::swap(super::data[i], super::data[pi]);
	super::intru_data_of(super::data[i]) = i;
	super::intru_data_of(super::data[pi]) = pi;
	i = pi;
      }
    } // sift_up

    void sift_down(index_t i) {
      while (i < super::count) {
	index_t li = lhs(i);
	index_t ri = rhs(i);

	if (li < super::count) {
	  if (comparator(*super::data[li], *super::data[i])) {
	    if (ri < super::count && comparator(*super::data[ri], *super::data[li])) {
	      std::swap(super::data[i], super::data[ri]);
	      super::intru_data_of(super::data[i]) = i;
	      super::intru_data_of(super::data[ri]) = ri;
	      i = ri;
	    } else {
	      std::swap(super::data[i], super::data[li]);
	      super::intru_data_of(super::data[i]) = i;
	      super::intru_data_of(super::data[li]) = li;
	      i = li;
	    }
	  } else if (ri < super::count && comparator(*super::data[ri], *super::data[i])) {
	    std::swap(super::data[i], super::data[ri]);
	    super::intru_data_of(super::data[i]) = i;
	    super::intru_data_of(super::data[ri]) = ri;
	    i = ri;
	  } else {
	    break;
	  }
	} else {
	  break;
	}
      }
    } // sift_down

    void sift(index_t i) {
      if (i == 0) {
	// if we're at top, can only go down
	sift_down(i);
      } else {
	index_t pi = parent(i);
	if (comparator(*super::data[i], *super::data[pi])) {
	  // if we can go up, we will
	  sift_up(i);
	} else {
	  // otherwise we'll try to go down
	  sift_down(i);
	}
      }
    } // sift

  }; // class IndIntruHeap
} // namespace crimson
