// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once

#include "indirect_intrusive_base.h"


namespace crimson {

  template<typename I, typename T, IndIntruHeapData T::*heap_info, typename C>
  class IndIntruSimplePQ : public IndIntruBase <I, T, heap_info, C>  {
    using super = IndIntruBase <I, T, heap_info, C>;

  protected:
    using index_t = IndIntruHeapData;

    C               comparator;
    T               *dummy_ref;
    index_t         min_index;

  public:

    IndIntruSimplePQ() :
      super(),
      dummy_ref(NULL),
      min_index(0)
    {
      // empty
    }

    IndIntruSimplePQ(const IndIntruSimplePQ < I, T, heap_info, C > &other) :
      super(other),
      dummy_ref(NULL),
      min_index(other.min_index)
    {
      // empty
    }

    void pop() {
      remove((index_t)0);
    }

    void push(I&& item) {
      super::push(std::move(item));
      adjust(*dummy_ref);
    }

    void push(const I& item) {
      I copy(item);
      push(std::move(copy));
    }

    void promote(T& item) {
      adjust(item);
    }

    void demote(T& item) {
      adjust(item);
    }

    // ignore the parameter
    void adjust(T& ) {
      min_index = 0;
      for (index_t i = 1 ; i < super::count; i++){
	if (comparator(*super::data[i], *super::data[min_index])){
	  min_index = i;
	}
      }
      if (min_index) {
	std::swap(super::data[0], super::data[min_index]);
	super::intru_data_of(super::data[0]) = 0;
	super::intru_data_of(super::data[min_index]) = min_index;
      }
    }

    void remove(typename super::Iterator& i) {
      super::remove(i);
      adjust(*dummy_ref);
    }

    void remove(const I& item) {
      super::remove(item);
      adjust(*dummy_ref);
    }

  protected:

    void remove(index_t i) {
      super::remove(i);
      adjust(*dummy_ref);
    }

  }; // class IndIntruSimplePQ

} // namespace crimson
