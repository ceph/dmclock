// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#include <iostream>
#include <memory>
#include <set>

#include "gtest/gtest.h"

#include "indirect_intrusive_base.h"


struct Elem {
  int data;

  crimson::IndIntruHeapData heap_data;
  crimson::IndIntruHeapData heap_data_alt;

  Elem(int _data) : data(_data) { }

  friend std::ostream& operator<<(std::ostream& out, const Elem& d) {
    out << d.data;
    return out;
  }
};


// sorted low to high
struct ElemCompare {
  bool operator()(const Elem& d1, const Elem& d2) {
    return d1.data < d2.data;
  }
};


class HeapFixture3: public ::testing::Test {

public:

  crimson::IndIntruBase<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  std::shared_ptr<Elem> data1, data2, data3, data4, data5, data6, data7;

  void SetUp() {
    data1 = std::make_shared<Elem>(2);
    data2 = std::make_shared<Elem>(99);
    data3 = std::make_shared<Elem>(1);
    data4 = std::make_shared<Elem>(-5);
    data5 = std::make_shared<Elem>(12);
    data6 = std::make_shared<Elem>(-12);
    data7 = std::make_shared<Elem>(-7);

    heap.push(data1);
    heap.push(data2);
    heap.push(data3);
    heap.push(data4);
    heap.push(data5);
    heap.push(data6);
    heap.push(data7);
  }

  void TearDown() { 
    // nothing to do
  }
}; // class HeapFixture3


TEST(IndIntruBase, shared_ptr) {
  crimson::IndIntruBase<std::shared_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(std::make_shared<Elem>(2));

  EXPECT_FALSE(heap.empty());

  heap.push(std::make_shared<Elem>(99));
  heap.push(std::make_shared<Elem>(1));
  heap.push(std::make_shared<Elem>(-5));
  heap.push(std::make_shared<Elem>(12));
  heap.push(std::make_shared<Elem>(-12));
  heap.push(std::make_shared<Elem>(-7));

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruBase, unique_ptr) {
  crimson::IndIntruBase<std::unique_ptr<Elem>,
			Elem,
			&Elem::heap_data,
			ElemCompare> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(std::unique_ptr<Elem>(new Elem(2)));

  EXPECT_FALSE(heap.empty());

  heap.push(std::unique_ptr<Elem>(new Elem(99)));
  heap.push(std::unique_ptr<Elem>(new Elem(1)));
  heap.push(std::unique_ptr<Elem>(new Elem(-5)));
  heap.push(std::unique_ptr<Elem>(new Elem(12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-12)));
  heap.push(std::unique_ptr<Elem>(new Elem(-7)));

  EXPECT_FALSE(heap.empty());
  EXPECT_EQ(2, heap.top().data);
  heap.pop();
  EXPECT_EQ(-7, heap.top().data);
  heap.pop();
  EXPECT_EQ(-12, heap.top().data);
  heap.pop();
  EXPECT_EQ(12, heap.top().data);
  heap.pop();
  EXPECT_EQ(-5, heap.top().data);
  heap.pop();
  EXPECT_EQ(1, heap.top().data);
  heap.pop();
  EXPECT_EQ(99, heap.top().data);

  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}


TEST(IndIntruBase, regular_ptr) {
  crimson::IndIntruBase<Elem*, Elem, &Elem::heap_data, ElemCompare> heap;

  EXPECT_TRUE(heap.empty());

  heap.push(new Elem(2));

  EXPECT_FALSE(heap.empty());

  heap.push(new Elem(99));
  heap.push(new Elem(1));
  heap.push(new Elem(-5));
  heap.push(new Elem(12));
  heap.push(new Elem(-12));
  heap.push(new Elem(-7));

  EXPECT_FALSE(heap.empty());

  EXPECT_EQ(2, heap.top().data);
  delete &heap.top();
  heap.pop();

  EXPECT_EQ(-7, heap.top().data);
  delete &heap.top();
  heap.pop();

  EXPECT_EQ(-12, heap.top().data);
  delete &heap.top();
  heap.pop();

  EXPECT_EQ(12, heap.top().data);
  delete &heap.top();
  heap.pop();

  EXPECT_EQ(-5, heap.top().data);
  delete &heap.top();
  heap.pop();

  EXPECT_EQ(1, heap.top().data);
  delete &heap.top();
  heap.pop();

  EXPECT_EQ(99, heap.top().data);
  delete &heap.top();
  EXPECT_FALSE(heap.empty());
  heap.pop();
  EXPECT_TRUE(heap.empty());
}

