// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#include <memory>
#include <chrono>
#include <iostream>
#include <list>
#include <vector>
#include <random>
#include <array>

#include "dmclock_server.h"
#include "dmclock_util.h"
#include "gtest/gtest.h"

// process control to prevent core dumps during gtest death tests
#include "dmcPrCtl.h"


namespace dmc = crimson::dmclock;


// we need a request object; an empty one will do
struct Request {
};

struct client_tick_t {
  uint8_t last_pq_num = 0;
  uint64_t last_tick_interval = 0;

  client_tick_t() = default;

  inline void update(uint8_t pq_num) {
    last_tick_interval++;
    last_pq_num = pq_num;
  }

  inline void reset_last_tick_interval() {
    last_tick_interval = 0;
  }

  inline uint64_t get_last_tick_interval() {
    return last_tick_interval;
  }
};


namespace crimson {
  namespace dmclock {

    /*
     * Allows us to test the code provided with the mutex provided locked.
     */
    static void test_locked(std::mutex& mtx, std::function<void()> code) {
      std::unique_lock<std::mutex> l(mtx);
      code();
    }


    TEST(dmclock_server, bad_tag_deathtest) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 18;

      double reservation = 0.0;
      double weight = 0.0;

      dmc::ClientInfo ci1(reservation, weight, 0.0);
      dmc::ClientInfo ci2(reservation, weight, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &ci1;
	else if (client2 == c) return &ci2;
	else {
	  ADD_FAILURE() << "got request from neither of two clients";
	  return nullptr;
	}
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));
      ReqParams req_params(1,1);

      // Disable coredumps
      PrCtl unset_dumpable;

      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(Request{}, client1, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";


      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(Request{}, client2, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";

      EXPECT_DEATH_IF_SUPPORTED(Queue(client_info_f, AtLimit::Reject),
				"Assertion.*Reject.*Delayed") <<
	"we should fail if a client tries to construct a queue with both "
        "DelayedTagCalc and AtLimit::Reject";
    }


    TEST(dmclock_server, client_idle_erase) {
      using ClientId = int;
      using Queue = dmc::PushPriorityQueue<ClientId,Request>;
      ClientId client = 17;
      double reservation = 100.0;

      dmc::ClientInfo ci(reservation, 1.0, 0.0);
      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
			      std::unique_ptr<Request> req,
			      dmc::PhaseType phase,
			      uint64_t req_cost) {
	// empty; do nothing
      };

      Queue pq(client_info_f,
	       server_ready_f,
	       submit_req_f,
	       std::chrono::seconds(3),
	       std::chrono::seconds(5),
	       std::chrono::seconds(2),
	       AtLimit::Wait);

      auto lock_pq = [&](std::function<void()> code) {
	test_locked(pq.data_mtx, code);
      };


      /* The timeline should be as follows:
       *
       *     0 seconds : request created
       *
       *     1 seconds : map is size 1, idle is false
       *
       * 2 seconds : clean notes first mark; +2 is base for further calcs
       *
       * 4 seconds : clean does nothing except makes another mark
       *
       *   5 seconds : when we're secheduled to idle (+2 + 3)
       *
       * 6 seconds : clean idles client
       *
       *   7 seconds : when we're secheduled to erase (+2 + 5)
       *
       *     7 seconds : verified client is idle
       *
       * 8 seconds : clean erases client info
       *
       *     9 seconds : verified client is erased
       */

      lock_pq([&] () {
	  EXPECT_EQ(0u, pq.client_map.size()) <<
	    "client map initially has size 0";
	});

      Request req;
      dmc::ReqParams req_params(1, 1);
      EXPECT_EQ(0, pq.add_request_time(req, client, req_params, dmc::get_time()));

      std::this_thread::sleep_for(std::chrono::seconds(1));

      lock_pq([&] () {
	  EXPECT_EQ(1u, pq.client_map.size()) <<
	    "client map has 1 after 1 client";
	  EXPECT_FALSE(pq.client_map.at(client)->idle) <<
	    "initially client map entry shows not idle.";
	});

      std::this_thread::sleep_for(std::chrono::seconds(6));

      lock_pq([&] () {
	  EXPECT_TRUE(pq.client_map.at(client)->idle) <<
	    "after idle age client map entry shows idle.";
	});

      std::this_thread::sleep_for(std::chrono::seconds(2));

      lock_pq([&] () {
	  EXPECT_EQ(0u, pq.client_map.size()) <<
	    "client map loses its entry after erase age";
	});
    } // TEST


    TEST(dmclock_server, add_req_pushprio_queue) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PushPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 17;
      ClientId client2 = 34;

      dmc::ClientInfo ci(0.0, 1.0, 0.0);
      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
                              std::unique_ptr<MyReq> req,
                              dmc::PhaseType phase,
                              uint64_t req_cost) {
        // empty; do nothing
      };

      Queue pq(client_info_f,
               server_ready_f,
               submit_req_f,
               std::chrono::seconds(3),
               std::chrono::seconds(5),
               std::chrono::seconds(2),
               AtLimit::Wait);

      auto lock_pq = [&](std::function<void()> code) {
        test_locked(pq.data_mtx, code);
      };

      lock_pq([&] () {
          EXPECT_EQ(0u, pq.client_map.size()) <<
            "client map initially has size 0";
        });

      dmc::ReqParams req_params(1, 1);

      // Create a reference to a request
      MyReqRef rr1 = MyReqRef(new MyReq(11));

      // Exercise different versions of add_request()
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(22), client2, req_params));

      std::this_thread::sleep_for(std::chrono::seconds(1));

      lock_pq([&] () {
          EXPECT_EQ(2u, pq.client_map.size()) <<
            "client map has 2 after 2 clients";
          EXPECT_FALSE(pq.client_map.at(client1)->idle) <<
            "initially client1 map entry shows not idle.";
          EXPECT_FALSE(pq.client_map.at(client2)->idle) <<
            "initially client2 map entry shows not idle.";
        });

      // Check client idle state
      std::this_thread::sleep_for(std::chrono::seconds(6));
      lock_pq([&] () {
          EXPECT_TRUE(pq.client_map.at(client1)->idle) <<
            "after idle age client1 map entry shows idle.";
          EXPECT_TRUE(pq.client_map.at(client2)->idle) <<
            "after idle age client2 map entry shows idle.";
        });

      // Sleep until after erase age elapses
      std::this_thread::sleep_for(std::chrono::seconds(2));

      lock_pq([&] () {
          EXPECT_EQ(0u, pq.client_map.size()) <<
          "client map loses its entries after erase age";
        });
    } // TEST


    TEST(dmclock_server, schedule_req_pushprio_queue) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PushPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 17;
      ClientId client2 = 34;
      ClientId client3 = 48;

      dmc::ClientInfo ci(1.0, 1.0, 1.0);
      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
                              std::unique_ptr<MyReq> req,
                              dmc::PhaseType phase,
                              uint64_t req_cost) {
        // empty; do nothing
      };

      Queue pq(client_info_f,
               server_ready_f,
               submit_req_f,
               std::chrono::seconds(3),
               std::chrono::seconds(5),
               std::chrono::seconds(2),
               AtLimit::Wait);

      dmc::ReqParams req_params(1, 1);

      // Create a reference to a request
      MyReqRef rr1 = MyReqRef(new MyReq(11));

      // Exercise different versions of add_request()
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(22), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(33), client3, req_params));

      std::this_thread::sleep_for(std::chrono::seconds(4));

      ASSERT_TRUE(pq.request_count() == 0);
    } // TEST


    TEST(dmclock_server, delayed_tag_calc) {
      using ClientId = int;
      constexpr ClientId client1 = 17;

      using DelayedQueue = PullPriorityQueue<ClientId, Request, true>;
      using ImmediateQueue = PullPriorityQueue<ClientId, Request, false>;

      ClientInfo info(0.0, 1.0, 1.0);
      auto client_info_f = [&] (ClientId c) -> const ClientInfo* {
	return &info;
      };

      Time t{1};
      {
	DelayedQueue queue(client_info_f);

	queue.add_request_time({}, client1, {0,0}, t);
	queue.add_request_time({}, client1, {0,0}, t + 1);
	queue.add_request_time({}, client1, {10,10}, t + 2);

	auto pr1 = queue.pull_request(t);
	ASSERT_TRUE(pr1.is_retn());
	auto pr2 = queue.pull_request(t + 1);
	// ReqParams{10,10} from request #3 pushes request #2 over limit by 10s
	ASSERT_TRUE(pr2.is_future());
	EXPECT_DOUBLE_EQ(t + 11, pr2.getTime());
      }
      {
	ImmediateQueue queue(client_info_f);

	queue.add_request_time({}, client1, {0,0}, t);
	queue.add_request_time({}, client1, {0,0}, t + 1);
	queue.add_request_time({}, client1, {10,10}, t + 2);

	auto pr1 = queue.pull_request(t);
	ASSERT_TRUE(pr1.is_retn());
	auto pr2 = queue.pull_request(t + 1);
	// ReqParams{10,10} from request #3 has no effect on request #2
	ASSERT_TRUE(pr2.is_retn());
	auto pr3 = queue.pull_request(t + 2);
	ASSERT_TRUE(pr3.is_future());
	EXPECT_DOUBLE_EQ(t + 12, pr3.getTime());
      }
    }

#if 0
    TEST(dmclock_server, reservation_timing) {
      using ClientId = int;
      // NB? PUSH OR PULL
      using Queue = std::unique_ptr<dmc::PriorityQueue<ClientId,Request>>;
      using std::chrono::steady_clock;

      int client = 17;

      std::vector<dmc::Time> times;
      std::mutex times_mtx;
      using Guard = std::lock_guard<decltype(times_mtx)>;

      // reservation every second
      dmc::ClientInfo ci(1.0, 0.0, 0.0);
      Queue pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &ci;
      };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [&] (const ClientId& c,
			       std::unique_ptr<Request> req,
			       dmc::PhaseType phase) {
	{
	  Guard g(times_mtx);
	  times.emplace_back(dmc::get_time());
	}
	std::thread complete([&](){ pq->request_completed(); });
	complete.detach();
      };

      // NB? PUSH OR PULL
      pq = Queue(new dmc::PriorityQueue<ClientId,Request>(client_info_f,
							  server_ready_f,
							  submit_req_f,
							  false));

      Request req;
      ReqParams<ClientId> req_params(client, 1, 1);

      for (int i = 0; i < 5; ++i) {
	pq->add_request_time(req, req_params, dmc::get_time());
      }

      {
	Guard g(times_mtx);
	std::this_thread::sleep_for(std::chrono::milliseconds(5500));
	EXPECT_EQ(5, times.size()) <<
	  "after 5.5 seconds, we should have 5 requests times at 1 second apart";
      }
    } // TEST
#endif


    TEST(dmclock_server, remove_by_req_filter) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(11), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(0), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(98), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(44), client1, req_params));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(9u, pq.request_count());

      pq.remove_by_req_filter([](MyReqRef&& r) -> bool {return 1 == r->id % 2;});

      EXPECT_EQ(5u, pq.request_count());

      std::list<MyReq> capture;
      pq.remove_by_req_filter(
	[&capture] (MyReqRef&& r) -> bool {
	  if (0 == r->id % 2) {
	    capture.push_front(*r);
	    return true;
	  } else {
	    return false;
	  }
	},
	true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(5u, capture.size());
      int total = 0;
      for (auto i : capture) {
	total += i.id;
      }
      EXPECT_EQ(146, total) << " sum of captured items should be 146";
    } // TEST


    TEST(dmclock_server, remove_by_req_filter_ordering_forwards_visit) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(3), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(4), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(5), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(6), client1, req_params));

      EXPECT_EQ(1u, pq.client_count());
      EXPECT_EQ(6u, pq.request_count());

      // remove odd ids in forward order and append to end

      std::vector<MyReq> capture;
      pq.remove_by_req_filter(
	[&capture] (MyReqRef&& r) -> bool {
	  if (1 == r->id % 2) {
	    capture.push_back(*r);
	    return true;
	  } else {
	    return false;
	  }
	},
	false);

      EXPECT_EQ(3u, pq.request_count());
      EXPECT_EQ(3u, capture.size());
      EXPECT_EQ(1, capture[0].id) << "items should come out in forward order";
      EXPECT_EQ(3, capture[1].id) << "items should come out in forward order";
      EXPECT_EQ(5, capture[2].id) << "items should come out in forward order";

      // remove even ids in reverse order but insert at front so comes
      // out forwards

      std::vector<MyReq> capture2;
      pq.remove_by_req_filter(
	[&capture2] (MyReqRef&& r) -> bool {
	  if (0 == r->id % 2) {
	    capture2.insert(capture2.begin(), *r);
	    return true;
	  } else {
	    return false;
	  }
	},
	false);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(3u, capture2.size());
      EXPECT_EQ(6, capture2[0].id) << "items should come out in reverse order";
      EXPECT_EQ(4, capture2[1].id) << "items should come out in reverse order";
      EXPECT_EQ(2, capture2[2].id) << "items should come out in reverse order";
    } // TEST


    TEST(dmclock_server, remove_by_req_filter_ordering_backwards_visit) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(3), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(4), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(5), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(6), client1, req_params));

      EXPECT_EQ(1u, pq.client_count());
      EXPECT_EQ(6u, pq.request_count());

      // now remove odd ids in forward order

      std::vector<MyReq> capture;
      pq.remove_by_req_filter(
	[&capture] (MyReqRef&& r) -> bool {
	  if (1 == r->id % 2) {
	    capture.insert(capture.begin(), *r);
	    return true;
	  } else {
	    return false;
	  }
	},
	true);

      EXPECT_EQ(3u, pq.request_count());
      EXPECT_EQ(3u, capture.size());
      EXPECT_EQ(1, capture[0].id) << "items should come out in forward order";
      EXPECT_EQ(3, capture[1].id) << "items should come out in forward order";
      EXPECT_EQ(5, capture[2].id) << "items should come out in forward order";

      // now remove even ids in reverse order

      std::vector<MyReq> capture2;
      pq.remove_by_req_filter(
	[&capture2] (MyReqRef&& r) -> bool {
	  if (0 == r->id % 2) {
	    capture2.push_back(*r);
	    return true;
	  } else {
	    return false;
	  }
	},
	true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(3u, capture2.size());
      EXPECT_EQ(6, capture2[0].id) << "items should come out in reverse order";
      EXPECT_EQ(4, capture2[1].id) << "items should come out in reverse order";
      EXPECT_EQ(2, capture2[2].id) << "items should come out in reverse order";
    } // TEST


    TEST(dmclock_server, remove_by_client) {
      struct MyReq {
	int id;

	MyReq(int _id) :
	  id(_id)
	{
	  // empty
	}
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info1;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      EXPECT_EQ(0, pq.add_request(MyReq(1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(11), client1, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(0), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(13), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(98), client2, req_params));
      EXPECT_EQ(0, pq.add_request(MyReq(44), client1, req_params));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(9u, pq.request_count());

      std::list<MyReq> removed;

      pq.remove_by_client(client1,
			  true,
			  [&removed] (MyReqRef&& r) {
			    removed.push_front(*r);
			  });

      EXPECT_EQ(3u, removed.size());
      EXPECT_EQ(1, removed.front().id);
      removed.pop_front();
      EXPECT_EQ(11, removed.front().id);
      removed.pop_front();
      EXPECT_EQ(44, removed.front().id);
      removed.pop_front();

      EXPECT_EQ(6u, pq.request_count());

      Queue::PullReq pr = pq.pull_request();
      EXPECT_TRUE(pr.is_retn());
      EXPECT_EQ(2, pr.get_retn().request->id);

      pr = pq.pull_request();
      EXPECT_TRUE(pr.is_retn());
      EXPECT_EQ(0, pr.get_retn().request->id);

      pq.remove_by_client(client2);
      EXPECT_EQ(0u, pq.request_count()) <<
	"after second client removed, none left";
    } // TEST


    TEST(dmclock_server, add_req_ref) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 22;
      ClientId client2 = 44;

      dmc::ClientInfo info(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &info;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      ReqParams req_params(1,1);

      MyReqRef rr1 = MyReqRef(new MyReq(1));
      MyReqRef rr2 = MyReqRef(new MyReq(2));
      MyReqRef rr3 = MyReqRef(new MyReq(3));
      MyReqRef rr4 = MyReqRef(new MyReq(4));
      MyReqRef rr5 = MyReqRef(new MyReq(5));
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr2), client2, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr3), client1, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr4), client2, req_params));
      EXPECT_EQ(0, pq.add_request(std::move(rr5), client2, req_params));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(5u, pq.request_count());

      pq.remove_by_req_filter([](MyReqRef&& r) -> bool {return 0 == r->id % 2;});

      EXPECT_EQ(3u, pq.request_count());

      std::list<MyReq> capture;
      pq.remove_by_req_filter(
        [&capture] (MyReqRef&& r) -> bool {
          if (1 == r->id % 2) {
            capture.push_front(*r);
            return true;
          } else {
            return false;
          }
        },
        true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(3u, capture.size());
      int total = 0;
      for (auto i : capture) {
        total += i.id;
      }
      EXPECT_EQ(9, total) << " sum of captured items should be 9";
    } // TEST


     TEST(dmclock_server, add_req_ref_null_req_params) {
      struct MyReq {
        int id;

        MyReq(int _id) :
          id(_id)
        {
          // empty
        }
      }; // MyReq

      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,MyReq>;
      using MyReqRef = typename Queue::RequestRef;
      ClientId client1 = 22;
      ClientId client2 = 44;

      dmc::ClientInfo info(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        return &info;
      };

      Queue pq(client_info_f, AtLimit::Allow);

      EXPECT_EQ(0u, pq.client_count());
      EXPECT_EQ(0u, pq.request_count());

      MyReqRef&& rr1 = MyReqRef(new MyReq(1));
      MyReqRef&& rr2 = MyReqRef(new MyReq(2));
      MyReqRef&& rr3 = MyReqRef(new MyReq(3));
      MyReqRef&& rr4 = MyReqRef(new MyReq(4));
      MyReqRef&& rr5 = MyReqRef(new MyReq(5));
      EXPECT_EQ(0, pq.add_request(std::move(rr1), client1));
      EXPECT_EQ(0, pq.add_request(std::move(rr2), client2));
      EXPECT_EQ(0, pq.add_request(std::move(rr3), client1));
      EXPECT_EQ(0, pq.add_request(std::move(rr4), client2));
      EXPECT_EQ(0, pq.add_request(std::move(rr5), client2));

      EXPECT_EQ(2u, pq.client_count());
      EXPECT_EQ(5u, pq.request_count());

      pq.remove_by_req_filter([](MyReqRef&& r) -> bool {return 1 == r->id % 2;});

      EXPECT_EQ(2u, pq.request_count());

      std::list<MyReq> capture;
      pq.remove_by_req_filter(
        [&capture] (MyReqRef&& r) -> bool {
          if (0 == r->id % 2) {
            capture.push_front(*r);
            return true;
          } else {
            return false;
          }
        },
        true);

      EXPECT_EQ(0u, pq.request_count());
      EXPECT_EQ(2u, capture.size());
      int total = 0;
      for (auto i : capture) {
        total += i.id;
      }
      EXPECT_EQ(6, total) << " sum of captured items should be 6";
    } // TEST


  TEST(dmclock_server_pull, pull_weight) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);
      dmc::ClientInfo info2(0.0, 2.0, 0.0);

      QueueRef pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      pq = QueueRef(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	now += 0.0001;
      }

      int c1_count = 0;
      int c2_count = 0;
      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(2, c1_count) <<
	"one-third of request should have come from first client";
      EXPECT_EQ(4, c2_count) <<
	"two-thirds of request should have come from second client";
    }


    TEST(dmclock_server_pull, pull_reservation) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info1(2.0, 0.0, 0.0);
      dmc::ClientInfo info2(1.0, 0.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto old_time = dmc::get_time() - 100.0;

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, old_time));
	EXPECT_EQ(0, pq->add_request_time(Request{}, client2, req_params, old_time));
	old_time += 0.001;
      }

      int c1_count = 0;
      int c2_count = 0;

      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::reservation, retn.phase);
      }

      EXPECT_EQ(4, c1_count) <<
	"two-thirds of request should have come from first client";
      EXPECT_EQ(2, c2_count) <<
	"one-third of request should have come from second client";
    } // dmclock_server_pull.pull_reservation


    TEST(dmclock_server_pull, update_client_info) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,false>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 100.0, 0.0);
      dmc::ClientInfo info2(0.0, 200.0, 0.0);

      QueueRef pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      pq = QueueRef(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	now += 0.0001;
      }

      int c1_count = 0;
      int c2_count = 0;
      for (int i = 0; i < 10; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

	if (i > 5) continue;
	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(2, c1_count) <<
	"before: one-third of request should have come from first client";
      EXPECT_EQ(4, c2_count) <<
	"before: two-thirds of request should have come from second client";

      std::chrono::seconds dura(1);
      std::this_thread::sleep_for(dura);

      info1 = dmc::ClientInfo(0.0, 200.0, 0.0);
      pq->update_client_info(17);

      now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	now += 0.0001;
      }

      c1_count = 0;
      c2_count = 0;
      for (int i = 0; i < 6; ++i) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

	if (client1 == retn.client) ++c1_count;
	else if (client2 == retn.client) ++c2_count;
	else ADD_FAILURE() << "got request from neither of two clients";

	EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(3, c1_count) <<
	"after: one-third of request should have come from first client";
      EXPECT_EQ(3, c2_count) <<
	"after: two-thirds of request should have come from second client";
    }


    TEST(dmclock_server_pull, dynamic_cli_info_f) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,true,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1[] = {
        dmc::ClientInfo(0.0, 100.0, 0.0),
        dmc::ClientInfo(0.0, 150.0, 0.0)};
      dmc::ClientInfo info2[] = {
        dmc::ClientInfo(0.0, 200.0, 0.0),
        dmc::ClientInfo(0.0, 50.0, 0.0)};

      size_t cli_info_group = 0;

      QueueRef pq;

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1[cli_info_group];
	else if (client2 == c) return &info2[cli_info_group];
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      pq = QueueRef(new Queue(client_info_f, AtLimit::Wait));

      auto now = dmc::get_time();

      auto run_test = [&](float lower_bound, float upper_bound) {
	ReqParams req_params(1,1);
	constexpr unsigned num_requests = 1000;

	for (int i = 0; i < num_requests; i += 2) {
	  EXPECT_EQ(0, pq->add_request(Request{}, client1, req_params));
	  EXPECT_EQ(0, pq->add_request(Request{}, client2, req_params));
	  now += 0.0001;
	}

	int c1_count = 0;
	int c2_count = 0;
	for (int i = 0; i < num_requests; ++i) {
	  Queue::PullReq pr = pq->pull_request();
	  EXPECT_EQ(Queue::NextReqType::returning, pr.type);
	  // only count the specified portion of the served request
	  if (i < num_requests * lower_bound || i > num_requests * upper_bound) {
	    continue;
	  }
	  auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
	  if (client1 == retn.client) {
	    ++c1_count;
	  } else if (client2 == retn.client) {
	    ++c2_count;
	  } else {
	    ADD_FAILURE() << "got request from neither of two clients";
	  }
	  EXPECT_EQ(PhaseType::priority, retn.phase);
	}

        constexpr float tolerance = 0.002;
        float prop1 = float(info1[cli_info_group].weight) / (info1[cli_info_group].weight +
                                                             info2[cli_info_group].weight);
        float prop2 = float(info2[cli_info_group].weight) / (info1[cli_info_group].weight +
                                                             info2[cli_info_group].weight);
        EXPECT_NEAR(float(c1_count) / (c1_count + c2_count), prop1, tolerance) <<
          "before: " << prop1 << " of requests should have come from first client";
        EXPECT_NEAR
          (float(c2_count) / (c1_count + c2_count), prop2, tolerance) <<
          "before: " << prop2 << " of requests should have come from second client";
      };
      cli_info_group = 0;
      // only count the first half of the served requests, so we can check
      // the prioritized ones
      run_test(0.0F /* lower bound */,
               0.5F /* upper bound */);

      std::chrono::seconds dura(1);
      std::this_thread::sleep_for(dura);

      // check the middle part of the request sequence which is less likely
      // to be impacted by previous requests served before we switch to the
      // new client info.
      cli_info_group = 1;
      run_test(1.0F/3 /* lower bound */,
               2.0F/3 /* upper bound */);
    }

    // This test shows what happens when a request can be ready (under
    // limit) but not schedulable since proportion tag is 0. We expect
    // to get some future and none responses.
    TEST(dmclock_server_pull, ready_and_under_limit) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      dmc::ClientInfo info1(1.0, 0.0, 0.0);
      dmc::ClientInfo info2(1.0, 0.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	if (client1 == c) return &info1;
	else if (client2 == c) return &info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existent client";
	  return nullptr;
	}
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      // make sure all times are well before now
      auto start_time = dmc::get_time() - 100.0;

      // add six requests; for same client reservations spaced one apart
      for (int i = 0; i < 3; ++i) {
	EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, start_time));
	EXPECT_EQ(0, pq->add_request_time(Request{}, client2, req_params, start_time));
      }

      Queue::PullReq pr = pq->pull_request(start_time + 0.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 0.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 0.5);
      EXPECT_EQ(Queue::NextReqType::future, pr.type) <<
	"too soon for next reservation";

      pr = pq->pull_request(start_time + 1.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 1.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 1.5);
      EXPECT_EQ(Queue::NextReqType::future, pr.type) <<
	"too soon for next reservation";

      pr = pq->pull_request(start_time + 2.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 2.5);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      pr = pq->pull_request(start_time + 2.5);
      EXPECT_EQ(Queue::NextReqType::none, pr.type) << "no more requests left";
    }


    TEST(dmclock_server_pull, pull_none) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      dmc::ClientInfo info(1.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      // Request req;
      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      Queue::PullReq pr = pq->pull_request(now + 100);

      EXPECT_EQ(Queue::NextReqType::none, pr.type);
    }


    TEST(dmclock_server_pull, pull_future) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      // ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, now + 100));
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::future, pr.type);

      Time when = boost::get<Time>(pr.data);
      EXPECT_EQ(now + 100, when);
    }


    TEST(dmclock_server_pull, pull_future_limit_break_weight) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      // ClientId client2 = 8;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Allow));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, now + 100));
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(client1, retn.client);
    }


    TEST(dmclock_server_pull, pull_future_limit_break_reservation) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      // ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Allow));

      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, now + 100));
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(client1, retn.client);
    }


    TEST(dmclock_server_pull, pull_reject_at_limit) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Request, false>;
      using MyReqRef = typename Queue::RequestRef;

      ClientId client1 = 52;
      ClientId client2 = 53;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      Queue pq(client_info_f, AtLimit::Reject);

      {
        // success at 1 request per second
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1}));
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{2}));
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{3}));
        // request too soon
        EXPECT_EQ(EAGAIN, pq.add_request_time({}, client1, {}, Time{3.9}));
        // previous rejected request counts against limit
        EXPECT_EQ(EAGAIN, pq.add_request_time({}, client1, {}, Time{4}));
        EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{6}));
      }
      {
        auto r1 = MyReqRef{new Request};
        ASSERT_EQ(0, pq.add_request(std::move(r1), client2, {}, Time{1}));
        EXPECT_EQ(nullptr, r1); // add_request takes r1 on success
        auto r2 = MyReqRef{new Request};
        ASSERT_EQ(EAGAIN, pq.add_request(std::move(r2), client2, {}, Time{1}));
        EXPECT_NE(nullptr, r2); // add_request does not take r2 on failure
      }
    }


    TEST(dmclock_server_pull, pull_reject_threshold) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Request, false>;

      ClientId client1 = 52;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
	return &info;
      };

      // allow up to 3 seconds worth of limit before rejecting
      Queue pq(client_info_f, RejectThreshold{3.0});

      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // at limit=1
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // 1 over
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // 2 over
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{1})); // 3 over
      EXPECT_EQ(EAGAIN, pq.add_request_time({}, client1, {}, Time{1})); // reject
      EXPECT_EQ(0, pq.add_request_time({}, client1, {}, Time{3})); // 3 over
    }


    TEST(dmclock_server_pull, pull_wait_at_limit) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 8;

      // Create client1 with high limit.
      // Create client2 with low limit and with lower weight than client1
      dmc::ClientInfo info1(1.0, 2.0, 100.0);
      dmc::ClientInfo info2(1.0, 1.0, 2.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) {
          return &info1;
        } else if (client2 == c) {
          return &info2;
        } else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      QueueRef pq(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(1,1);

      // make sure all times are before now
      auto add_time = dmc::get_time() - 1.0;
      auto old_time = add_time;

      for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(0, pq->add_request_time(Request{}, client1, req_params, add_time));
        EXPECT_EQ(0, pq->add_request_time(Request{}, client2, req_params, add_time));
        add_time += 0.01;
      }

      EXPECT_EQ(2u, pq->client_count());
      EXPECT_EQ(100u, pq->request_count());
      int c1_count = 0;
      int c2_count = 0;

      // Pull couple of requests, should come from reservation queue.
      // One request each from client1 and client2 should be pulled.
      for (int i = 0; i < 2; ++i) {
        Queue::PullReq pr = pq->pull_request();
        EXPECT_EQ(Queue::NextReqType::returning, pr.type);
        auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

        if (client1 == retn.client) {
          ++c1_count;
        } else if (client2 == retn.client) {
          ++c2_count;
        } else {
          ADD_FAILURE() << "got request from neither of two clients";
        }

        EXPECT_EQ(PhaseType::reservation, retn.phase);
      }

      EXPECT_EQ(1, c1_count) <<
        "one request should have come from first client";
      EXPECT_EQ(1, c2_count) <<
        "one request should have come from second client";

      EXPECT_EQ(2u, pq->client_count());
      EXPECT_EQ(98u, pq->request_count());

      // Pull more requests out.
      // All remaining requests from client1 should be pulled.
      // Only 1 request from client2 should be pulled.
      for (int i = 0; i < 50; ++i) {
        Queue::PullReq pr = pq->pull_request();
        EXPECT_EQ(Queue::NextReqType::returning, pr.type);
        auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);

        if (client1 == retn.client) {
          ++c1_count;
        } else if (client2 == retn.client) {
          ++c2_count;
        } else {
          ADD_FAILURE() << "got request from neither of two clients";
        }

        EXPECT_EQ(PhaseType::priority, retn.phase);
      }

      EXPECT_EQ(2u, pq->client_count());
      EXPECT_EQ(48u, pq->request_count());

      // Pulling the remaining client2 requests shouldn't succeed.
      Queue::PullReq pr = pq->pull_request();
      EXPECT_EQ(Queue::NextReqType::future, pr.type);
      Time when_ready = pr.getTime();
      EXPECT_EQ(old_time + 2.0, when_ready);

      EXPECT_EQ(50, c1_count) <<
        "half of the total requests should have come from first client";
      EXPECT_EQ(2, c2_count) <<
        "only two requests should have come from second client";

      // Trying to pull a request after restoring the limit should succeed.
      pr = pq->pull_request(old_time + 2.0);
      EXPECT_EQ(Queue::NextReqType::returning, pr.type);
      auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(retn.client, client2);
      EXPECT_EQ(47u, pq->request_count());
    } // dmclock_server_pull.pull_wait_at_limit

    TEST(dmclock_server_pull_multiq, pull_reservation_demo_immtag_incorrect) {
      // This test using the immediate tag calculation demonstrates the case
      // where a single client interacts with multiple mClock queues. The
      // multiple mClock queues are configued with the same server id.
      // Since the mClock queues are independent, the tag calculations are
      // also independent and this results in inaccurate QoS provided to
      // the client as the test shows.
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;

      dmc::ClientInfo info1(3.0, 1.0, 3.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      QueueRef pq1(new Queue(client_info_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      // make sure all times are well before now
      auto start_time = dmc::get_time() - 100.0;

      // enqueue 8 requests from client1 distributed
      // equally across pq1 and pq2.
      for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(0, pq1->add_request_time(Request{}, client1,
                                           req_params, start_time));
        EXPECT_EQ(0, pq2->add_request_time(Request{}, client1,
                                           req_params, start_time));
        start_time += 0.001;
      }

      int c1_count = 0;
      // Perform dequeue operation on pq1 & pq2 in a 1 second
      // span. Ideally, according to the reservation and limit
      // setting, only 3 requests should be pulled across the
      // queues. But the following sequence demonstrates that
      // double the requests (i.e., 6) are actually dequeued.
      for (double t = 0.0; t <= 0.7; t += 0.35) {
        Queue::PullReq pr1 = pq1->pull_request(start_time + t);
        EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
        auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
        EXPECT_EQ(PhaseType::reservation, retn1.phase);
        if (client1 == retn1.client) ++c1_count;
          else ADD_FAILURE() << "got request from no clients";

        // Perform dequeue operation on pq2
        Queue::PullReq pr2 = pq2->pull_request(start_time + t);
        EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
        auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
        EXPECT_EQ(PhaseType::reservation, retn2.phase);
        if (client1 == retn2.client) ++c1_count;
          else ADD_FAILURE() << "got request from no clients";
      }

      // The number of dequeued items should actually be 3 considering
      // the QoS parameters of reservation & limit!
      EXPECT_EQ(6, c1_count) <<
        "3/4th of the client1 requests should be from pq1 & pq2";

      // Dequeueing the last item before reservation is restored
      // should fail with a future work item on both queues
      Queue::PullReq pr1 = pq1->pull_request(start_time + 0.98);
      EXPECT_EQ(Queue::NextReqType::future, pr1.type) <<
        "shouldn't be able to pull requests until reservation is restored";
      Queue::PullReq pr2 = pq2->pull_request(start_time + 0.98);
      EXPECT_EQ(Queue::NextReqType::future, pr2.type) <<
        "shouldn't be able to pull requests until reservation is restored";

      // Dequeue the last item from both queues after reservation is restored
      pr1 = pq1->pull_request(start_time + 1.0);
      EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
      auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
      EXPECT_EQ(PhaseType::reservation, retn1.phase);
      if (client1 == retn1.client) ++c1_count;
        else ADD_FAILURE() << "got request from no clients";

      pr2 = pq2->pull_request(start_time + 1.0);
      EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
      auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
      EXPECT_EQ(PhaseType::reservation, retn2.phase);
      if (client1 == retn2.client) ++c1_count;
        else ADD_FAILURE() << "got request from no clients";

      EXPECT_EQ(8, c1_count) <<
        "all client1 requests should be dequeued";
    } // dmclock_server_pull_multiq.pull_reservation_demo_immtag_incorrect

    TEST(dmclock_server_pull_multiq, pull_reservation_demo_delydtag_incorrect) {
      // This test using the delayed tag calculation demonstrates the case
      // where a single client interacts with multiple mClock queues. The
      // multiple mClock queues are configued with the same server id.
      // Since the mClock queues are independent, the tag calculations are
      // also independent and this results in inaccurate QoS provided to
      // the client as the test shows.
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;

      dmc::ClientInfo info1(3.0, 1.0, 3.0);

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      QueueRef pq1(new Queue(client_info_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      // make sure all times are well before now
      auto start_time = dmc::get_time() - 100.0;

      // enqueue 8 requests from client1 distributed
      // equally across pq1 and pq2.
      for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(0, pq1->add_request_time(Request{}, client1,
                                           req_params, start_time));
        EXPECT_EQ(0, pq2->add_request_time(Request{}, client1,
                                           req_params, start_time));
        start_time += 0.001;
      }

      int c1_count = 0;
      // Perform dequeue operation on pq1 & pq2 in a 1 second
      // span. Ideally, according to the reservation and limit
      // setting, only 3 requests should be pulled across the
      // queues. But the following sequence demonstrates that
      // double the requests (i.e., 6) are actually dequeued.
      for (double t = 0.0; t <= 0.7; t += 0.35) {
        Queue::PullReq pr1 = pq1->pull_request(start_time + t);
        EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
        auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
        EXPECT_EQ(PhaseType::reservation, retn1.phase);
        if (client1 == retn1.client) ++c1_count;
          else ADD_FAILURE() << "got request from no clients";

        // Perform dequeue operation on pq2
        Queue::PullReq pr2 = pq2->pull_request(start_time + t);
        EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
        auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
        EXPECT_EQ(PhaseType::reservation, retn2.phase);
        if (client1 == retn2.client) ++c1_count;
          else ADD_FAILURE() << "got request from no clients";
      }

      // The number of dequeued items should actually be 3
      // considering the QoS parameters of reservation & limit!
      EXPECT_EQ(6, c1_count) <<
        "3/4th of the client1 requests should be from pq1 & pq2";

      // Dequeueing the last item before reservation is restored
      // should fail with a future work item on both queues
      Queue::PullReq pr1 = pq1->pull_request(start_time + 0.98);
      EXPECT_EQ(Queue::NextReqType::future, pr1.type) <<
        "shouldn't be able to pull requests until reservation is restored";
      Queue::PullReq pr2 = pq2->pull_request(start_time + 0.98);
      EXPECT_EQ(Queue::NextReqType::future, pr2.type) <<
        "shouldn't be able to pull requests until reservation is restored";

      // Dequeue the last item from both queues after reservation is restored
      pr1 = pq1->pull_request(start_time + 1.0);
      EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
      auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
      EXPECT_EQ(PhaseType::reservation, retn1.phase);
      if (client1 == retn1.client) ++c1_count;
        else ADD_FAILURE() << "got request from no clients";

      pr2 = pq2->pull_request(start_time + 1.0);
      EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
      auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
      EXPECT_EQ(PhaseType::reservation, retn2.phase);
      if (client1 == retn2.client) ++c1_count;
        else ADD_FAILURE() << "got request from no clients";

      EXPECT_EQ(8, c1_count) <<
        "all client1 requests should be dequeued";
    } // dmclock_server_pull_multiq.pull_reservation_demo_delydtag_incorrect

    TEST(dmclock_server_pull_multiq, pull_reservation_immtag_correct) {
      // This test demonstrates the correct behavior with the fix for
      // the case when a client tries to dequeue items from multiple
      // queues on the same server. The test demonstrates that requests
      // cannot be pulled from any of the queues until the reservation
      // is restored. The tag calculation method used is ImmediateTagCalc.
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,false,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;

      dmc::ClientInfo info1(3.0, 1.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      // make sure all times are well before now
      auto start_time = dmc::get_time() - 100.0;

      // Perform enqueue operations on pq1
      EXPECT_EQ(0, pq1->add_request_time(Request{}, client1,
                                         req_params, start_time));

      // Perform enqueue operations on pq2
      EXPECT_EQ(0, pq2->add_request_time(Request{}, client1,
                                         req_params, start_time + 0.001));

      // Pull request from pq1
      Queue::PullReq pr1 = pq1->pull_request(start_time);
      EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
      auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
      EXPECT_EQ(PhaseType::reservation, retn1.phase);

      // Pull request from pq2 before reservation is restored -- should fail
      Queue::PullReq pr2 = pq2->pull_request(start_time + 0.05);
      EXPECT_EQ(Queue::NextReqType::future, pr2.type) <<
        "shouldn't be able to pull requests until reservation is restored";

      // Pull request from pq2 after reservation is restored
      pr2 = pq2->pull_request(start_time + 0.35);
      EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
      auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
      EXPECT_EQ(PhaseType::reservation, retn2.phase);
      EXPECT_EQ(1, client_reqtag_map.size());
    } // dmclock_server_pull_multiq.pull_reservation_immtag_correct

    TEST(dmclock_server_pull_multiq, pull_reservation_delydtag_correct) {
      // This test demonstrates the correct behavior with the fix for
      // the case when a client tries to dequeue items from multiple
      // queues on the same server. The test demonstrates that requests
      // cannot be pulled from any of the queues until the reservation
      // is restored. The tag calculation method used is DelayedTagCalc.
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,true,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;

      dmc::ClientInfo info1(3.0, 1.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      // update the tick_interval for a client on specified queue
      auto update_client_pq_tick_f = [&] (ClientId c, unsigned pq_num) {
        auto it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[c].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[c].get_last_tick_interval();
          uint64_t tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[c].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      // make sure all times are well before now
      auto start_time = dmc::get_time() - 100.0;

      // Perform enqueue operations on pq1
      update_client_pq_tick_f(client1, 1);
      EXPECT_EQ(0, pq1->add_request_time(Request{}, client1,
                                         req_params, start_time));
      client_pq_tick_map[client1].update(1);

      // Perform enqueue operations on pq2
      update_client_pq_tick_f(client1, 2);
      EXPECT_EQ(0, pq2->add_request_time(Request{}, client1,
                                         req_params, start_time + 0.001));
      client_pq_tick_map[client1].update(2);

      // Pull request from pq1
      Queue::PullReq pr1 = pq1->pull_request(start_time);
      EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
      auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
      EXPECT_EQ(PhaseType::reservation, retn1.phase);

      // Pull request from pq2 before reservation is restored -- should fail
      Queue::PullReq pr2 = pq2->pull_request(start_time + 0.05);
      EXPECT_EQ(Queue::NextReqType::future, pr2.type) <<
        "shouldn't be able to pull requests until reservation is restored";

      // Pull request from pq2 after reservation is restored
      pr2 = pq2->pull_request(start_time + 0.35);
      EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
      auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
      EXPECT_EQ(PhaseType::reservation, retn2.phase);
      EXPECT_EQ(1, client_reqtag_map.size());
    } // dmclock_server_pull_multiq.pull_reservation_delydtag_correct

    TEST(dmclock_server_pull_multiq, pull_reservation_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Req, false, false, true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(5.0, 0.0, 0.0);
      dmc::ClientInfo info2(10.0, 0.0, 0.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      auto start_time = dmc::get_time() - 100;
      auto deq_time = start_time;

      ReqParams req_params(0, 0);
      constexpr unsigned num_requests = 20;

      // Add requests from both clients to both mClock queues.
      // Therefore each queue will have the following:
      // client1 -> pq1 = 5 reqs; client1 -> pq2 = 5 reqs
      // client2 -> pq1 = 5 reqs; client2 -> pq2 = 5 reqs
      int data = 0;
      for (int i = 0; i < num_requests; i += 4) {
        // client1 -> pq1
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1, req_params, start_time));

        // client2 -> pq1
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client2, req_params, start_time));

        start_time += 0.001;
        data++;

        // client1 -> pq2
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1, req_params, start_time));

        // client2 -> pq2
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client2, req_params, start_time));

        start_time += 0.001;
        data++;
      }
      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;

      for (double t = 0.0; t <= 0.9; t+=0.1) {
        Queue::PullReq pr1 = pq1->pull_request(deq_time + t);
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          EXPECT_EQ(PhaseType::reservation, retn1.phase);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        // Add minimal delay but which is still within the reservation
        // time boundary for both clients.
        deq_time += 0.001;
        Queue::PullReq pr2 = pq2->pull_request(deq_time + t);
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          EXPECT_EQ(PhaseType::reservation, retn2.phase);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        // Add some delay for the next phase
        deq_time += 0.01;
      }

      EXPECT_EQ(2, client_reqtag_map.size());
      EXPECT_EQ(5, c1_count) <<
        "1/3rd of the requests during reservation phase should be from client1";
      EXPECT_EQ(10, c2_count) <<
        "2/3rd of the requests during reservation phase should be from client2";
    } // dmclock_server_pull_multiq.pull_reservation_immtag

    TEST(dmclock_server_pull_multiq, pull_reservation_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Req, true, false, true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(5.0, 0.0, 0.0);
      dmc::ClientInfo info2(10.0, 0.0, 0.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()},
                            {client2, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      // update the tick_interval for a client on specified queue
      auto update_client_pq_tick_f = [&] (ClientId c, unsigned pq_num) {
        auto it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[c].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[c].get_last_tick_interval();
          uint64_t tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[c].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      auto start_time = dmc::get_time() - 100;
      auto deq_time = start_time;

      ReqParams req_params(0, 0);
      constexpr unsigned num_requests = 20;

      // Add requests from both clients to both mClock queues.
      // Therefore each queue will have the following:
      // client1 -> pq1 = 5 reqs; client1 -> pq2 = 5 reqs
      // client2 -> pq1 = 5 reqs; client2 -> pq2 = 5 reqs
      int data = 0;
      for (int i = 0; i < num_requests; i += 4) {
        // client1 -> pq1
        update_client_pq_tick_f(client1, 1);
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1,
                                           req_params, start_time));
        client_pq_tick_map[client1].update(1);

        // client2 -> pq1
        update_client_pq_tick_f(client2, 1);
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client2,
                                           req_params, start_time));
        client_pq_tick_map[client2].update(1);

        start_time += 0.001;
        data++;

        // client1 -> pq2
        update_client_pq_tick_f(client1, 2);
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1,
                                           req_params, start_time));
        client_pq_tick_map[client1].update(2);

        // client2 -> pq2
        update_client_pq_tick_f(client2, 2);
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client2,
                                           req_params, start_time));
        client_pq_tick_map[client2].update(2);

        start_time += 0.001;
        data++;
      }
      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;

      for (double t = 0.0; t <= 0.9; t+=0.1) {
        Queue::PullReq pr1 = pq1->pull_request(deq_time + t);
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          EXPECT_EQ(PhaseType::reservation, retn1.phase);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        // Add minimal delay but which is still within the reservation
        // time boundary for both clients.
        deq_time += 0.001;
        Queue::PullReq pr2 = pq2->pull_request(deq_time + t);
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          EXPECT_EQ(PhaseType::reservation, retn2.phase);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        // Add some delay for the next phase
        deq_time += 0.01;
      }

      EXPECT_EQ(2, client_reqtag_map.size());
      EXPECT_EQ(5, c1_count) <<
        "1/3rd of the requests during reservation phase should be from client1";
      EXPECT_EQ(10, c2_count) <<
        "2/3rd of the requests during reservation phase should be from client2";
    } // dmclock_server_pull_multiq.pull_reservation_delydtag

    TEST(dmclock_server_pull_multiq, pull_weight_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Req, false, false, true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 1.0);
      dmc::ClientInfo info2(0.0, 3.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      auto start_time = dmc::get_time() - 100;
      auto deq_time = start_time;

      ReqParams req_params(0, 0);
      constexpr unsigned num_requests = 20;

      // Add requests from both clients to both mClock queues.
      // Therefore each queue will have the following:
      // client1 -> pq1 = 5 reqs; client1 -> pq2 = 5 reqs
      // client2 -> pq1 = 5 reqs; client2 -> pq2 = 5 reqs
      int data = 0;
      for (int i = 0; i < num_requests; i += 4) {
        // client1 -> pq1
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1, req_params, start_time));

        // client2 -> pq1
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client2, req_params, start_time));

        start_time += 0.001;
        data++;

        // client1 -> pq2
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1, req_params, start_time));

        // client2 -> pq2
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client2, req_params, start_time));

        start_time += 0.001;
        data++;
      }
      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;

      for (double t = 0.0; t <= 2.1; t+=0.1) {
        Queue::PullReq pr1 = pq1->pull_request(deq_time + t);
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          EXPECT_EQ(PhaseType::priority, retn1.phase);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        // Add minimal delay but which is still within the limit
        // time boundary for both clients.
        deq_time += 0.001;
        Queue::PullReq pr2 = pq2->pull_request(deq_time + t);
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          EXPECT_EQ(PhaseType::priority, retn2.phase);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        deq_time += 0.01;
      }

      EXPECT_EQ(2, client_reqtag_map.size());
      EXPECT_EQ(3, c1_count) <<
        "1/3rd of the requests during reservation phase should be from client1";
      EXPECT_EQ(7, c2_count) <<
        "2/3rds of the requests during reservation phase should be from client2";
    } // dmclock_server_pull_multiq.pull_weight_immtag

    TEST(dmclock_server_pull_multiq, pull_weight_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId, Req, true, false, true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 1.0);
      dmc::ClientInfo info2(0.0, 3.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()},
                            {client2, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      // update the tick_interval for a client on specified queue
      auto update_client_pq_tick_f = [&] (ClientId c, unsigned pq_num) {
        auto it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[c].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[c].get_last_tick_interval();
          uint64_t tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[c].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      auto start_time = dmc::get_time() - 100;
      auto deq_time = start_time;

      ReqParams req_params(0, 0);
      constexpr unsigned num_requests = 20;

      // Add requests from both clients to both mClock queues.
      // Therefore each queue will have the following:
      // client1 -> pq1 = 5 reqs; client1 -> pq2 = 5 reqs
      // client2 -> pq1 = 5 reqs; client2 -> pq2 = 5 reqs
      int data = 0;
      for (int i = 0; i < num_requests; i += 4) {
        // client1 -> pq1
        update_client_pq_tick_f(client1, 1);
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1,
                                           req_params, start_time));
        client_pq_tick_map[client1].update(1);

        // client2 -> pq1
        update_client_pq_tick_f(client2, 1);
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client2,
                                           req_params, start_time));
        client_pq_tick_map[client2].update(1);

        start_time += 0.001;
        data++;

        // client1 -> pq2
        update_client_pq_tick_f(client1, 2);
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1,
                                           req_params, start_time));
        client_pq_tick_map[client1].update(2);

        // client2 -> pq2
        update_client_pq_tick_f(client2, 2);
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client2,
                                           req_params, start_time));
        client_pq_tick_map[client2].update(2);

        start_time += 0.001;
        data++;
      }

      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;

      for (double t = 0.0; t <= 2.1; t+=0.1) {
        Queue::PullReq pr1 = pq1->pull_request(deq_time + t);
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          EXPECT_EQ(PhaseType::priority, retn1.phase);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        // Add minimal delay but which is still within the limit
        // time boundary for both clients.
        deq_time += 0.001;

        Queue::PullReq pr2 = pq2->pull_request(deq_time + t);
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          EXPECT_EQ(PhaseType::priority, retn2.phase);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
        }
        deq_time += 0.01;
      }

      EXPECT_EQ(2, client_reqtag_map.size());
      EXPECT_EQ(3, c1_count) <<
        "1/3rd of the requests during reservation phase should be from client1";
      EXPECT_EQ(7, c2_count) <<
        "2/3rds of the requests during reservation phase should be from client2";
    } // dmclock_server_pull_multiq.pull_weight_delydtag

    TEST(dmclock_server_pull_multiq, pull_weight_mult_reqs_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,true,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 1.0);
      dmc::ClientInfo info2(0.0, 3.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()},
                            {client2, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      // update the tick_interval for a client on specified queue
      auto update_client_pq_tick_f = [&] (ClientId c, unsigned pq_num) {
        auto it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[c].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[c].get_last_tick_interval();
          uint64_t tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[c].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        update_client_pq_tick_f(client1, 1);
        EXPECT_EQ(0, pq1->add_request(Req{data}, client1, req_params));
        client_pq_tick_map[client1].update(1);

        update_client_pq_tick_f(client2, 1);
        EXPECT_EQ(0, pq1->add_request(Req{data}, client2, req_params));
        client_pq_tick_map[client2].update(1);

        data++;

        update_client_pq_tick_f(client1, 2);
        EXPECT_EQ(0, pq2->add_request(Req{data}, client1, req_params));
        client_pq_tick_map[client1].update(2);

        update_client_pq_tick_f(client2, 2);
        EXPECT_EQ(0, pq2->add_request(Req{data}, client2, req_params));
        client_pq_tick_map[client2].update(2);

        data++;
      }

      EXPECT_EQ(20u, pq1->request_count());
      EXPECT_EQ(20u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request();
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::priority, retn1.phase);
          num_reqs++;
        }

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request();
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::priority, retn2.phase);
          num_reqs++;
        }
      }

      EXPECT_EQ(5, c1_count) <<
        "one-fourth of requests should have come from first client";
      EXPECT_EQ(15, c2_count) <<
        "three-fourth of requests should have come from second client";
    } // dmclock_server_pull_multiq.pull_weight_mult_reqs_delydtag

    TEST(dmclock_server_pull_multiq, pull_weight_mult_reqs_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,false,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 1.0);
      dmc::ClientInfo info2(0.0, 3.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(0, pq1->add_request(Req{data}, client1, req_params));
        EXPECT_EQ(0, pq1->add_request(Req{data}, client2, req_params));
        data++;
        EXPECT_EQ(0, pq2->add_request(Req{data}, client1, req_params));
        EXPECT_EQ(0, pq2->add_request(Req{data}, client2, req_params));
        data++;
      }

      EXPECT_EQ(20u, pq1->request_count());
      EXPECT_EQ(20u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request();
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::priority, retn1.phase);
          num_reqs++;
        }

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request();
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::priority, retn2.phase);
          num_reqs++;
        }
      }

      EXPECT_EQ(5, c1_count) <<
        "one-fourth of requests should have come from first client";
      EXPECT_EQ(15, c2_count) <<
        "three-fourth of requests should have come from second client";
    } // dmclock_server_pull_multiq.pull_weight_mult_reqs_immtag

    TEST(dmclock_server_pull_multiq, pull_reservation_mult_reqs_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,true,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(1.0, 1.0, 1.0);
      dmc::ClientInfo info2(3.0, 1.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()},
                            {client2, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      // update the tick_interval for a client on specified queue
      auto update_client_pq_tick_f = [&] (ClientId c, unsigned pq_num) {
        auto it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[c].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[c].get_last_tick_interval();
          uint64_t tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[c].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        update_client_pq_tick_f(client1, 1);
        EXPECT_EQ(0, pq1->add_request(Req{data}, client1, req_params));
        client_pq_tick_map[client1].update(1);

        update_client_pq_tick_f(client2, 1);
        EXPECT_EQ(0, pq1->add_request(Req{data}, client2, req_params));
        client_pq_tick_map[client2].update(1);

        data++;

        update_client_pq_tick_f(client1, 2);
        EXPECT_EQ(0, pq2->add_request(Req{data}, client1, req_params));
        client_pq_tick_map[client1].update(2);

        update_client_pq_tick_f(client2, 2);
        EXPECT_EQ(0, pq2->add_request(Req{data}, client2, req_params));
        client_pq_tick_map[client2].update(2);

        data++;
      }

      EXPECT_EQ(20u, pq1->request_count());
      EXPECT_EQ(20u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request();
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::reservation, retn1.phase);
          num_reqs++;
        }

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request();
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::reservation, retn2.phase);
          num_reqs++;
        }
      }

      EXPECT_EQ(5, c1_count) <<
        "one-fourth of requests should have come from first client";
      EXPECT_EQ(15, c2_count) <<
        "three-fourth of requests should have come from second client";
    } // dmclock_server_pull_multiq.pull_reservation_mult_reqs_delydtag

    TEST(dmclock_server_pull_multiq, pull_reservation_mult_reqs_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,false,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(1.0, 1.0, 1.0);
      dmc::ClientInfo info2(3.0, 1.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Wait));

      ReqParams req_params(0, 0);

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(0, pq1->add_request(Req{data}, client1, req_params));
        EXPECT_EQ(0, pq1->add_request(Req{data}, client2, req_params));
        data++;
        EXPECT_EQ(0, pq2->add_request(Req{data}, client1, req_params));
        EXPECT_EQ(0, pq2->add_request(Req{data}, client2, req_params));
        data++;
      }

      EXPECT_EQ(20u, pq1->request_count());
      EXPECT_EQ(20u, pq2->request_count());

      int c1_count = 0;
      int c2_count = 0;
      int c1_data = 0;
      int c2_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request();
        if (pr1.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr1.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
          auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
          if (client1 == retn1.client) {
            ++c1_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn1.client) {
            ++c2_count;
            auto r = std::move(*retn1.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::reservation, retn1.phase);
          num_reqs++;
        }

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request();
        if (pr2.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr2.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
          auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
          if (client1 == retn2.client) {
            ++c1_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn2.client) {
            ++c2_count;
            auto r = std::move(*retn2.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::reservation, retn2.phase);
          num_reqs++;
        }
      }

      EXPECT_EQ(5, c1_count) <<
        "one-fourth of requests should have come from first client";
      EXPECT_EQ(15, c2_count) <<
        "three-fourth of requests should have come from second client";
    } // dmclock_server_pull_multiq.pull_reservation_mult_reqs_immtag

    TEST(dmclock_server_pull_multiq, pull_future_limit_break_weight_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,true,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;

      dmc::ClientInfo info1(0.0, 3.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      // update the tick_interval for a client on specified queue
      auto update_client_pq_tick_f = [&] (ClientId c, unsigned pq_num) {
        auto it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[c].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[c].get_last_tick_interval();
          uint64_t tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[c].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));

      ReqParams req_params(0, 0);

      auto now = dmc::get_time();

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        update_client_pq_tick_f(client1, 1);
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        client_pq_tick_map[client1].update(1);
        data++;
        now += 0.001;

        update_client_pq_tick_f(client1, 2);
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        client_pq_tick_map[client1].update(2);
        data++;
        now += 0.001;
      }

      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c1_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
        auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
        if (client1 == retn1.client) {
          ++c1_count;
          auto r = std::move(*retn1.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::priority, retn1.phase);
        num_reqs++;

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
        auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
        if (client1 == retn2.client) {
          ++c1_count;
          auto r = std::move(*retn2.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::priority, retn2.phase);
        num_reqs++;
        now += 0.001;
      }

      EXPECT_EQ(20, c1_count) <<
        "All requests from client must be dequeued with AtLimit::Allow";
    } // dmclock_server_pull_multiq.pull_future_limit_break_weight_delydtag

    TEST(dmclock_server_pull_multiq, pull_future_limit_break_weight_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,false,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;

      dmc::ClientInfo info1(0.0, 3.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));

      ReqParams req_params(0, 0);

      auto now = dmc::get_time();

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        data++;
        now += 0.001;

        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        data++;
        now += 0.001;
      }

      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c1_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
        auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
        if (client1 == retn1.client) {
          ++c1_count;
          auto r = std::move(*retn1.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::priority, retn1.phase);
        num_reqs++;

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
        auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
        if (client1 == retn2.client) {
          ++c1_count;
          auto r = std::move(*retn2.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::priority, retn2.phase);
        num_reqs++;
        now += 0.001;
      }

      EXPECT_EQ(20, c1_count) <<
        "All requests from client must be dequeued with AtLimit::Allow";
    } // dmclock_server_pull_multiq.pull_future_limit_break_weight_immtag

    TEST(dmclock_server_pull_multiq, pull_future_limit_break_reservation_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,true,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;

      dmc::ClientInfo info1(3.0, 0.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      client_pq_tick_map = {{client1, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      // update the tick_interval for a client on specified queue
      auto update_client_pq_tick_f = [&] (ClientId c, unsigned pq_num) {
        auto it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[c].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[c].get_last_tick_interval();
          uint64_t tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[c].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));

      ReqParams req_params(0, 0);

      auto now = dmc::get_time();

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        update_client_pq_tick_f(client1, 1);
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        client_pq_tick_map[client1].update(1);
        data++;
        now += 0.001;

        update_client_pq_tick_f(client1, 2);
        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        client_pq_tick_map[client1].update(2);
        data++;
        now += 0.001;
      }

      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c1_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
        auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
        if (client1 == retn1.client) {
          ++c1_count;
          auto r = std::move(*retn1.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::reservation, retn1.phase);
        num_reqs++;

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
        auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
        if (client1 == retn2.client) {
          ++c1_count;
          auto r = std::move(*retn2.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::reservation, retn2.phase);
        num_reqs++;
        now += 0.001;
      }

      EXPECT_EQ(20, c1_count) <<
        "All requests from client must be dequeued with AtLimit::Allow";
    } // dmclock_server_pull_multiq.pull_future_limit_break_reservation_delydtag

    TEST(dmclock_server_pull_multiq, pull_future_limit_break_reservation_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,false,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;

      dmc::ClientInfo info1(3.0, 0.0, 3.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Allow));

      ReqParams req_params(0, 0);

      auto now = dmc::get_time();

      int data = 0;
      for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(0, pq1->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        auto t1 = reqtag_info_f(client1);
        data++;
        now += 0.001;

        EXPECT_EQ(0, pq2->add_request_time(Req{data}, client1,
                                           req_params, now + 100));
        auto t2 = reqtag_info_f(client1);
        data++;
        now += 0.001;
      }

      EXPECT_EQ(10u, pq1->request_count());
      EXPECT_EQ(10u, pq2->request_count());

      int c1_count = 0;
      int c1_data = 0;
      int num_reqs = 0;
      while (num_reqs < 20) {
        // Pull req from pq1
        Queue::PullReq pr1 = pq1->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr1.type);
        auto& retn1 = boost::get<Queue::PullReq::Retn>(pr1.data);
        if (client1 == retn1.client) {
          ++c1_count;
          auto r = std::move(*retn1.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::reservation, retn1.phase);
        num_reqs++;

        // Pull req from pq2
        Queue::PullReq pr2 = pq2->pull_request(now);
        EXPECT_EQ(Queue::NextReqType::returning, pr2.type);
        auto& retn2 = boost::get<Queue::PullReq::Retn>(pr2.data);
        if (client1 == retn2.client) {
          ++c1_count;
          auto r = std::move(*retn2.request);
          EXPECT_EQ(r.data, c1_data++);
        } else {
          ADD_FAILURE() << "got no request from client";
        }
        EXPECT_EQ(PhaseType::reservation, retn2.phase);
        num_reqs++;
        now += 0.001;
      }

      EXPECT_EQ(20, c1_count) <<
        "All requests from client must be dequeued with AtLimit::Allow";
    } // dmclock_server_pull_multiq.pull_future_limit_break_reservation_immtag

    TEST(dmclock_server_pull_multiq, pull_reject_at_limit_immtag) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request,false,false,true>;
      using MyReqRef = typename Queue::RequestRef;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 52;
      ClientId client2 = 53;

      dmc::ClientInfo info1(0.0, 4.0, 4.0);
      dmc::ClientInfo info2(0.0, 1.0, 1.0);

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      QueueRef pq1(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Reject));
      QueueRef pq2(new Queue(client_info_f, reqtag_info_f,
                             reqtag_updt_f, AtLimit::Reject));

      ReqParams req_params(0, 0);

      {
        // success at 4 requests per second
        EXPECT_EQ(0, pq1->add_request_time({}, client1, {}, Time{0.25}));
        EXPECT_EQ(0, pq1->add_request_time({}, client1, {}, Time{0.50}));
        EXPECT_EQ(0, pq1->add_request_time({}, client1, {}, Time{0.75}));
        // request too soon on the second pq
        EXPECT_EQ(EAGAIN, pq2->add_request_time({}, client1, {}, Time{0.90}));
        // previous rejected request counts against limit
        EXPECT_EQ(EAGAIN, pq2->add_request_time({}, client1, {}, Time{1.0}));
        EXPECT_EQ(0, pq2->add_request_time({}, client1, {}, Time{2.0}));
      }
      {
        auto r1 = MyReqRef{new Request};
        // add r1 to pq1
        ASSERT_EQ(0, pq1->add_request(std::move(r1), client2, {}, Time{1}));
        EXPECT_EQ(nullptr, r1); // add_request takes r1 on success
        auto r2 = MyReqRef{new Request};
        // add r2 to pq2
        ASSERT_EQ(EAGAIN, pq2->add_request(std::move(r2), client2, {}, Time{1}));
        EXPECT_NE(nullptr, r2); // add_request does not take r2 on failure
      }
    } // pull_reject_at_limit_immtag

    TEST(dmclock_server_pull_multiq, pull_reservation_randomize_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,false,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      std::random_device rd;
      std::mt19937 random_gen(rd());

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(50.0, 1.0, 50.0);
      dmc::ClientInfo info2(100.0, 1.0, 100.0);
      ReqParams req_params(0, 0);

      const uint8_t num_pqs = 5;

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      auto get_pq_num_f = [&] () {
        return random_gen() % num_pqs;
      };

      auto get_client_num_f = [&] () {
        return (random_gen() % 2 < 1) ? client1 : client2;
      };

      // Distribute requests across all queues
      std::array<QueueRef, num_pqs> pq_arr;
      for (uint8_t i = 0; i < num_pqs; i++) {
        pq_arr[i] = QueueRef(new Queue(client_info_f, reqtag_info_f,
                                       reqtag_updt_f, AtLimit::Wait));
      }

      // function to enqueue to desired priority queue
      auto enqueue_to_pq_f = [&] (uint8_t pq_num, int client, int data, Time t) {
        if (t != dmc::TimeZero) {
          pq_arr[pq_num]->add_request_time(Req{data}, client, req_params, t);
        } else {
          pq_arr[pq_num]->add_request(Req{data}, client, req_params);
        }
      };

      // function to pull reqs from desired priority queue
      auto pull_request_f = [&] (uint8_t pq_num, Time t) -> Queue::PullReq {
        if (t != dmc::TimeZero) {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request(t);
          return std::move(pr);
        } else {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request();
          return std::move(pr);
        }
      };

      int c1_data = 0;
      int c2_data = 0;
      int c1_req_count = 0;
      int c2_req_count = 0;
      int client_id = 0;
      uint8_t pq_num = 0;
      const int total_reqs = 400;

      // ENQUEUE ITEMS
      // Randomly add reqs across all the queues
      for (int i = 0; i < total_reqs; ++i) {
        pq_num = get_pq_num_f();
        client_id = get_client_num_f();

        auto data = (client_id == client1) ? c1_data : c2_data;
        enqueue_to_pq_f(pq_num, client_id, data, dmc::TimeZero);
        if (client_id == client1) {
          c1_data++;
          c1_req_count++;
        } else if(client_id == client2) {
          c2_data++;
          c2_req_count++;
        } else {
          ADD_FAILURE() << "invalid client_id...cannot add request";
        }
      }

      int total_req_cnt = 0;
      for (uint8_t i = 0; i < num_pqs; i++) {
        total_req_cnt += pq_arr[i]->request_count();
      }
      EXPECT_EQ(total_req_cnt, total_reqs);

      // DEQUEUE ITEMS
      int c1_deq_count = 0;
      int c2_deq_count = 0;
      c1_data = 0;
      c2_data = 0;
      int num_reqs = 0;
      const int num_reqs_to_dequeue = 300;
      // Randomly dequeue reqs from all queues until num_reqs are pulled
      while (num_reqs < num_reqs_to_dequeue) {
        pq_num = get_pq_num_f();
        Queue::PullReq pr = pull_request_f(pq_num, dmc::TimeZero);
        if (pr.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr.type);
          auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
          if (client1 == retn.client) {
            ++c1_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn.client) {
            ++c2_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::reservation, retn.phase);
          num_reqs++;
        }
      }

      // Expected client dequeue counts depending on req params
      const int exp_c1_deq_count = 100;
      const int exp_c2_deq_count = 200;

      // Due to the random nature of the test, it's quite possible
      // for client requests generated to be less than the reservation
      // or limit values. The following checks the dequeued counts
      // accordingly for each client.
      if (c1_req_count < exp_c1_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count - (exp_c1_deq_count - c1_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count + (exp_c1_deq_count - c1_req_count),
          c2_deq_count);
      } else if (c2_req_count < exp_c2_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count + (exp_c2_deq_count - c2_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count - (exp_c2_deq_count - c2_req_count),
          c2_deq_count);
      } else { // normal case: desired client req counts were generated
        EXPECT_EQ(exp_c1_deq_count, c1_deq_count);
        EXPECT_EQ(exp_c2_deq_count, c2_deq_count);
      }
    } // dmclock_server_pull_multiq.pull_reservation_randomize_immtag

    TEST(dmclock_server_pull_multiq, pull_reservation_randomize_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Counter = uint64_t;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,true,false,true>;
      using QueueRef = std::unique_ptr<Queue>;
      using ReqTagInfoTime = std::pair<dmc::RequestTag, Time>;
      std::random_device rd;
      std::mt19937 random_gen(rd());

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(50.0, 1.0, 50.0);
      dmc::ClientInfo info2(100.0, 1.0, 100.0);
      ReqParams req_params(0, 0);

      const uint8_t num_pqs = 5;

      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      // initialize the per client tick counts
      client_pq_tick_map = {{client1, client_tick_t()},
                            {client2, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      auto get_pq_num_f = [&] () {
        return random_gen() % num_pqs;
      };

      auto get_client_num_f = [&] () {
        return (random_gen() % 2 < 1) ? client1 : client2;
      };

      // Distribute requests across all queues
      std::array<QueueRef, num_pqs> pq_arr;
      for (uint8_t i = 0; i < num_pqs; i++) {
        pq_arr[i] = QueueRef(new Queue(client_info_f, reqtag_info_f,
                                       reqtag_updt_f, AtLimit::Wait));
      }

      // function to enqueue to desired priority queue
      auto enqueue_to_pq_f = [&] (uint8_t pq_num, int client, int data, Time t) {
        if (t != dmc::TimeZero) {
          pq_arr[pq_num]->add_request_time(Req{data}, client, req_params, t);
        } else {
          pq_arr[pq_num]->add_request(Req{data}, client, req_params);
        }
      };

      // function to pull reqs from desired priority queue
      auto pull_request_f = [&] (uint8_t pq_num, Time t) -> Queue::PullReq {
        if (t != dmc::TimeZero) {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request(t);
          return std::move(pr);
        } else {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request();
          return std::move(pr);
        }
      };

      int c1_data = 0;
      int c2_data = 0;
      int c1_req_count = 0;
      int c2_req_count = 0;
      int client_id = 0;
      uint8_t pq_num = 0;
      const int total_reqs = 400;

      // ENQUEUE ITEMS
      // Randomly add reqs across all the queues
      for (int i = 0; i < total_reqs; ++i) {
        pq_num = get_pq_num_f();
        client_id = get_client_num_f();

        // update the tick_diff for the client on this queue
        auto it = client_reqtag_map.find(client_id);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[client_id].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[client_id].get_last_tick_interval();
          Counter tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[client_id].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }

        auto data = (client_id == client1) ? c1_data : c2_data;
        enqueue_to_pq_f(pq_num, client_id, data, dmc::TimeZero);
        client_pq_tick_map[client_id].update(pq_num);
        if (client_id == client1) {
          c1_data++;
          c1_req_count++;
        } else if(client_id == client2) {
          c2_data++;
          c2_req_count++;
        } else {
          ADD_FAILURE() << "invalid client_id...cannot add request";
        }
      }

      int total_req_cnt = 0;
      for (uint8_t i = 0; i < num_pqs; i++) {
        total_req_cnt += pq_arr[i]->request_count();
      }
      EXPECT_EQ(total_req_cnt, total_reqs);

      // DEQUEUE ITEMS
      int c1_deq_count = 0;
      int c2_deq_count = 0;
      c1_data = 0;
      c2_data = 0;
      //int num_reqs = 0;
      int num_reqs_to_dequeue = 300;
      pq_num = 0;
      // Randomly dequeue reqs from all queues until num_reqs are pulled
      while (num_reqs_to_dequeue > 0) {
        pq_num = get_pq_num_f();
        Queue::PullReq pr = pull_request_f(pq_num, dmc::TimeZero);
        if (pr.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr.type);
          auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
          if (client1 == retn.client) {
            ++c1_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn.client) {
            ++c2_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::reservation, retn.phase);
          num_reqs_to_dequeue--;
        }
      }

      // Expected client dequeue counts depending on req params
      const int exp_c1_deq_count = 100;
      const int exp_c2_deq_count = 200;

      // Due to the random nature of the test, it's quite possible
      // for client requests generated to be less than the reservation
      // or limit values. The following checks the dequeued counts
      // accordingly for each client.
      if (c1_req_count < exp_c1_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count - (exp_c1_deq_count - c1_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count + (exp_c1_deq_count - c1_req_count),
          c2_deq_count);
      } else if (c2_req_count < exp_c2_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count + (exp_c2_deq_count - c2_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count - (exp_c2_deq_count - c2_req_count),
          c2_deq_count);
      } else { // normal case: desired client req counts were generated
        EXPECT_EQ(exp_c1_deq_count, c1_deq_count);
        EXPECT_EQ(exp_c2_deq_count, c2_deq_count);
      }
    } // dmclock_server_pull_multiq.pull_reservation_randomize_delydtag

    TEST(dmclock_server_pull_multiq, pull_weight_randomize_immtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,false,false,true>;
      using QueueRef = std::unique_ptr<Queue>;

      std::random_device rd;
      std::mt19937 random_gen(rd());

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 50.0);
      dmc::ClientInfo info2(0.0, 3.0, 150.0);
      ReqParams req_params(0, 0);

      const uint8_t num_pqs = 5;

      // Map maintained by clients of dmClock server
      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      auto get_pq_num_f = [&] () {
        return random_gen() % num_pqs;
      };

      auto get_client_num_f = [&] () {
        return (random_gen() % 2 < 1) ? client1 : client2;
      };

      // Distribute requests across all queues
      std::array<QueueRef, num_pqs> pq_arr;
      for (uint8_t i = 0; i < num_pqs; i++) {
        pq_arr[i] = QueueRef(new Queue(client_info_f, reqtag_info_f,
                                       reqtag_updt_f, AtLimit::Wait));
      }

      // function to enqueue to desired priority queue
      auto enqueue_to_pq_f = [&] (uint8_t pq_num, int client, int data, Time t) {
        if (t != dmc::TimeZero) {
          pq_arr[pq_num]->add_request_time(Req{data}, client, req_params, t);
        } else {
          pq_arr[pq_num]->add_request(Req{data}, client, req_params);
        }
      };

      // function to pull reqs from desired priority queue
      auto pull_request_f = [&] (uint8_t pq_num, Time t) -> Queue::PullReq {
        if (t != dmc::TimeZero) {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request(t);
          return std::move(pr);
        } else {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request();
          return std::move(pr);
        }
      };

      int c1_data = 0;
      int c2_data = 0;
      int c1_req_count = 0;
      int c2_req_count = 0;
      int client_id = 0;
      uint8_t pq_num = 0;
      const int total_reqs = 400;

      // ENQUEUE ITEMS
      // Randomly add reqs across all the queues
      for (int i = 0; i < total_reqs; ++i) {
        pq_num = get_pq_num_f();
        client_id = get_client_num_f();

        auto data = (client_id == client1) ? c1_data : c2_data;
        enqueue_to_pq_f(pq_num, client_id, data, dmc::TimeZero);
        if (client_id == client1) {
          c1_data++;
          c1_req_count++;
        } else if(client_id == client2) {
          c2_data++;
          c2_req_count++;
        } else {
          ADD_FAILURE() << "invalid client_id...cannot add request";
        }
      }

      int total_req_cnt = 0;
      for (uint8_t i = 0; i < num_pqs; i++) {
        total_req_cnt += pq_arr[i]->request_count();
      }
      EXPECT_EQ(total_req_cnt, total_reqs);

      // DEQUEUE ITEMS
      int c1_deq_count = 0;
      int c2_deq_count = 0;
      c1_data = 0;
      c2_data = 0;
      int num_reqs = 0;
      const int num_reqs_to_dequeue = 300;
      // Randomly dequeue reqs from all queues until num_reqs are pulled
      while (num_reqs < num_reqs_to_dequeue) {
        pq_num = get_pq_num_f();
        Queue::PullReq pr = pull_request_f(pq_num, dmc::TimeZero);
        if (pr.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr.type);
          auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
          if (client1 == retn.client) {
            ++c1_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn.client) {
            ++c2_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::priority, retn.phase);
          num_reqs++;
        }
      }

      // Expected client dequeue counts depending on req params
      const int exp_c1_deq_count = 75;
      const int exp_c2_deq_count = 225;

      // Due to the random nature of the test, it's quite possible
      // for client requests generated to be less than the reservation
      // or limit values. The following checks the dequeued counts
      // accordingly for each client.
      if (c1_req_count < exp_c1_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count - (exp_c1_deq_count - c1_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count + (exp_c1_deq_count - c1_req_count),
          c2_deq_count);
      } else if (c2_req_count < exp_c2_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count + (exp_c2_deq_count - c2_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count - (exp_c2_deq_count - c2_req_count),
          c2_deq_count);
      } else { // normal case: desired client req counts were generated
        EXPECT_EQ(exp_c1_deq_count, c1_deq_count);
        EXPECT_EQ(exp_c2_deq_count, c2_deq_count);
      }
    } // dmclock_server_pull_multiq.pull_weight_randomize_immtag

    TEST(dmclock_server_pull_multiq, pull_weight_randomize_delydtag) {
      struct Req {
        int data;
      };
      using ClientId = int;
      using Counter = uint64_t;
      using Queue = dmc::PullPriorityQueue<ClientId,Req,true,false,true>;
      using QueueRef = std::unique_ptr<Queue>;
      std::random_device rd;
      std::mt19937 random_gen(rd());

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 50.0);
      dmc::ClientInfo info2(0.0, 3.0, 150.0);
      ReqParams req_params(0, 0);

      const uint8_t num_pqs = 5;

      std::map<ClientId, dmc::ReqTagInfo> client_reqtag_map;
      client_reqtag_map = {{client1, dmc::ReqTagInfo()},
                           {client2, dmc::ReqTagInfo()}};
      std::map<ClientId, client_tick_t> client_pq_tick_map;
      // initialize the per client tick counts
      client_pq_tick_map = {{client1, client_tick_t()},
                            {client2, client_tick_t()}};

      auto client_info_f = [&] (ClientId c) -> const dmc::ClientInfo* {
        if (client1 == c) return &info1;
        else if (client2 == c) return &info2;
        else {
          ADD_FAILURE() << "client info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_info_f = [&] (ClientId c) -> const dmc::ReqTagInfo* {
        auto client_it = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != client_it) {
          return &(client_it->second);
        } else {
          ADD_FAILURE() << "request tag info looked up for non-existent client";
          return nullptr;
        }
      };

      auto reqtag_updt_f = [&] (ClientId c, dmc::RequestTag t) {
        auto insert = client_reqtag_map.find(c);
        if (client_reqtag_map.end() != insert) {
          insert->second.update_tag(t);
        } else {
          client_reqtag_map.emplace(c, ReqTagInfo(t, 0));
        }
      };

      auto get_pq_num_f = [&] () {
        return random_gen() % num_pqs;
      };

      auto get_client_num_f = [&] () {
        return (random_gen() % 2 < 1) ? client1 : client2;
      };

      // Distribute requests across all queues
      std::array<QueueRef, num_pqs> pq_arr;
      for (uint8_t i = 0; i < num_pqs; i++) {
        pq_arr[i] = QueueRef(new Queue(client_info_f, reqtag_info_f,
                                       reqtag_updt_f, AtLimit::Wait));
      }

      // function to enqueue to desired priority queue
      auto enqueue_to_pq_f = [&] (uint8_t pq_num, int client, int data, Time t) {
        if (t != dmc::TimeZero) {
          pq_arr[pq_num]->add_request_time(Req{data}, client, req_params, t);
        } else {
          pq_arr[pq_num]->add_request(Req{data}, client, req_params);
        }
      };

      // function to pull reqs from desired priority queue
      auto pull_request_f = [&] (uint8_t pq_num, Time t) -> Queue::PullReq {
        if (t != dmc::TimeZero) {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request(t);
          return std::move(pr);
        } else {
          Queue::PullReq pr = pq_arr[pq_num]->pull_request();
          return std::move(pr);
        }
      };

      int c1_data = 0;
      int c2_data = 0;
      int c1_req_count = 0;
      int c2_req_count = 0;
      int client_id = 0;
      uint8_t pq_num = 0;
      const int total_reqs = 400;

      // ENQUEUE ITEMS
      // Randomly add reqs across all the queues
      for (int i = 0; i < total_reqs; ++i) {
        pq_num = get_pq_num_f();
        client_id = get_client_num_f();

        // update the tick_diff for the client on this queue
        auto it = client_reqtag_map.find(client_id);
        if (client_reqtag_map.end() != it) {
          auto last_pq_num = client_pq_tick_map[client_id].last_pq_num;
          auto last_tick_interval =
            client_pq_tick_map[client_id].get_last_tick_interval();
          Counter tick_interval = 0;
          if (last_pq_num != pq_num) {
            tick_interval = last_tick_interval;
            client_pq_tick_map[client_id].reset_last_tick_interval();
          }
          it->second.update_tick(tick_interval);
        }

        auto data = (client_id == client1) ? c1_data : c2_data;
        enqueue_to_pq_f(pq_num, client_id, data, dmc::TimeZero);
        client_pq_tick_map[client_id].update(pq_num);
        if (client_id == client1) {
          c1_data++;
          c1_req_count++;
        } else if(client_id == client2) {
          c2_data++;
          c2_req_count++;
        } else {
          ADD_FAILURE() << "invalid client_id...cannot add request";
        }
      }

      int total_req_cnt = 0;
      for (uint8_t i = 0; i < num_pqs; i++) {
        total_req_cnt += pq_arr[i]->request_count();
      }
      EXPECT_EQ(total_req_cnt, total_reqs);

      // DEQUEUE ITEMS
      int c1_deq_count = 0;
      int c2_deq_count = 0;
      c1_data = 0;
      c2_data = 0;
      //int num_reqs = 0;
      int num_reqs_to_dequeue = 300;
      pq_num = 0;
      // Randomly dequeue reqs from all queues until num_reqs are pulled
      while (num_reqs_to_dequeue > 0) {
        pq_num = get_pq_num_f();
        Queue::PullReq pr = pull_request_f(pq_num, dmc::TimeZero);
        if (pr.type != Queue::NextReqType::returning) {
          EXPECT_EQ(Queue::NextReqType::future, pr.type);
        } else {
          EXPECT_EQ(Queue::NextReqType::returning, pr.type);
          auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
          if (client1 == retn.client) {
            ++c1_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c1_data++);
          } else if (client2 == retn.client) {
            ++c2_deq_count;
            auto r = std::move(*retn.request);
            EXPECT_EQ(r.data, c2_data++);
          } else {
            ADD_FAILURE() << "got request from neither of two clients";
          }
          EXPECT_EQ(PhaseType::priority, retn.phase);
          num_reqs_to_dequeue--;
        }
      }

      // Expected client dequeue counts depending on req params
      const int exp_c1_deq_count = 75;
      const int exp_c2_deq_count = 225;

      // Due to the random nature of the test, it's quite possible
      // for client requests generated to be less than the reservation
      // or limit values. The following checks the dequeued counts
      // accordingly for each client.
      if (c1_req_count < exp_c1_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count - (exp_c1_deq_count - c1_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count + (exp_c1_deq_count - c1_req_count),
          c2_deq_count);
      } else if (c2_req_count < exp_c2_deq_count) {
        EXPECT_EQ(
          exp_c1_deq_count + (exp_c2_deq_count - c2_req_count),
          c1_deq_count);
        EXPECT_EQ(
          exp_c2_deq_count - (exp_c2_deq_count - c2_req_count),
          c2_deq_count);
      } else { // normal case: desired client req counts were generated
        EXPECT_EQ(exp_c1_deq_count, c1_deq_count);
        EXPECT_EQ(exp_c2_deq_count, c2_deq_count);
      }
    } // dmclock_server_pull_multiq.pull_weight_randomize_delydtag

  } // namespace dmclock
} // namespace crimson
