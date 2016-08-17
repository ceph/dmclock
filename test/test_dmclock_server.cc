// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include <memory>
#include <chrono>
#include <iostream>
#include <list>
#include <vector>


#include "dmclock_server.h"
#include "dmclock_util.h"
#include "gtest/gtest.h"


namespace dmc = crimson::dmclock;


// we need a request object; an empty one will do
struct Request {
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
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;
      using QueueRef = std::unique_ptr<Queue>;

      ClientId client1 = 17;
      ClientId client2 = 18;

      double reservation = 0.0;
      double weight = 0.0;

      dmc::ClientInfo ci1(reservation, weight, 0.0);
      dmc::ClientInfo ci2(reservation, weight, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return ci1;
	else if (client2 == c) return ci2;
	else {
	  ADD_FAILURE() << "got request from neither of two clients";
	  return ci1; // must return
	}
      };

      QueueRef pq(new Queue(client_info_f, false));
      Request req;
      ReqParams req_params(1,1);

      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(req, client1, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";


      EXPECT_DEATH_IF_SUPPORTED(pq->add_request(req, client2, req_params),
				"Assertion.*reservation.*max_tag.*"
				"proportion.*max_tag") <<
	"we should fail if a client tries to generate a reservation tag "
	"where reservation and proportion are both 0";
    }


    TEST(dmclock_server, client_idle_erase) {
      using ClientId = int;
      using Queue = dmc::PushPriorityQueue<ClientId,Request>;
      int client = 17;
      double reservation = 100.0;

      dmc::ClientInfo ci(reservation, 1.0, 0.0);
      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo { return ci; };
      auto server_ready_f = [] () -> bool { return true; };
      auto submit_req_f = [] (const ClientId& c,
			      std::unique_ptr<Request> req,
			      dmc::PhaseType phase) {
	// empty; do nothing
      };

      Queue pq(client_info_f,
	       server_ready_f,
	       submit_req_f,
	       std::chrono::seconds(3),
	       std::chrono::seconds(5),
	       std::chrono::seconds(2),
	       false);

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
	  EXPECT_EQ(pq.client_map.size(), 0) << "client map initially has size 0";
	});

      Request req;
      dmc::ReqParams req_params(1, 1);
      pq.add_request_time(req, client, req_params, dmc::get_time());

      std::this_thread::sleep_for(std::chrono::seconds(1));

      lock_pq([&] () {
	  EXPECT_EQ(1, pq.client_map.size()) << "client map has 1 after 1 client";
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
	  EXPECT_EQ(0, pq.client_map.size()) <<
	    "client map loses its entry after erase age";
	});
    } // TEST
    

    TEST(dmclock_server, client_idle_activation) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;

      Queue* pq;
      std::map<ClientId, dmc::ClientInfo*> client_map;

      // client IDs
      ClientId client1 = 1;
      ClientId client2 = 2;
      ClientId client3 = 3;
      ClientId client4 = 4;
      ClientId client5 = 5;
      ClientId client6 = 6;

      // ClientInfo
      client_map[client1] = new dmc::ClientInfo{0, 1, 0};
      client_map[client2] = new dmc::ClientInfo(0, 2, 4);
      client_map[client3] = new dmc::ClientInfo(0, 4, 4);
      client_map[client4] = new dmc::ClientInfo(0, 1, 10);
      client_map[client5] = new dmc::ClientInfo(1, 1, 0);
      client_map[client6] = new dmc::ClientInfo(0, 10, 0);

      double weight = 1;
      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return *client_map[c];
      };

      pq = new Queue(client_info_f,
		     std::chrono::seconds(3),
		     std::chrono::seconds(5),
		     std::chrono::seconds(2),
		     false);
      EXPECT_EQ(pq->client_map.size(), 0) << "client map initially has size 0";


      Request req;
      dmc::ReqParams req_params(1, 1);


      auto prop_f = [](const Queue::ClientRec& top) -> double {
	if (!top.idle && top.has_request()) {
	  return top.next_request().tag.proportion + top.prop_delta;
	}
	return NaN;
      };

      auto prop_min_O1_f = [&]()-> double {
	return fmin(prop_f(pq->ready_heap.top()),
		    prop_f(pq->limit_heap.top()));
      };

      auto prop_min_On_f = [&]()-> double {
	double lowest_prop_tag = NaN; // mark unset value as NaN
	for (auto const &c : pq->client_map) {
	  // don't use ourselves (or anything else that might be
	  // listed as idle) since we're now in the map
	  if (!c.second->idle) {
	    // use either lowest proportion tag or previous proportion tag
	    if (c.second->has_request()) {
	      double p = c.second->next_request().tag.proportion +
		c.second->prop_delta;
	      if (std::isnan(lowest_prop_tag) || p < lowest_prop_tag) {
		lowest_prop_tag = p;
	      }
	    }
	  }
	}
	return lowest_prop_tag;
      };

      auto check_ready_f = [&] (ClientId& cl, bool ground_truth) {
	EXPECT_EQ(ground_truth, pq->client_map.at(cl)->next_request().tag.ready);
      };

      auto check_same_f = [&] (double prop1, double prop2) {
	EXPECT_TRUE(std::isnan(prop1) == std::isnan(prop2));
	if (!std::isnan(prop1)) {
	  EXPECT_DOUBLE_EQ(prop1, prop2);
	}
      };

      // before adding any request, compare whether 
      // the min_p tag computed in O(n) and O(1) ways are matched.
      // debugging purpose -- set should_print to true
      auto add_f = [&](ClientId& cl, int count = 1, Time t = dmc::get_time(),  bool should_print = false) {
	++pq->tick;
	bool is_new = false;
	Queue::ClientRec* temp_client;

	auto client_it = pq->client_map.find(cl);
	if (pq->client_map.end() != client_it) {
	  temp_client = &(*client_it->second); 
	} else { 
	// hook: temporarily added new client to heaps to 
	// compute min p_tag independent of regular dmclock code.
	// We need to do this because dmclock code calls
	// pq->add_request_time(...) which adjusts the heaps twice:
	// (i)  when the client is new, and 
	// (ii) at the end of add_request_time(...) routine.
	// We do not want (ii) at this point, as it might alter the heaps' tops again. 
	    
	  is_new = true;
	  ClientInfo info = client_info_f(cl);
	  Queue::ClientRecRef client_rec =
	    std::make_shared<Queue::ClientRec>(cl, info, pq->tick);
	  pq->resv_heap.push(client_rec);
	  pq->limit_heap.push(client_rec);
	  pq->ready_heap.push(client_rec);

	  pq->client_map[cl] = client_rec;
	  temp_client = &(*client_rec); 
	}
	Queue::ClientRec& client = *temp_client;

	if (client.idle){
	  // checking two min p-tags
	  check_same_f(prop_min_O1_f(), prop_min_On_f());
	}

	if (is_new) {
	  // now undo the changes, so that regular dmclock code 
	  // flow is restored 
	  --pq->tick;
	  pq->remove_by_client(cl);
	  is_new = false;
	}

	// add 'count' requests
	for (int i = 0 ; i < count ; i++) {
	  pq->add_request_time(req, cl, req_params, t);
	  if (should_print) {
	    std::cout << "added :" << cl << std::endl;
	  }
	}
      };

      // debugging purpose -- set should_print to true
      auto pop_f = [&] (ClientId& cl, bool should_print = false) {
	Queue::PullReq pr = pq->pull_request();
	EXPECT_TRUE(pr.is_retn());
	EXPECT_EQ(cl, pr.get_retn().client);

	if (should_print) {
	  std::cout << "popped: " << pr.get_retn().client << std::endl;
	}
      };

      // debugging purpose -- set should_print to true 
      auto just_pop_f = [&] (bool should_print = false) {
	Queue::PullReq pr = pq->pull_request();
	if(pr.is_retn()){
	  if (should_print) {
	    std::cout << "popped: " << pr.get_retn().client << std::endl;
	  }
	}
	else if (pr.is_future()) {
	  if (should_print) {
	    std::cout << "future: " << dmc::format_time( pr.getTime()) << std::endl;
	  }
	  std::this_thread::sleep_for(std::chrono::milliseconds(250));
	}

	else if (pr.is_none()) {
	  if (should_print) {
	    std::cout << "none " << std::endl;
	  }
	  std::this_thread::sleep_for(std::chrono::milliseconds(250));
	}
	else {}
      };

      // debugging purpose
      auto print_f = [&](ClientId cl){
	auto client_it = pq->client_map.find(cl);
	if (pq->client_map.end() != client_it) {
	  std::cout << *client_it->second << std::endl;
	}
      };

      // debugging purpose
      auto print_all_f = [&]() {
	std::cout << std::endl << "At time-stamp " <<dmc::format_time(dmc::get_time())<< " : " << std::endl;
	for (auto &c : client_map) {
	  print_f(c.first);
	}
	std::cout <<"heap tops :" << std::endl;
	std::cout <<"resv: " ; print_f(pq->resv_heap.top().client);
	std::cout <<"ready: "; print_f(pq->ready_heap.top().client);
	std::cout <<"limit: "; print_f(pq->limit_heap.top().client);
      };

      Time time = dmc::get_time();

      add_f(client1, 3);
      add_f(client2, 2);

      check_ready_f(client1, false);
      check_ready_f(client2, false);

      /*
	* We'll add clients one by one.
	* Whenever a new client comes into the system, we use special add_f(...) 
	* routine that computes the min_p tag in O(1) and O(n) times, and
	* compares them whether they are same. 
	* During the addition of a new client, we'll create different situations such as  
	* - ready_heap's top element has higher p-tag than the limit_heap's top element,
	* - all existing clients are in 'ready' states,
	* - all existing clients are in 'idle' states, etc.
	* - all but one client has only next_request() true when an idle client wakes up.
	* It is worthwhile to mention that the ClientCompare functor is slightly
	* modified to adjust new changes. Because, there is a situation where an idle 
	* client sits on top of the heap even if there exists an active client. 
      */

      // pop client1
      pop_f(client1);

      // add more client 2
      add_f(client2, 1, time);
      add_f(client2);

      // client 2 is ready
      check_ready_f(client1, false);
      check_ready_f(client2, true);

      // pop client2
      pop_f(client2);
      //print_all_f();
      check_ready_f(client1, true);
      check_ready_f(client2, false);

      EXPECT_GT(prop_f(pq->ready_heap.top()), prop_f(pq->limit_heap.top()));

      //print_all_f();

      // when new client comes
      // check min_prop is same computed in two ways
      check_same_f(prop_min_O1_f(), prop_min_On_f());
      add_f(client3, 3);

      pop_f(client3);
      //print_all_f();

      // another client comes
      add_f(client4, 2);

      std::this_thread::sleep_for(std::chrono::milliseconds(250));
      //print_all_f();

      pop_f(client4);
      pop_f(client2);
      //print_all_f();

      std::this_thread::sleep_for(std::chrono::milliseconds(250));
      pop_f(client3);
      pop_f(client1);
      //print_all_f();

      //add another client
      add_f(client5);
      //print_all_f();

      // pop-off all requests
      size_t count = pq->request_count();
      while (count != 0) {
	auto p = pq->pull_request();
	if (!p.is_retn()) {
	  std::this_thread::sleep_for(std::chrono::milliseconds(250));
	} else {
	  count--;
	}
      }
      add_f(client1);

      // all clients are now idle
      std::this_thread::sleep_for(std::chrono::milliseconds(6500));
      //print_all_f();

      // activate idle clients one by one
      add_f(client2);
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
      //print_all_f();

      // only one client is active and has_request true.
      // without the recent modification in ClientCompare, add_t(client3) will fail
      add_f(client3);
      std::this_thread::sleep_for(std::chrono::milliseconds(250));

      add_f(client4);
      std::this_thread::sleep_for(std::chrono::milliseconds(250));

      add_f(client5);
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
      //print_all_f();
      std::this_thread::sleep_for(std::chrono::milliseconds(250));

      add_f(client6);
      //print_all_f();
      std::this_thread::sleep_for(std::chrono::milliseconds(250));

      just_pop_f();

      //print_all_f();



      // clean-up
      for (auto &c : client_map) {
	delete c.second;
      }
      delete pq;

    } // TEST

    
#if 0
    TEST(dmclock_server, client_idle_activation) {
      using ClientId = int;
      using Queue = dmc::PullPriorityQueue<ClientId,Request>;

      Queue* pq;

      // 3 clients
      ClientId client1 = 17;
      ClientId client2 = 18;
      ClientId client3 = 19;

      // SLOs
      double weight = 1;

      // ClientInfo
      dmc::ClientInfo ci1(0, 1*weight, 1*weight);
      dmc::ClientInfo ci2(0, 2*weight, 2*weight + 1);
      dmc::ClientInfo ci3(0, 4*weight, 4*weight);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return ci1;
	else if (client2 == c) return ci2;
	else if (client3 == c) return ci3;
	else {
	  ADD_FAILURE() << "got request from neither of two clients";
	  return ci1; // must return
	}
      };


      pq = new Queue(client_info_f,
	       std::chrono::seconds(3), // idle-time
	       std::chrono::seconds(5), // erase time
	       std::chrono::seconds(2), // check-time
	       false);

      auto lock_pq = [&](std::function<void()> code) {
	test_locked(pq->data_mtx, code);
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
       *   5 seconds : when we're scheduled to idle (+2 + 3)
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
	  EXPECT_EQ(pq->client_map.size(), 0) << "client map initially has size 0";
	});

      Request req;
      dmc::ReqParams req_params(1, 1);

      int num_req = 10;
      Time time = dmc::get_time();
      for (int i = 0 ; i < num_req; i++) {
	pq->add_request_time(req, client1, req_params, time);
	pq->add_request_time(req, client2, req_params, time);
	pq->add_request_time(req, client3, req_params, time);
	//time = dmc::get_time();
      }
//
//      std::this_thread::sleep_for(std::chrono::seconds(1));
//
//      lock_pq([&] () {
//	  EXPECT_EQ(3, pq.client_map.size()) << "client map has 3 after 3 client";
//	  EXPECT_FALSE(pq.client_map.at(client1)->idle) <<
//	    "initially client map entry shows not idle.";
//	});
//
//      std::this_thread::sleep_for(std::chrono::seconds(6));
//
//      lock_pq([&] () {
//	  EXPECT_TRUE(pq.client_map.at(client)->idle) <<
//	    "after idle age client map entry shows idle.";
//	});
//
//      std::this_thread::sleep_for(std::chrono::seconds(2));
//
//      lock_pq([&] () {
//	  EXPECT_EQ(0, pq.client_map.size()) <<
//	    "client map loses its entry after erase age";
//	});
            // count the number of times each client is picked up
      std::vector<ClientId> times;

      for(int i = 0 ; i < 3*num_req ; ) {
	Queue::PullReq pr  = pq->pull_request();
	if (pr.is_retn()) {
	  times.emplace_back(pr.get_retn().client);
	  std::cout <<pr.get_retn().client <<"\n";
	  i++;
	} else {
	  std::cout <<pr.getTime() <<"\n";
	  std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
      }
      //EXPECT_EQ(7, times.size()) << "";
    } // TEST
#endif

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

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo { return ci; };
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

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info1;
      };

      Queue pq(client_info_f, true);

      EXPECT_EQ(0, pq.client_count());
      EXPECT_EQ(0, pq.request_count());

      ReqParams req_params(1,1);

      pq.add_request(MyReq(1), client1, req_params);
      pq.add_request(MyReq(11), client1, req_params);
      pq.add_request(MyReq(2), client2, req_params);
      pq.add_request(MyReq(0), client2, req_params);
      pq.add_request(MyReq(13), client2, req_params);
      pq.add_request(MyReq(2), client2, req_params);
      pq.add_request(MyReq(13), client2, req_params);
      pq.add_request(MyReq(98), client2, req_params);
      pq.add_request(MyReq(44), client1, req_params);

      EXPECT_EQ(2, pq.client_count());
      EXPECT_EQ(9, pq.request_count());

      pq.remove_by_req_filter([](const MyReq& r) -> bool {return 1 == r.id % 2;});

      EXPECT_EQ(5, pq.request_count());

      std::list<MyReq> capture;
      pq.remove_by_req_filter([](const MyReq& r) -> bool {return 0 == r.id % 2;},
			      capture);

      EXPECT_EQ(0, pq.request_count());
      EXPECT_EQ(5, capture.size());
      int total = 0;
      for (auto i : capture) {
	total += i.id;
      }
      EXPECT_EQ(146, total) << " sum of captured items should be 146";
    } // TEST


    TEST(dmclock_server, remove_by_req_filter_ordering) {
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

      ClientId client1 = 17;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info1;
      };

      Queue pq(client_info_f, true);

      EXPECT_EQ(0, pq.client_count());
      EXPECT_EQ(0, pq.request_count());

      ReqParams req_params(1,1);

      pq.add_request(MyReq(1), client1, req_params);
      pq.add_request(MyReq(2), client1, req_params);
      pq.add_request(MyReq(3), client1, req_params);
      pq.add_request(MyReq(4), client1, req_params);
      pq.add_request(MyReq(5), client1, req_params);
      pq.add_request(MyReq(6), client1, req_params);

      EXPECT_EQ(1, pq.client_count());
      EXPECT_EQ(6, pq.request_count());

      // now remove odd ids in forward order

      std::vector<MyReq> capture;
      pq.remove_by_req_filter([](const MyReq& r) -> bool {return 1 == r.id % 2;},
			      capture);

      EXPECT_EQ(3, pq.request_count());
      EXPECT_EQ(3, capture.size());
      EXPECT_EQ(1, capture[0].id) << "items should come out in forward order";
      EXPECT_EQ(3, capture[1].id) << "items should come out in forward order";
      EXPECT_EQ(5, capture[2].id) << "items should come out in forward order";

      // now remove even ids in reverse order

      std::vector<MyReq> capture2;
      pq.remove_by_req_filter([](const MyReq& r) -> bool {return 0 == r.id % 2;},
			      capture2,
			      true);
      EXPECT_EQ(0, pq.request_count());
      EXPECT_EQ(3, capture2.size());
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

      ClientId client1 = 17;
      ClientId client2 = 98;

      dmc::ClientInfo info1(0.0, 1.0, 0.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info1;
      };

      Queue pq(client_info_f, true);

      EXPECT_EQ(0, pq.client_count());
      EXPECT_EQ(0, pq.request_count());

      ReqParams req_params(1,1);

      pq.add_request(MyReq(1), client1, req_params);
      pq.add_request(MyReq(11), client1, req_params);
      pq.add_request(MyReq(2), client2, req_params);
      pq.add_request(MyReq(0), client2, req_params);
      pq.add_request(MyReq(13), client2, req_params);
      pq.add_request(MyReq(2), client2, req_params);
      pq.add_request(MyReq(13), client2, req_params);
      pq.add_request(MyReq(98), client2, req_params);
      pq.add_request(MyReq(44), client1, req_params);

      EXPECT_EQ(2, pq.client_count());
      EXPECT_EQ(9, pq.request_count());

      std::list<MyReq> removed;

      pq.remove_by_client(client1, removed);

      EXPECT_EQ(3, removed.size());
      EXPECT_EQ(1, removed.front().id);
      removed.pop_front();
      EXPECT_EQ(11, removed.front().id);
      removed.pop_front();
      EXPECT_EQ(44, removed.front().id);
      removed.pop_front();

      EXPECT_EQ(6, pq.request_count());

      Queue::PullReq pr = pq.pull_request();
      EXPECT_TRUE(pr.is_retn());
      EXPECT_EQ(2, pr.get_retn().request->id);

      pr = pq.pull_request();
      EXPECT_TRUE(pr.is_retn());
      EXPECT_EQ(0, pr.get_retn().request->id);

      pq.remove_by_client(client2);
      EXPECT_EQ(0, pq.request_count()) <<
	"after second client removed, none left";
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

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return info1;
	else if (client2 == c) return info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existant client";
	  return info1;
	}
      };

      pq = QueueRef(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      auto now = dmc::get_time();

      for (int i = 0; i < 5; ++i) {
	pq->add_request(req, client1, req_params);
	pq->add_request(req, client2, req_params);
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

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return info1;
	else if (client2 == c) return info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existant client";
	  return info1;
	}
      };

      QueueRef pq(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto old_time = dmc::get_time() - 100.0;

      for (int i = 0; i < 5; ++i) {
	pq->add_request_time(req, client1, req_params, old_time);
	pq->add_request_time(req, client2, req_params, old_time);
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

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	if (client1 == c) return info1;
	else if (client2 == c) return info2;
	else {
	  ADD_FAILURE() << "client info looked up for non-existant client";
	  return info1;
	}
      };

      QueueRef pq(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto start_time = dmc::get_time() - 100.0;

      // add six requests; for same client reservations spaced one apart
      for (int i = 0; i < 3; ++i) {
	pq->add_request_time(req, client1, req_params, start_time);
	pq->add_request_time(req, client2, req_params, start_time);
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

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, false));

      Request req;
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
      ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, false));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      pq->add_request_time(req, client1, req_params, now + 100);
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
      ClientId client2 = 8;

      dmc::ClientInfo info(0.0, 1.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, true));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      pq->add_request_time(req, client1, req_params, now + 100);
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
      ClientId client2 = 8;

      dmc::ClientInfo info(1.0, 0.0, 1.0);

      auto client_info_f = [&] (ClientId c) -> dmc::ClientInfo {
	return info;
      };

      QueueRef pq(new Queue(client_info_f, true));

      Request req;
      ReqParams req_params(1,1);

      // make sure all times are well before now
      auto now = dmc::get_time();

      pq->add_request_time(req, client1, req_params, now + 100);
      Queue::PullReq pr = pq->pull_request(now);

      EXPECT_EQ(Queue::NextReqType::returning, pr.type);

      auto& retn = boost::get<Queue::PullReq::Retn>(pr.data);
      EXPECT_EQ(client1, retn.client);
    }
  } // namespace dmclock
} // namespace crimson
