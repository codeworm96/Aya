/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 */

#include "http/httpd.hh"
#include "http/handlers.hh"
#include "http/api_docs.hh"

#include "db.hh"

#include <fstream>
#include <sstream>

Database database;

namespace bpo = boost::program_options;

using namespace seastar;
using namespace httpd;

class SeckillHandler : public httpd::handler_base {
public:
  virtual future<std::unique_ptr<reply> > handle(const sstring& path,
                                                 std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    return do_with(std::move(req), std::move(rep), [] (auto& req, auto& rep) {
        std::string user_id = req->get_query_param("user_id");
        std::string commodity_id = req->get_query_param("commodity_id");
        return seastar::smp::submit_to(seastar::smp::count - 1, [user_id, commodity_id]() {
            return database.seckill(user_id, commodity_id);
          }).then([user_id, commodity_id, &rep](std::pair<int, int> res) {
              std::ostringstream oss;
              if (res.first) {
                oss << "{\"result\":1, \"order_id\":"
                    << res.second << ", \"user_id\":\""
                    << user_id << "\", \"commodity_id\":\""
                    << commodity_id << "\"}\n";
              } else {
                oss << "{\"result\":0, \"order_id\":-1, \"user_id\":\""
                    << user_id << "\"}\n";
              }
              rep->_content = oss.str().c_str();
              rep->done("application/json");
              return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            });
      });
  }
};

class GetUserByIdHandler : public httpd::handler_base {
public:
  virtual future<std::unique_ptr<reply> > handle(const sstring& path,
                                                 std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    return do_with(std::move(req), std::move(rep), [] (auto& req, auto& rep) {
        std::string user_id = req->get_query_param("user_id");
        return seastar::smp::submit_to(seastar::smp::count - 1, [user_id]() {
            return database.get_user_by_id(user_id);
          }).then([&rep, user_id](User user) {
              std::ostringstream oss;
              oss << "{\"user_id\":\"" << user_id << "\", \"user_name\":\""
                  << user.name << "\", \"account_balance\":"
                  << user.account_balance / 100 << '.' << user.account_balance / 10 % 10
                  << user.account_balance % 10 << "}\n";
              rep->_content = oss.str().c_str();
              rep->done("application/json");
              return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
            });
      });
  }
};

void set_routes(routes& r) {
    r.add(operation_type::GET, url("/seckill/seckill"), new SeckillHandler());
    r.add(operation_type::GET, url("/seckill/getUserById"), new GetUserByIdHandler());
}

int main(int ac, char** av) {
    app_template app;
    app.add_options()("port", bpo::value<uint16_t>()->default_value(10000),
            "HTTP Server port");
    return app.run_deprecated(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        auto server = new http_server_control();
        auto rb = make_shared<api_registry_builder>("apps/httpd/");
        seastar::smp::submit_to(seastar::smp::count - 1, []() {
            database.recover("input.txt", "log.txt");
        });
        server->start().then([server] {
            return server->set_routes(set_routes);
        }).then([server, rb]{
            return server->set_routes([rb](routes& r){rb->set_api_doc(r);});
        }).then([server, rb]{
            return server->set_routes([rb](routes& r) {rb->register_function(r, "demo", "hello world application");});
        }).then([server, port] {
            return server->listen(port);
        }).then([server, port] {
            std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
            engine().at_exit([server] {
                return server->stop();
            });
        });

    });
}
