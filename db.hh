#ifndef _DB_H_
#define _DB_H_

#include <string>
#include <ctime>
#include <unordered_map>
#include <map>
#include <vector>
#include <utility>

#define BUFSIZE 4096

struct User
{
  std::string name;
  int account_balance;
};

struct Commodity
{
  std::string name;
  int quantity;
  int unit_price;
};

struct Order
{
  std::string user_id;
  std::string commodity_id;
  std::time_t timestamp;
  Order() {}
  Order(std::string u, std::string c, std::time_t t): user_id(u), commodity_id(c), timestamp(t) {}
};

class Database
{
public:
  void recover(std::string input_file, std::string log_file);
  std::pair<int, int> seckill(std::string user_id, std::string commodity_id);
  User get_user_by_id(std::string user_id) {
    return users[user_id];
  }
  Commodity get_commodity_by_id(std::string commodity_id) {
    return commodities[commodity_id];
  }
private:
  std::unordered_map<std::string, User> users;
  std::unordered_map<std::string, Commodity> commodities;
  std::unordered_map<int, Order> orders;
  int order_size;
  int log_fd;
  char buf[BUFSIZE];

  void log_write(const Order& order);
  int perform_order(const Order& order);
};

#endif // _DB_H_
